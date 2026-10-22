/**
 * @license
 * Copyright 2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

import { Seconds } from "@matter/main";
import { BLE_PROXY_IDENTITY_FEATURE, BinaryFrameOpcode, BleProxyCommand } from "../src/BleProxyProtocol.js";
import { createBleApiHarness } from "./BleApiHarness.js";
import { BleProxyTestClient } from "./BleProxyTestClient.js";
import { MockBleDevice } from "./MockBleDevice.js";

describe("Selected BLE commissioning API", function () {
    this.timeout(10_000);
    let h: Awaited<ReturnType<typeof createBleApiHarness>>;
    const clients: BleProxyTestClient[] = [];
    const connect = async (identity = true) => {
        const client = new BleProxyTestClient();
        clients.push(client);
        await client.connect(`${h.url}/ble`, identity ? [BLE_PROXY_IDENTITY_FEATURE] : undefined);
        return client;
    };
    const id = (client: BleProxyTestClient) => client.helloResponse!.proxy_id!;
    const start = (socket: Awaited<ReturnType<typeof h.openControl>>["socket"], proxy: string) =>
        h.request(socket, "commission_with_code", {
            code: "synthetic-local-onboarding",
            network_only: false,
            ble_proxy_id: proxy,
        });

    beforeEach(async () => {
        h = await createBleApiHarness();
    });
    afterEach(async () => {
        clients.splice(0).forEach(client => client.close());
        await h.close();
    });

    it("advertises actual routing support in server_info", async () => {
        const { info } = await h.openControl();
        expect(info.ble_proxy_selection).to.equal(true);
    });

    it("rejects unknown proxies before the controller allocates work", async () => {
        const { socket } = await h.openControl();
        const result = await start(socket, "unknown");
        expect(result.error_code).to.not.be.undefined;
        expect(h.observed).to.have.length(0);
    });

    it("rejects network-only and malformed selections before commissioning", async () => {
        const phone = await connect();
        const { socket } = await h.openControl();
        for (const selection of ["", null, 42, "x".repeat(129)]) {
            const result = await h.request(socket, "commission_with_code", {
                code: "synthetic",
                ble_proxy_id: selection,
            });
            expect(result.error_code).to.equal(8);
        }
        const result = await h.request(socket, "commission_with_code", {
            code: "synthetic",
            network_only: true,
            ble_proxy_id: id(phone),
        });
        expect(result.error_code).to.equal(8);
        expect(h.observed).to.have.length(0);
    });

    it("runs the real API, SDK scan and GATT/BTP transport through the selected phone while HA stays connected", async () => {
        const ha = await connect(false);
        const phone = await connect();
        const device = new MockBleDevice({ discriminator: 1234, vendorId: 0xfff1, productId: 0x8000 });
        let seen!: () => void;
        const oldDiscovery = new Promise<void>(resolve => {
            seen = resolve;
        });
        void h.ble.scanner;
        const observer = () => {
            h.bleHandler.deviceDiscovered.off(observer);
            seen();
        };
        h.bleHandler.deviceDiscovered.on(observer);
        ha.sendEvent("device_discovered", device.discoveredEventData);
        await oldDiscovery;
        phone.onCommand(BleProxyCommand.StartScan, async () => {
            setTimeout(() => phone.sendEvent("device_discovered", device.discoveredEventData), 5);
        });
        phone.onCommand(BleProxyCommand.Connect, async () => ({ connection_handle: 7, mtu: 247 }));
        phone.onCommand(BleProxyCommand.DiscoverServices, async () => ({ services: device.services }));
        phone.onCommand(BleProxyCommand.DiscoverCharacteristics, async () => ({
            characteristics: device.characteristics,
        }));
        phone.onCommand(BleProxyCommand.WriteAndSubscribe, async () => {
            phone.sendBinaryFrame(BinaryFrameOpcode.Notification, 7, device.generateBtpHandshakeResponse());
        });
        h.onCommission(async request => {
            let finish!: () => void;
            const canceled = new Promise<void>(resolve => {
                finish = resolve;
            });
            request.abort!.addEventListener("abort", finish, { once: true });
            try {
                const devices = await h.ble.scanner.findCommissionableDevicesContinuously(
                    {},
                    finish,
                    Seconds(1),
                    canceled,
                );
                request.abort!.throwIfAborted();
                expect(devices).to.have.length(1);
            } finally {
                request.abort!.removeEventListener("abort", finish);
            }
            await h.central.openChannel({ type: "ble", peripheralAddress: device.address });
        });
        const { socket } = await h.openControl();
        const result = await start(socket, id(phone));
        expect(result.error_code).to.be.undefined;
        expect(result.result.available).to.equal(true);
        expect(ha.receivedCommands).to.have.length(0);
        expect(phone.receivedCommands.map(command => command.command)).to.include.members([
            "connect",
            "write_and_subscribe",
            "disconnect",
        ]);
    });

    it("cancels only operations owned by the requesting control connection", async () => {
        const phone = await connect();
        const owner = await h.openControl();
        const other = await h.openControl();
        let started!: () => void;
        const entered = new Promise<void>(resolve => {
            started = resolve;
        });
        h.onCommission(async request => {
            started();
            await new Promise<void>(resolve =>
                request.abort!.addEventListener("abort", () => resolve(), { once: true }),
            );
            request.abort!.throwIfAborted();
        });
        const pending = start(owner.socket, id(phone));
        await entered;
        const denied = await h.request(other.socket, "cancel_commissioning", { ble_proxy_id: id(phone) });
        expect(denied.result.canceled).to.equal(false);
        expect(h.observed[0].abort!.aborted).to.equal(false);
        const accepted = await h.request(owner.socket, "cancel_commissioning", { ble_proxy_id: id(phone) });
        expect(accepted.result.canceled).to.equal(true);
        expect((await pending).error_code).to.not.be.undefined;
        const repeated = await h.request(owner.socket, "cancel_commissioning", { ble_proxy_id: id(phone) });
        expect(repeated.result.canceled).to.equal(false);
        h.onCommission(async () => {});
        expect((await start(owner.socket, id(phone))).error_code).to.be.undefined;
    });

    it("rejects a second selected commissioning with the busy error code while the lease is held", async () => {
        const phone = await connect();
        const owner = await h.openControl();
        const other = await h.openControl();
        let started!: () => void;
        const entered = new Promise<void>(resolve => {
            started = resolve;
        });
        h.onCommission(async request => {
            started();
            await new Promise<void>(resolve =>
                request.abort!.addEventListener("abort", () => resolve(), { once: true }),
            );
            request.abort!.throwIfAborted();
        });
        const pending = start(owner.socket, id(phone));
        await entered;
        const busy = await start(other.socket, id(phone));
        expect(busy.error_code).to.equal(107);
        expect(h.observed).to.have.length(1);
        await h.request(owner.socket, "cancel_commissioning", { ble_proxy_id: id(phone) });
        expect((await pending).error_code).to.not.be.undefined;
    });

    it("reports a cancel as not canceled when commissioning completes anyway", async () => {
        const phone = await connect();
        const owner = await h.openControl();
        let started!: () => void;
        const entered = new Promise<void>(resolve => {
            started = resolve;
        });
        h.onCommission(async request => {
            started();
            // Past PASE the controller finishes the device even though the caller gave up.
            await new Promise<void>(resolve =>
                request.abort!.addEventListener("abort", () => resolve(), { once: true }),
            );
        });
        const pending = start(owner.socket, id(phone));
        await entered;
        const cancel = await h.request(owner.socket, "cancel_commissioning", { ble_proxy_id: id(phone) });
        expect(cancel.result.canceled).to.equal(false);
        const result = await pending;
        expect(result.error_code).to.be.undefined;
        expect(result.result.available).to.equal(true);
    });

    it("keeps legacy requests working without proxy selection", async () => {
        const { socket } = await h.openControl();
        const result = await h.request(socket, "commission_with_code", { code: "synthetic", network_only: true });
        expect(result.error_code).to.be.undefined;
        expect(h.observed[0].abort).to.be.undefined;
    });
});
