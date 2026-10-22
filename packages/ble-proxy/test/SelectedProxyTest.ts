/**
 * @license
 * Copyright 2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

import { Seconds } from "@matter/main";
import { createServer } from "node:http";
import { BleProxyHandler } from "../src/BleProxyHandler.js";
import { BLE_PROXY_IDENTITY_FEATURE, BinaryFrameOpcode, BleProxyCommand } from "../src/BleProxyProtocol.js";
import { ProxyBle } from "../src/ProxyBle.js";
import type { ProxyBleCentralInterface, ProxyBleChannel } from "../src/ProxyBleChannel.js";
import { BleProxyTestClient } from "./BleProxyTestClient.js";
import { MockBleDevice } from "./MockBleDevice.js";

function deferred<T = void>() {
    let resolve!: (value: T | PromiseLike<T>) => void;
    const promise = new Promise<T>(r => {
        resolve = r;
    });
    return { promise, resolve };
}

async function waitFor(condition: () => boolean): Promise<void> {
    const deadline = Date.now() + 2000;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error("Condition did not settle");
        await new Promise<void>(resolve => setTimeout(resolve, 5));
    }
}

describe("Explicit BLE proxy selection", function () {
    this.timeout(10_000);
    let handler: BleProxyHandler;
    let server: ReturnType<typeof createServer>;
    let url: string;
    let ble: ProxyBle;
    let central: ProxyBleCentralInterface;
    let device: MockBleDevice;
    const clients: BleProxyTestClient[] = [];

    const connect = async (identity = true) => {
        const client = new BleProxyTestClient();
        clients.push(client);
        await client.connect(url, identity ? [BLE_PROXY_IDENTITY_FEATURE] : undefined);
        return client;
    };
    const id = (client: BleProxyTestClient) => client.helloResponse!.proxy_id!;
    const advertise = async (client: BleProxyTestClient, data = device.discoveredEventData) => {
        const received = deferred();
        const observer = () => {
            handler.deviceDiscovered.off(observer);
            received.resolve();
        };
        handler.deviceDiscovered.on(observer);
        client.sendEvent("device_discovered", data);
        await received.promise;
    };
    const wireBtp = (client: BleProxyTestClient, handle: number, target = device) => {
        client.onCommand(BleProxyCommand.Connect, async () => ({ connection_handle: handle, mtu: 247 }));
        client.onCommand(BleProxyCommand.DiscoverServices, async () => ({ services: target.services }));
        client.onCommand(BleProxyCommand.DiscoverCharacteristics, async () => ({
            characteristics: target.characteristics,
        }));
        client.onCommand(BleProxyCommand.WriteAndSubscribe, async () => {
            client.sendBinaryFrame(BinaryFrameOpcode.Notification, handle, target.generateBtpHandshakeResponse());
            return {};
        });
    };
    const openChannel = async (target = device) =>
        (await central.openChannel({
            type: "ble",
            peripheralAddress: target.address,
        })) as ProxyBleChannel;

    beforeEach(async () => {
        handler = new BleProxyHandler();
        server = createServer();
        await handler.register(server);
        await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        if (typeof address !== "object" || !address) throw new Error("No test listener");
        url = `ws://127.0.0.1:${address.port}/ble`;
        ble = new ProxyBle(handler);
        central = ble.centralInterface as ProxyBleCentralInterface;
        central.onData(() => {});
        device = new MockBleDevice({ discriminator: 1234, vendorId: 0xfff1, productId: 0x8000 });
    });

    afterEach(async () => {
        await ble.close();
        clients.splice(0).forEach(client => client.close());
        await handler.unregister();
        await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
    });

    it("preserves the exact v1 response for a legacy client", async () => {
        const legacy = await connect(false);
        expect(legacy.helloResponse).to.deep.equal({ type: "hello_response", version: 1 });
    });

    it("negotiates distinct connection identities and rejects unknown identities before starting work", async () => {
        const a = await connect();
        const b = await connect();
        expect(id(a)).to.be.a("string").and.not.empty;
        expect(id(a)).to.not.equal(id(b));
        let called = false;
        await expect(
            handler.withProxySelection("unknown", async () => {
                called = true;
            }),
        ).to.be.rejectedWith("Unknown or disconnected");
        expect(called).to.be.false;
    });

    it("reproduces first-seen routing without explicit selection despite the phone's stronger RSSI", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha, { ...device.discoveredEventData, rssi: -95 });
        await advertise(phone, { ...device.discoveredEventData, rssi: -10 });
        wireBtp(ha, 11);
        wireBtp(phone, 22);
        const channel = await openChannel();
        expect(ha.receivedCommands.some(c => c.command === "connect")).to.be.true;
        expect(phone.receivedCommands.some(c => c.command === "connect")).to.be.false;
        await channel.close();
    });

    it("routes the full GATT/BTP handshake only to the selected phone and restores HA ownership afterward", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        await advertise(phone);
        const previousOwner = handler.getOwner(device.address);
        wireBtp(ha, 11);
        wireBtp(phone, 22);
        let selectedChannel: ProxyBleChannel | undefined;
        await handler.withProxySelection(id(phone), async () => {
            expect(handler.getOwner(device.address)).to.be.undefined;
            await advertise(phone);
            selectedChannel = await openChannel();
            expect(selectedChannel.connected).to.be.true;
        });
        expect(selectedChannel!.connected).to.be.false;
        expect(ha.receivedCommands.some(c => c.command === "connect")).to.be.false;
        expect(phone.receivedCommands.map(c => c.command)).to.include.members([
            "connect",
            "discover_services",
            "discover_characteristics",
            "write_and_subscribe",
            "disconnect",
        ]);
        expect(phone.receivedCommands.find(c => c.command === "disconnect")?.args).to.deep.equal({
            connection_handle: 22,
        });
        expect(handler.getOwner(device.address)).to.equal(previousOwner);
    });

    it("filters both SDK cache and fresh HA advertisements until the selected phone reports a valid fresh advertisement", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        await advertise(phone);
        expect(ble.scanner.getDiscoveredCommissionableDevices({})).to.have.length(1);
        await handler.withProxySelection(id(phone), async () => {
            expect(ble.scanner.getDiscoveredCommissionableDevices({})).to.have.length(0);
            await advertise(ha);
            expect(ble.scanner.getDiscoveredCommissionableDevices({})).to.have.length(0);
            await advertise(phone, { ...device.discoveredEventData, service_data: { fff6: "AA==" } });
            expect(ble.scanner.getDiscoveredCommissionableDevices({})).to.have.length(0);
            await advertise(phone);
            expect(ble.scanner.getDiscoveredCommissionableDevices({})).to.have.length(1);
        });
    });

    it("starts and stops scans only on the selected phone, including when another proxy joins", async () => {
        const ha = await connect(false);
        const phone = await connect();
        let late: BleProxyTestClient;
        await handler.withProxySelection(id(phone), async () => {
            await handler.startScan({ service_uuids: ["fff6"] });
            late = await connect();
        });
        expect(phone.receivedCommands.map(c => c.command)).to.deep.equal(["start_scan", "stop_scan"]);
        expect(ha.receivedCommands).to.have.length(0);
        expect(late!.receivedCommands).to.have.length(0);
    });

    it("delivers real asynchronous advertisements to an active SDK discovery before its deadline", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        phone.onCommand(BleProxyCommand.StartScan, async () => {
            setTimeout(() => phone.sendEvent("device_discovered", device.discoveredEventData), 5);
        });
        await handler.withProxySelection(id(phone), async () => {
            const found = deferred();
            let count = 0;
            const devices = await ble.scanner.findCommissionableDevicesContinuously(
                {},
                () => {
                    count++;
                    found.resolve();
                },
                Seconds(1),
                found.promise,
            );
            expect(count).to.equal(1);
            expect(devices).to.have.length(1);
        });
    });

    it("rejects overlapping selections and unrelated discovery rather than sharing the selected scanner", async () => {
        const phone = await connect();
        const started = deferred();
        const finish = deferred();
        const running = handler.withProxySelection(id(phone), async () => {
            started.resolve();
            await finish.promise;
        });
        await started.promise;
        try {
            await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith("busy");
            await expect(handler.startScan({})).to.be.rejectedWith("busy");
        } finally {
            finish.resolve();
            await running;
        }
    });

    it("does not replace a scan that is already active", async () => {
        const phone = await connect();
        await handler.startScan({});
        await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith("busy");
        await handler.stopScan();
    });

    it("does not start a selection halfway through an existing HA GATT handshake", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        wireBtp(ha, 11);
        const allowConnect = deferred();
        ha.onCommand(BleProxyCommand.Connect, async () => {
            await allowConnect.promise;
            return { connection_handle: 11, mtu: 247 };
        });
        const opening = openChannel();
        await ha.waitForCommand("connect");
        try {
            await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith("busy");
        } finally {
            allowConnect.resolve();
        }
        const channel = await opening;
        await handler.withProxySelection(id(phone), async () => {
            expect(channel.connected).to.be.true;
        });
        await channel.close();
    });

    it("aborts on selected-client loss without falling back, retaining the lease until work settles", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        const entered = deferred();
        const release = deferred();
        let aborted!: AbortSignal;
        const running = handler.withProxySelection(id(phone), async signal => {
            aborted = signal;
            await advertise(phone);
            entered.resolve();
            await release.promise;
            expect(handler.getOwner(device.address)).to.be.undefined;
            signal.throwIfAborted();
        });
        const outcome = running.then(
            () => undefined,
            error => error as Error,
        );
        await entered.promise;
        phone.close();
        await waitFor(() => aborted.aborted);
        const replacement = await connect();
        expect(id(replacement)).to.not.equal(id(phone));
        await expect(handler.withProxySelection(id(replacement), async () => {})).to.be.rejectedWith("busy");
        release.resolve();
        expect((await outcome)?.message).to.include("Selected BLE proxy disconnected");
        await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith(
            "Unknown or disconnected",
        );
        await handler.withProxySelection(id(replacement), async () => {
            expect(handler.getOwner(device.address)).to.be.undefined;
        });
    });

    it("reports an operation that completed before noticing the lost proxy as a success", async () => {
        const phone = await connect();
        wireBtp(phone, 41);
        const finishing = deferred();
        const finish = deferred();
        let aborted!: AbortSignal;
        const running = handler.withProxySelection(id(phone), async signal => {
            aborted = signal;
            await advertise(phone);
            await openChannel();
            // Commissioning no longer needs BLE once the device has joined its network.
            finishing.resolve();
            await finish.promise;
            return "commissioned node";
        });
        await finishing.promise;
        phone.close();
        await waitFor(() => aborted.aborted);
        finish.resolve();
        expect(await running).to.equal("commissioned node");
        const replacement = await connect();
        await handler.withProxySelection(id(replacement), async () => {});
    });

    it("closes the selected channel and releases selection after an operation error", async () => {
        const phone = await connect();
        wireBtp(phone, 22);
        let channel!: ProxyBleChannel;
        await expect(
            handler.withProxySelection(id(phone), async () => {
                await advertise(phone);
                channel = await openChannel();
                throw new Error("simulated commissioning failure");
            }),
        ).to.be.rejectedWith("simulated commissioning failure");
        expect(channel.connected).to.be.false;
        await handler.withProxySelection(id(phone), async () => {});
    });

    it("retains selection until the GATT disconnect has been acknowledged", async () => {
        const phone = await connect();
        wireBtp(phone, 22);
        const allowDisconnect = deferred();
        phone.onCommand(BleProxyCommand.Disconnect, async () => {
            await allowDisconnect.promise;
            return {};
        });
        const running = handler.withProxySelection(id(phone), async () => {
            await advertise(phone);
            await openChannel();
        });
        await phone.waitForCommand("disconnect");
        try {
            await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith("busy");
        } finally {
            allowDisconnect.resolve();
            await running;
        }
        await handler.withProxySelection(id(phone), async () => {});
    });

    it("disconnects only the selected handle when GATT service discovery fails", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        wireBtp(phone, 22);
        phone.onCommand(BleProxyCommand.DiscoverServices, async () => ({ services: [] }));
        await expect(
            handler.withProxySelection(id(phone), async () => {
                await advertise(phone);
                await openChannel();
            }),
        ).to.be.rejectedWith("does not have Matter BLE service");
        expect(phone.receivedCommands.find(c => c.command === "disconnect")?.args).to.deep.equal({
            connection_handle: 22,
        });
        expect(ha.receivedCommands.some(c => c.command === "connect" || c.command === "disconnect")).to.be.false;
        await handler.withProxySelection(id(phone), async () => {});
    });

    it("rejects a pending GATT connect when the phone disappears instead of connecting through HA", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        phone.onCommand(BleProxyCommand.Connect, async () => await new Promise<Record<string, unknown>>(() => {}));
        const outcome = handler
            .withProxySelection(id(phone), async () => {
                await advertise(phone);
                await openChannel();
            })
            .then(
                () => undefined,
                error => error as Error,
            );
        await phone.waitForCommand("connect");
        phone.close();
        expect((await outcome)?.message).to.include("disconnected");
        expect(ha.receivedCommands.some(c => c.command === "connect")).to.be.false;
        await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith(
            "Unknown or disconnected",
        );
    });

    it("cancels continuous SDK discovery, stops the scan and permits a fresh subsequent operation", async () => {
        const phone = await connect();
        const abort = new AbortController();
        const running = handler.withProxySelection(
            id(phone),
            async signal => {
                const canceled = new Promise<void>(resolve =>
                    signal.addEventListener("abort", () => resolve(), { once: true }),
                );
                await ble.scanner.findCommissionableDevicesContinuously({}, () => {}, Seconds(5), canceled);
                // Discovery returns on cancel; commissioning turns that into a failure.
                signal.throwIfAborted();
            },
            abort.signal,
        );
        const outcome = running.then(
            () => undefined,
            error => error as Error,
        );
        await phone.waitForCommand("start_scan");
        abort.abort(new Error("user canceled"));
        expect((await outcome)?.message).to.equal("user canceled");
        expect(phone.receivedCommands.some(c => c.command === "stop_scan")).to.be.true;
        await handler.withProxySelection(id(phone), async () => {
            await advertise(phone);
            expect(ble.scanner.getDiscoveredCommissionableDevices({})).to.have.length(1);
        });
    });

    it("handles a caller deadline and refuses a pre-aborted operation", async () => {
        const phone = await connect();
        const signal = AbortSignal.timeout(30);
        const result = handler
            .withProxySelection(
                id(phone),
                async active => {
                    await new Promise<void>(resolve =>
                        active.addEventListener("abort", () => resolve(), { once: true }),
                    );
                    active.throwIfAborted();
                },
                signal,
            )
            .then(
                () => undefined,
                error => error as Error,
            );
        expect((await result)?.name).to.equal("TimeoutError");
        let called = false;
        await expect(
            handler.withProxySelection(
                id(phone),
                async () => {
                    called = true;
                },
                signal,
            ),
        ).to.be.rejected;
        expect(called).to.be.false;
        await handler.withProxySelection(id(phone), async () => {});
    });

    it("keeps a pre-existing HA GATT channel bound to HA while the phone is selected", async () => {
        const ha = await connect(false);
        const phone = await connect();
        await advertise(ha);
        wireBtp(ha, 11);
        // BLE handles are local to each proxy; deliberately collide with HA's handle.
        wireBtp(phone, 11);
        const existing = await openChannel();
        await handler.withProxySelection(id(phone), async () => {
            await advertise(phone);
            await openChannel();
        });
        expect(existing.connected).to.be.true;
        expect(ha.receivedCommands.some(c => c.command === "disconnect")).to.be.false;
        await existing.close();
        expect(ha.receivedCommands.find(c => c.command === "disconnect")?.args).to.deep.equal({
            connection_handle: 11,
        });
    });

    it("rejects work retained from an expired selection while another selection is active", async () => {
        const phone = await connect();
        const gate = deferred();
        let late!: Promise<unknown>;
        await handler.withProxySelection(id(phone), async () => {
            late = gate.promise
                .then(() => handler.startScan({}))
                .then(
                    () => undefined,
                    error => error as Error,
                );
        });
        await handler.withProxySelection(id(phone), async () => {
            gate.resolve();
            expect(((await late) as Error).message).to.include("selection has expired");
            await handler.startScan({});
        });
    });

    it("retains the lease until an open the operation abandoned has disconnected", async () => {
        const phone = await connect();
        wireBtp(phone, 23);
        const connecting = deferred();
        const allowConnect = deferred();
        phone.onCommand(BleProxyCommand.Connect, async () => {
            connecting.resolve();
            await allowConnect.promise;
            return { connection_handle: 23, mtu: 247 };
        });
        let abandoned!: Promise<unknown>;
        let settled = false;
        const outcome = handler
            .withProxySelection(id(phone), async () => {
                await advertise(phone);
                // The SDK races opens against its own abort and stops waiting while the open continues.
                abandoned = openChannel().then(
                    () => undefined,
                    error => error as Error,
                );
                await connecting.promise;
                throw new Error("commissioning abandoned the open");
            })
            .then(
                () => undefined,
                error => error as Error,
            )
            .finally(() => {
                settled = true;
            });
        await connecting.promise;
        await new Promise(resolve => setTimeout(resolve, 20));
        expect(settled).to.be.false;
        await expect(handler.withProxySelection(id(phone), async () => {})).to.be.rejectedWith("busy");
        allowConnect.resolve();
        expect((await outcome)?.message).to.equal("commissioning abandoned the open");
        expect(await abandoned).to.be.instanceOf(Error);
        const commands = phone.receivedCommands.map(c => c.command);
        expect(commands).to.include("disconnect");
        expect(commands).to.not.include("discover_services");
        expect(commands).to.not.include("write_and_subscribe");
        await handler.withProxySelection(id(phone), async () => {});
    });

    it("disconnects an open whose caller aborted after the proxy connected", async () => {
        const ha = await connect(false);
        wireBtp(ha, 31);
        const connecting = deferred();
        const allowConnect = deferred();
        ha.onCommand(BleProxyCommand.Connect, async () => {
            connecting.resolve();
            await allowConnect.promise;
            return { connection_handle: 31, mtu: 247 };
        });
        await advertise(ha);
        const abort = new AbortController();
        const opening = central.openChannel(
            { type: "ble", peripheralAddress: device.address },
            { abort: abort.signal },
        );
        const outcome = opening.then(
            () => undefined,
            error => error as Error,
        );
        await connecting.promise;
        abort.abort(new Error("another candidate won PASE"));
        allowConnect.resolve();
        expect((await outcome)?.message).to.equal("another candidate won PASE");
        expect(ha.receivedCommands.find(c => c.command === "disconnect")?.args).to.deep.equal({
            connection_handle: 31,
        });
        expect(ha.receivedCommands.some(c => c.command === "discover_services")).to.be.false;
    });
});
