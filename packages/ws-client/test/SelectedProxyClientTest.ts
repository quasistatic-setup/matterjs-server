/**
 * @license
 * Copyright 2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

import WebSocket from "ws";
import { InvalidServerVersion, MatterClient, type WebSocketLike } from "../src/index.js";
import { MockMatterServer } from "./MockMatterServer.js";

describe("Selected proxy WS client", () => {
    let server: MockMatterServer;
    let client: MatterClient;
    const connect = async (capability?: boolean) => {
        server = new MockMatterServer({ serverInfo: { schema_version: 14, ble_proxy_selection: capability } });
        await server.start();
        client = new MatterClient(server.url, url => new WebSocket(url) as unknown as WebSocketLike);
        await client.connect();
    };
    afterEach(async () => {
        client?.disconnect();
        await server?.stop();
    });

    for (const capability of [undefined, false]) {
        it(`refuses unadvertised selection (${String(capability)}) before sending the command`, async () => {
            await connect(capability);
            await expect(client.commissionWithCode("synthetic", false, { bleProxyId: "phone" })).to.be.rejectedWith(
                InvalidServerVersion,
            );
            expect(server.getReceivedCommands().some(c => c.message.command === "commission_with_code")).to.be.false;
        });
    }

    it("forwards an explicitly selected phone without changing credential selection", async () => {
        await connect(true);
        server.onCommand("commission_with_code", () => ({
            node_id: 7,
            date_commissioned: "2026-10-18T00:00:00",
            last_interview: "2026-10-18T00:00:00",
            interview_version: 1,
            available: true,
            attributes: {},
            attribute_subscriptions: [],
            is_bridge: false,
        }));
        await client.commissionWithCode("synthetic", false, { bleProxyId: "phone", threadDatasetId: "default" });
        const args = server.getReceivedCommands().find(c => c.message.command === "commission_with_code")!.message.args;
        expect(args).to.include({ ble_proxy_id: "phone", network_only: false, thread_dataset_id: "default" });
    });

    it("refuses a network-only selected-proxy request before sending", async () => {
        await connect(true);
        await expect(client.commissionWithCode("synthetic", true, { bleProxyId: "phone" })).to.be.rejectedWith(
            "networkOnly false",
        );
        expect(server.getReceivedCommands().some(c => c.message.command === "commission_with_code")).to.be.false;
    });

    it("sends an explicit cancellation using the same client connection", async () => {
        await connect(true);
        server.onCommand("cancel_commissioning", () => ({ canceled: true }));
        expect(await client.cancelCommissioning("phone")).to.be.true;
        expect(
            server.getReceivedCommands().find(c => c.message.command === "cancel_commissioning")!.message.args,
        ).to.deep.equal({ ble_proxy_id: "phone" });
    });
});
