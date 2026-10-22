/**
 * @license
 * Copyright 2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

import {
    ConfigStorage,
    type CommissioningRequest,
    type MatterController,
    WebSocketControllerHandler,
} from "@matter-server/ws-controller";
import { AsyncObservable, Environment, MockStorageService, NodeId, Observable } from "@matter/main";
import { createServer } from "node:http";
import { WebSocket } from "ws";
import { BleProxyHandler } from "../src/BleProxyHandler.js";
import { ProxyBle } from "../src/ProxyBle.js";
import type { ProxyBleCentralInterface } from "../src/ProxyBleChannel.js";

/** Real WS/HTTP/API/BLE stack; only the Matter controller's commissioning work is supplied by each test. */
export async function createBleApiHarness(selectionEnabled = true) {
    const env = new Environment("ble-api-test");
    new MockStorageService(env);
    const config = await ConfigStorage.create(env);
    const bleHandler = new BleProxyHandler();
    const ble = new ProxyBle(bleHandler);
    const central = ble.centralInterface as ProxyBleCentralInterface;
    central.onData(() => {});
    const observed: CommissioningRequest[] = [];
    let act: (request: CommissioningRequest) => Promise<void> = async () => {};
    let nodeId: NodeId | undefined;
    const eventNames = [
        "attributeChanged",
        "eventChanged",
        "nodeAdded",
        "nodeStateChanged",
        "nodeAvailabilityChanged",
        "nodeStructureChanged",
        "nodeDecommissioned",
        "nodeEndpointAdded",
        "nodeEndpointRemoved",
        "webRtcCallback",
    ];
    const commandHandler = {
        events: {
            started: new AsyncObservable(),
            ...Object.fromEntries(eventNames.map(name => [name, new Observable()])),
        },
        bleEnabled: true,
        bleProxyEnabled: true,
        async start() {},
        async initializeNodes() {},
        getNodeIds: () => (nodeId === undefined ? [] : [nodeId]),
        getCommissionerNodeId: () => 1n,
        isNodeIdInUse: () => false,
        async getCommissionerFabricData() {
            return { fabricId: 1n, compressedFabricId: 2n, fabricIndex: 1 };
        },
        async commissionNode(request: CommissioningRequest) {
            observed.push(request);
            await act(request);
            nodeId = request.nodeId ?? NodeId(77);
            return { nodeId };
        },
        getNodeDetails(id: NodeId) {
            return {
                node_id: id,
                date_commissioned: "2026-10-18T00:00:00",
                last_interview: "2026-10-18T00:00:00",
                interview_version: 1,
                available: true,
                attributes: {},
                attribute_subscriptions: [],
                is_bridge: false,
            };
        },
    };
    const controller = {
        commandHandler,
        async certificateService() {},
        threadDiagnostics: { events: { batchUpdated: new Observable() } },
    } as unknown as MatterController;
    const wsHandler = new WebSocketControllerHandler(
        controller,
        config,
        "local-ble-test",
        selectionEnabled ? bleHandler : undefined,
    );
    const server = createServer();
    await bleHandler.register(server);
    await wsHandler.register(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (typeof address !== "object" || !address) throw new Error("No test listener");
    const url = `ws://127.0.0.1:${address.port}`;
    const sockets: WebSocket[] = [];
    let nextId = 0;
    return {
        url,
        ble,
        central,
        bleHandler,
        observed,
        onCommission(action: typeof act) {
            act = action;
        },
        async openControl() {
            const socket = new WebSocket(`${url}/ws`);
            sockets.push(socket);
            const info = await new Promise<Record<string, unknown>>((resolve, reject) => {
                socket.once("message", data => resolve(JSON.parse(data.toString())));
                socket.once("error", reject);
            });
            return { socket, info };
        },
        request(socket: WebSocket, command: string, args: Record<string, unknown>) {
            const messageId = `ble-api-${nextId++}`;
            return new Promise<Record<string, any>>((resolve, reject) => {
                const cleanup = () => {
                    clearTimeout(timer);
                    socket.off("message", receive);
                    socket.off("close", onClosed);
                };
                const onClosed = () => {
                    cleanup();
                    reject(new Error("Control connection closed"));
                };
                const timer = setTimeout(() => {
                    cleanup();
                    reject(new Error("API reply timed out"));
                }, 3000);
                const receive = (raw: WebSocket.RawData) => {
                    const message = JSON.parse(raw.toString());
                    if (message.message_id !== messageId) return;
                    cleanup();
                    resolve(message);
                };
                socket.on("message", receive);
                socket.once("close", onClosed);
                socket.send(JSON.stringify({ message_id: messageId, command, args }));
            });
        },
        async close() {
            sockets.forEach(socket => socket.close());
            await wsHandler.unregister();
            await ble.close();
            await bleHandler.unregister();
            await new Promise<void>(resolve => server.close(() => resolve()));
            await config.close();
        },
    };
}
