/**
 * @license
 * Copyright 2025-2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

import { ServerError, type WebServerHandler } from "@matter-server/ws-controller";
import { Logger, Minutes, Observable, Time, Timestamp } from "@matter/main";
import { BleError } from "@matter/main/protocol";
import { AsyncLocalStorage } from "node:async_hooks";
import type { createServer } from "node:http";
import { WebSocket, WebSocketServer } from "ws";
import { BleProxyConnection } from "./BleProxyConnection.js";
import {
    BleProxyCommand,
    BleProxyCommandError,
    BleProxyErrorCode,
    type BleProxyErrorCodeValue,
    BleProxyEvent,
    type DeviceDiscoveredData,
    type StartScanArgs,
} from "./BleProxyProtocol.js";

type HttpServer = ReturnType<typeof createServer>;

interface ProxySelection {
    connection: BleProxyConnection;
    seen: Set<string>;
    abort: AbortController;
    signal: AbortSignal;
    channels: Set<{ connected: boolean; close(): Promise<void> }>;
    opening: Set<Promise<unknown>>;
}

const logger = Logger.get("BleProxyHandler");

/**
 * Age after which an ownership entry is dropped when a new scan starts. The entry ages like the BLE scanner's record for
 * a client without listening time: from the later of its last report and the end of the last scan, so it never expires
 * while the scanner still offers the device.
 */
const OWNER_MAX_AGE = Minutes(15);

function isErrorCode(err: unknown, code: BleProxyErrorCodeValue): boolean {
    return err instanceof BleProxyCommandError && err.code === code;
}

/**
 * WebSocket server handler (hub) for the `/ble` BLE proxy endpoint.
 *
 * Accepts any number of proxy client connections, broadcasts scan commands to
 * all of them, and tracks which client owns each discovered peripheral so that
 * per-peripheral traffic routes to a single client.
 */
export class BleProxyHandler implements WebServerHandler {
    #wss?: WebSocketServer;
    /** Upgrade listener removers, one per HTTP server `register()` call (multi-bind). */
    #removeUpgradeListeners: (() => void)[] = [];
    #connections = new Set<BleProxyConnection>();
    #closed = false;
    #selection?: ProxySelection;
    #openingChannels = 0;
    readonly #selectionContext = new AsyncLocalStorage<ProxySelection>();

    /** Active scan intent + args, so clients joining mid-scan can be synced. */
    #scanActive = false;
    #scanArgs?: StartScanArgs;
    #scanStoppedAt?: Timestamp;
    /** Connections currently told to scan, for aggregate `scanStopped`. */
    #scanning = new Set<BleProxyConnection>();

    /** address -> owning connection, every connection that has seen it, and when it was last reported. */
    #owners = new Map<
        string,
        { owner: BleProxyConnection; seers: Set<BleProxyConnection>; lastReportedAt: Timestamp }
    >();

    /** Emitted whenever a connection completes its handshake. */
    readonly connectionEstablished = new Observable<[]>();
    /** Emitted for each `device_discovered`, after ownership is updated. */
    readonly deviceDiscovered = new Observable<[data: DeviceDiscoveredData, connection: BleProxyConnection]>();
    /** Emitted once no connection is scanning anymore. */
    readonly scanStopped = new Observable<[reason: string]>();

    get connected(): boolean {
        if (this.#selection) {
            return this.#selection.connection.connected && !this.#selection.signal.aborted;
        }
        for (const c of this.#connections) {
            if (c.connected) return true;
        }
        return false;
    }

    /**
     * Keep one commissioning operation on its explicitly selected proxy connection.
     * The operation must observe signal and await its work before settling; its own result decides
     * success. Retain the lease until it has settled, even after abort, so a late task cannot inherit
     * a new selection.
     */
    async withProxySelection<T>(
        proxyId: string,
        operation: (signal: AbortSignal) => Promise<T>,
        signal?: AbortSignal,
    ): Promise<T> {
        signal?.throwIfAborted();
        if (this.#closed) throw new BleError("BLE proxy handler is closed");
        if (this.#selection || this.#scanActive || this.#scanning.size || this.#openingChannels) {
            throw ServerError.bleProxyBusy("BLE proxy is busy");
        }
        const connection = [...this.#connections].find(c => c.proxyId === proxyId && c.connected);
        if (!connection) throw new BleError("Unknown or disconnected BLE proxy");
        const abort = new AbortController();
        const selection: ProxySelection = {
            connection,
            seen: new Set(),
            abort,
            signal: signal ? AbortSignal.any([signal, abort.signal]) : abort.signal,
            channels: new Set(),
            opening: new Set(),
        };
        this.#selection = selection;
        return this.#selectionContext.run(selection, async () => {
            let outcome: { ok: true; value: T } | { ok: false; error: unknown };
            try {
                // A completed operation stays a success even if the proxy left afterwards: a commissioned node exists.
                outcome = { ok: true, value: await operation(selection.signal) };
            } catch (error) {
                outcome = { ok: false, error };
            }
            // The SDK may have stopped waiting for an open that is still talking to the proxy.
            abort.abort(new BleError("BLE proxy selection ended"));
            const cleanupErrors: unknown[] = [];
            try {
                await Promise.allSettled(selection.opening);
                const closed = await Promise.allSettled(
                    [...selection.channels].filter(c => c.connected).map(c => Promise.resolve().then(() => c.close())),
                );
                for (const result of closed) {
                    if (result.status === "rejected") cleanupErrors.push(result.reason);
                }
                await this.stopScan();
            } catch (error) {
                cleanupErrors.push(error);
            } finally {
                this.#selection = undefined;
            }
            if (cleanupErrors.length) {
                logger.warn(`BLE selection cleanup had ${cleanupErrors.length} failure(s)`, cleanupErrors);
            }
            if (!outcome.ok) throw outcome.error;
            return outcome.value;
        });
    }

    /** Only validated advertisements enter the selected operation's freshness set. */
    recordDiscovery(address: string, connection: BleProxyConnection): boolean {
        const selection = this.#selection;
        if (!selection) return true;
        if (selection.connection !== connection || selection.signal.aborted) return false;
        selection.seen.add(address);
        return true;
    }

    trackChannel(channel: { connected: boolean; close(): Promise<void> }): void {
        this.#currentSelection()?.channels.add(channel);
    }

    assertOperationActive(): void {
        this.#currentSelection()?.signal.throwIfAborted();
    }

    /** A selection must not start halfway through an existing unselected GATT handshake. */
    async withChannelOpen<T>(operation: () => Promise<T>): Promise<T> {
        const selection = this.#currentSelection();
        selection?.signal.throwIfAborted();
        this.#openingChannels++;
        const opening = operation();
        selection?.opening.add(opening);
        try {
            return await opening;
        } finally {
            this.#openingChannels--;
            selection?.opening.delete(opening);
        }
    }

    #currentSelection(): ProxySelection | undefined {
        const context = this.#selectionContext.getStore();
        if (context !== this.#selection) {
            throw new BleError("BLE proxy is busy or selection has expired");
        }
        return context;
    }

    #selectedOwner(address: string): BleProxyConnection | undefined {
        const selection = this.#selection;
        return selection && !selection.signal.aborted && selection.connection.connected && selection.seen.has(address)
            ? selection.connection
            : undefined;
    }

    /** Discovery callbacks arrive on the WebSocket's IO context, not the operation's async context. */
    isPeripheralReachable(address: string): boolean {
        return this.#selection ? this.#selectedOwner(address) !== undefined : this.getOwner(address) !== undefined;
    }

    async register(server: HttpServer): Promise<void> {
        const isFirstBind = this.#wss === undefined;
        const wss = (this.#wss ??= new WebSocketServer({ noServer: true }));
        const upgradeHandler = (
            req: { url?: string; _matterHandledUpgrade?: boolean },
            socket: unknown,
            head: unknown,
        ) => {
            logger.debug(`Upgrade request received: ${req.url}`);
            const path = req.url?.split("?")[0];
            if (path === "/ble") {
                req._matterHandledUpgrade = true;
                wss.handleUpgrade(
                    req as Parameters<typeof wss.handleUpgrade>[0],
                    socket as Parameters<typeof wss.handleUpgrade>[1],
                    head as Parameters<typeof wss.handleUpgrade>[2],
                    ws => wss.emit("connection", ws, req),
                );
            }
        };
        server.on("upgrade", upgradeHandler);
        this.#removeUpgradeListeners.push(() => server.removeListener("upgrade", upgradeHandler));
        logger.info("BLE proxy WebSocket endpoint registered on /ble");

        if (!isFirstBind) {
            return;
        }

        wss.on("connection", ws => {
            if (this.#closed) {
                ws.close();
                return;
            }

            const connection = new BleProxyConnection(ws);
            this.#connections.add(connection);

            connection.handshakeCompleted.on(() => this.#onHandshakeCompleted(connection));
            connection.eventReceived.on((event, data) => this.#onConnectionEvent(connection, event, data));
            connection.closed.on(() => this.#onConnectionClosed(connection));
        });
    }

    unregister(): Promise<void> {
        if (!this.#wss || this.#closed) {
            return Promise.resolve();
        }

        this.#closed = true;
        for (const remove of this.#removeUpgradeListeners) remove();
        this.#removeUpgradeListeners = [];

        for (const connection of this.#connections) {
            connection.close();
        }
        this.#connections.clear();
        this.#scanning.clear();
        this.#owners.clear();

        const wss = this.#wss;
        this.#wss = undefined;

        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.close();
            }
        });

        return new Promise<void>((resolve, reject) => {
            wss.close(err => (err ? reject(err) : resolve()));
        });
    }

    /**
     * Returns the connected connection that owns `address`. An owner that is no longer connected but whose close has
     * not been processed yet hands ownership to a connected seer, so the peripheral stays reachable meanwhile.
     */
    getOwner(address: string): BleProxyConnection | undefined {
        const selection = this.#currentSelection();
        if (selection) {
            return this.#selectedOwner(address);
        }
        const entry = this.#owners.get(address);
        if (entry === undefined) return undefined;
        if (!entry.owner.connected) {
            const seer = this.#connectedSeer(entry.seers);
            if (seer === undefined) return undefined;
            entry.owner = seer;
        }
        return entry.owner;
    }

    #connectedSeer(seers: Set<BleProxyConnection>): BleProxyConnection | undefined {
        for (const seer of seers) {
            if (seer.connected) return seer;
        }
        return undefined;
    }

    async startScan(args: StartScanArgs): Promise<void> {
        const selection = this.#currentSelection();
        selection?.signal.throwIfAborted();
        if (!this.#scanActive) {
            this.#forgetUnreportedOwners();
        }
        this.#scanActive = true;
        this.#scanArgs = args;
        const sends = new Array<Promise<unknown>>();
        for (const connection of this.#connections) {
            if (connection.connected && (!selection || connection === selection.connection)) {
                this.#scanning.add(connection);
                sends.push(
                    connection.sendCommand(BleProxyCommand.StartScan, args).catch(err => {
                        this.#onStartScanRejected(connection, err);
                    }),
                );
            }
        }
        await Promise.all(sends);
    }

    async stopScan(): Promise<void> {
        const selection = this.#currentSelection();
        this.#scanActive = false;
        this.#scanStoppedAt = Time.nowUs;
        const sends = new Array<Promise<unknown>>();
        for (const connection of this.#connections) {
            if (connection.connected && (!selection || connection === selection.connection)) {
                sends.push(
                    connection.sendCommand(BleProxyCommand.StopScan).catch(err => {
                        if (isErrorCode(err, BleProxyErrorCode.NotScanning)) {
                            logger.debug(`[${connection.id}] was not scanning`);
                            return;
                        }
                        logger.warn(`[${connection.id}] Failed to stop scan:`, err);
                    }),
                );
            }
        }
        this.#scanning.clear();
        await Promise.all(sends);
    }

    #onHandshakeCompleted(connection: BleProxyConnection): void {
        this.connectionEstablished.emit();
        if (this.#scanActive && this.#scanArgs && !this.#selection) {
            this.#scanning.add(connection);
            connection.sendCommand(BleProxyCommand.StartScan, this.#scanArgs).catch(err => {
                this.#onStartScanRejected(connection, err);
            });
        }
    }

    #onConnectionEvent(connection: BleProxyConnection, event: string, data: Record<string, unknown>): void {
        if (event === BleProxyEvent.DeviceDiscovered) {
            this.#onDeviceDiscovered(connection, data as unknown as DeviceDiscoveredData);
        } else if (event === BleProxyEvent.ScanStopped) {
            this.#markNotScanning(connection, (data as { reason?: string }).reason ?? "unknown");
        }
    }

    #onStartScanRejected(connection: BleProxyConnection, err: unknown): void {
        // `already_scanning` means the client is scanning. A client that cannot re-arm keeps the
        // parameters of its running scan, so this is only equivalent while the args do not change.
        if (isErrorCode(err, BleProxyErrorCode.AlreadyScanning)) {
            logger.debug(`[${connection.id}] was already scanning`);
            return;
        }
        logger.warn(`[${connection.id}] Failed to start scan:`, err);
        this.#markNotScanning(connection, "start scan failed");
    }

    /** Drop a connection from the scanning set; emit aggregate scanStopped once none remain. */
    #markNotScanning(connection: BleProxyConnection, reason: string): void {
        if (!this.#scanning.delete(connection)) {
            return;
        }
        if (this.#scanning.size === 0) {
            this.scanStopped.emit(reason);
        }
    }

    #onDeviceDiscovered(connection: BleProxyConnection, data: DeviceDiscoveredData): void {
        let entry = this.#owners.get(data.address);
        if (!entry) {
            entry = { owner: connection, seers: new Set([connection]), lastReportedAt: Time.nowUs };
            this.#owners.set(data.address, entry);
        } else {
            entry.seers.add(connection);
            entry.lastReportedAt = Time.nowUs;
            if (!entry.owner.connected) {
                entry.owner = connection;
            }
        }
        logger.debug(
            `[${connection.id}] device_discovered ${data.address} rssi=${data.rssi ?? "n/a"} seers=${entry.seers.size} isOwner=${entry.owner === connection}`,
        );
        this.deviceDiscovered.emit(data, connection);
    }

    #forgetUnreportedOwners(): void {
        const now = Time.nowUs;
        if (this.#scanStoppedAt !== undefined && Timestamp.delta(this.#scanStoppedAt, now) <= OWNER_MAX_AGE) {
            return;
        }
        for (const [address, { lastReportedAt }] of this.#owners) {
            if (Timestamp.delta(lastReportedAt, now) > OWNER_MAX_AGE) {
                this.#owners.delete(address);
            }
        }
    }

    #onConnectionClosed(connection: BleProxyConnection): void {
        if (this.#selection?.connection === connection) {
            this.#selection.abort.abort(new BleError("Selected BLE proxy disconnected"));
        }
        this.#connections.delete(connection);
        this.#markNotScanning(connection, "client disconnected");

        for (const [address, entry] of this.#owners) {
            entry.seers.delete(connection);
            if (entry.owner === connection) {
                const next = this.#connectedSeer(entry.seers);
                if (next) {
                    entry.owner = next;
                    logger.info(`Reassigned ownership of ${address} to [${next.id}]`);
                } else {
                    this.#owners.delete(address);
                }
            }
        }
    }
}
