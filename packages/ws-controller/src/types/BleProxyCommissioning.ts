/**
 * @license
 * Copyright 2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

/** Injected by the server to keep ws-controller independent of the BLE transport package. */
export interface BleProxyCommissioning {
    withProxySelection<T>(
        proxyId: string,
        operation: (signal: AbortSignal) => Promise<T>,
        signal?: AbortSignal,
    ): Promise<T>;
}
