/**
 * @license
 * Copyright 2026 Open Home Foundation
 * SPDX-License-Identifier: Apache-2.0
 */

import { BLE_PROXY_IDENTITY_FEATURE } from "../src/BleProxyProtocol.js";
import { createBleApiHarness } from "./BleApiHarness.js";
import { BleProxyTestClient } from "./BleProxyTestClient.js";

const h = await createBleApiHarness();
const phone = new BleProxyTestClient();
await phone.connect(`${h.url}/ble`, [BLE_PROXY_IDENTITY_FEATURE]);
h.onCommission(async request => {
    if ("manualCode" in request && request.manualCode === "wait") {
        console.log("COMMISSION_ENTERED");
        await new Promise<void>(resolve => request.abort!.addEventListener("abort", () => resolve(), { once: true }));
        request.abort!.throwIfAborted();
    }
});
console.log(`READY:${JSON.stringify({ url: `${h.url}/ws`, proxy_id: phone.helloResponse!.proxy_id })}`);
process.once("SIGTERM", () => {
    phone.close();
    void h.close().finally(() => process.exit(0));
});
