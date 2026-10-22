"""Selected-proxy capability checks, cancellation and real Python-to-JS API round trips."""

from __future__ import annotations

import asyncio
import contextlib
import json
from pathlib import Path
import select
import subprocess
import time
from typing import TYPE_CHECKING, Any, TypedDict, cast
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from matter_server.client import MatterClient
from matter_server.client.exceptions import ServerVersionTooOld
from matter_server.common.errors import BleProxyBusy, MatterError
from matter_server.common.models import APICommand, ServerInfoMessage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


def client_with_capability(enabled: bool) -> MatterClient:
    client = MatterClient.__new__(MatterClient)
    client.connection = MagicMock()
    client.connection.server_info = ServerInfoMessage(
        fabric_id=1,
        compressed_fabric_id=1,
        schema_version=11,
        min_supported_schema_version=11,
        sdk_version="local-test",
        wifi_credentials_set=False,
        thread_credentials_set=False,
        bluetooth_enabled=True,
        ble_proxy_selection=enabled,
    )
    client.send_command = AsyncMock(
        return_value={
            "node_id": 1,
            "date_commissioned": "2026-10-18T00:00:00",
            "last_interview": "2026-10-18T00:00:00",
            "interview_version": 1,
            "attributes": {},
            "attribute_subscriptions": [],
        }
    )
    client.send_command_no_wait = AsyncMock()
    return client


async def test_refuses_unadvertised_selection_before_sending() -> None:
    client = client_with_capability(False)
    with pytest.raises(ServerVersionTooOld):
        await client.commission_with_code("synthetic", ble_proxy_id="local-phone")
    cast(AsyncMock, client.send_command).assert_not_awaited()


async def test_selection_is_feature_gated_independently_of_schema_version() -> None:
    client = client_with_capability(True)
    await client.commission_with_code("synthetic", ble_proxy_id="local-phone")
    args, kwargs = cast(AsyncMock, client.send_command).call_args
    assert args[0] == APICommand.COMMISSION_WITH_CODE
    assert kwargs["ble_proxy_id"] == "local-phone"
    assert kwargs["require_schema"] is None
    assert kwargs["network_only"] is False


async def test_network_only_cannot_silently_ignore_selected_phone() -> None:
    client = client_with_capability(True)
    with pytest.raises(ValueError, match="network_only"):
        await client.commission_with_code("synthetic", network_only=True, ble_proxy_id="local-phone")
    cast(AsyncMock, client.send_command).assert_not_awaited()


async def test_explicit_cancel_is_capability_checked() -> None:
    client = client_with_capability(False)
    with pytest.raises(ServerVersionTooOld):
        await client.cancel_commissioning("local-phone")
    cast(AsyncMock, client.send_command).assert_not_awaited()
    assert client.connection.server_info is not None
    client.connection.server_info.ble_proxy_selection = True
    cast(AsyncMock, client.send_command).return_value = {"canceled": True}
    assert await client.cancel_commissioning("local-phone") is True
    cast(AsyncMock, client.send_command).assert_awaited_once_with(
        APICommand.CANCEL_COMMISSIONING, ble_proxy_id="local-phone"
    )


@pytest.mark.parametrize("cancel_send_fails", [False, True])
async def test_task_cancel_requests_remote_cleanup_and_preserves_cancellation(cancel_send_fails: bool) -> None:
    client = client_with_capability(True)
    client.cancel_commissioning = AsyncMock(return_value=True)
    entered = asyncio.Event()

    async def pending(*args: Any, **kwargs: Any) -> None:
        entered.set()
        await asyncio.Future()

    cast(AsyncMock, client.send_command).side_effect = pending
    if cancel_send_fails:
        client.cancel_commissioning.side_effect = ConnectionError("test transport closed")
    task = asyncio.create_task(client.commission_with_code("synthetic", ble_proxy_id="local-phone"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    client.cancel_commissioning.assert_awaited_once_with("local-phone")


async def test_caller_timeout_also_requests_remote_cleanup() -> None:
    client = client_with_capability(True)
    client.cancel_commissioning = AsyncMock(return_value=True)

    async def pending(*args: Any, **kwargs: Any) -> None:
        await asyncio.Future()

    cast(AsyncMock, client.send_command).side_effect = pending
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            await client.commission_with_code("synthetic", ble_proxy_id="local-phone")
    client.cancel_commissioning.assert_awaited_once()


class ActualApi(TypedDict):
    url: str
    proxy_id: str
    process: subprocess.Popen[str]


@pytest.fixture
def actual_api() -> Iterator[ActualApi]:
    """Start the real JS WS handler and BLE router, with only the Matter device work simulated."""
    root = Path(__file__).resolve().parents[2]
    process = subprocess.Popen(
        ["node", "--import", "tsx", "packages/ble-proxy/test/PythonBleApiServer.ts"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([process.stdout], [], [], 0.1)
            if not ready:
                if process.poll() is not None:
                    raise RuntimeError("Local JS API process exited before readiness")
                continue
            line = process.stdout.readline()
            if line.startswith("READY:"):
                info = json.loads(line.removeprefix("READY:"))
                yield ActualApi(url=info["url"], proxy_id=info["proxy_id"], process=process)
                return
        raise RuntimeError("Local JS API process did not become ready")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


@contextlib.asynccontextmanager
async def live_client(url: str) -> AsyncIterator[MatterClient]:
    async with aiohttp.ClientSession() as session:
        client = MatterClient(url, session)
        ready = asyncio.Event()
        task = asyncio.create_task(client.start_listening(ready))
        try:
            async with asyncio.timeout(5):
                await ready.wait()
            yield client
        finally:
            await client.disconnect()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def test_python_round_trip_through_actual_js_commissioning_api(actual_api: ActualApi) -> None:
    async with live_client(actual_api["url"]) as client:
        assert client.server_info is not None
        assert client.server_info.ble_proxy_selection is True
        node = await client.commission_with_code("synthetic", ble_proxy_id=actual_api["proxy_id"])
        assert node.available is True
        assert await client.cancel_commissioning(actual_api["proxy_id"]) is False


@pytest.mark.parametrize("cancel_task", [False, True])
async def test_python_cancel_reaches_actual_js_and_allows_a_new_operation(
    actual_api: ActualApi, cancel_task: bool
) -> None:
    async with live_client(actual_api["url"]) as client:
        task = asyncio.create_task(client.commission_with_code("wait", ble_proxy_id=actual_api["proxy_id"]))

        output = actual_api["process"].stdout
        assert output is not None

        def entered() -> None:
            for line in output:
                if line.startswith("COMMISSION_ENTERED"):
                    return
            raise RuntimeError("Server closed before commissioning started")

        async with asyncio.timeout(3):
            await asyncio.to_thread(entered)
        if cancel_task:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert await client.cancel_commissioning(actual_api["proxy_id"]) is True
            with pytest.raises(MatterError):
                await task
        node = await client.commission_with_code("synthetic", ble_proxy_id=actual_api["proxy_id"])
        assert node.available is True


async def test_python_sees_a_held_proxy_as_busy(actual_api: ActualApi) -> None:
    async with live_client(actual_api["url"]) as client:
        task = asyncio.create_task(client.commission_with_code("wait", ble_proxy_id=actual_api["proxy_id"]))
        output = actual_api["process"].stdout
        assert output is not None

        def entered() -> None:
            for line in output:
                if line.startswith("COMMISSION_ENTERED"):
                    return
            raise RuntimeError("Server closed before commissioning started")

        async with asyncio.timeout(3):
            await asyncio.to_thread(entered)
        with pytest.raises(BleProxyBusy) as busy:
            await client.commission_with_code("synthetic", ble_proxy_id=actual_api["proxy_id"])
        assert busy.value.error_code == 107
        assert await client.cancel_commissioning(actual_api["proxy_id"]) is True
        with pytest.raises(MatterError):
            await task
