"""Record Binance spot order book updates and trades to Parquet files.

Usage:
    python -m mmsim.recorder --symbol BTCUSDT --out data/raw

Stop with Ctrl-C (or SIGTERM); the in-memory buffer is flushed on exit.

What gets stored (one row per websocket message, raw JSON kept as-is):
    recv_ns   local receive time, nanoseconds since epoch
    kind      "depth" | "trade" | "snapshot"
    event_ms  exchange event time in ms (0 for snapshots)
    first_id  depth: first update id (U); trade: trade id; snapshot: 0
    final_id  depth: last update id (u);  trade: trade id; snapshot: lastUpdateId
    payload   the raw JSON string

A REST snapshot is stored every time the websocket (re)connects. Day 2 uses the
snapshots plus the update ids to rebuild the order book and detect gaps.
Needs Python 3.11+.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import websockets

WS_URL = "wss://stream.binance.com:9443/stream?streams={s}@depth@100ms/{s}@trade"
SNAPSHOT_URL = "https://api.binance.com/api/v3/depth?symbol={S}&limit=1000"
FLUSH_SECONDS = 60
FLUSH_ROWS = 50_000

SCHEMA = pa.schema(
    [
        ("recv_ns", pa.int64()),
        ("kind", pa.string()),
        ("event_ms", pa.int64()),
        ("first_id", pa.int64()),
        ("final_id", pa.int64()),
        ("payload", pa.string()),
    ]
)


class Writer:
    """Collects rows in memory and writes a new Parquet part file per flush."""

    def __init__(self, out_dir: Path, symbol: str) -> None:
        self.dir = out_dir / symbol.upper()
        self.rows: list[tuple] = []
        self.last_flush = time.monotonic()

    def add(self, row: tuple) -> None:
        self.rows.append(row)

    def due(self) -> bool:
        return (
            len(self.rows) >= FLUSH_ROWS
            or time.monotonic() - self.last_flush >= FLUSH_SECONDS
        )

    def flush(self) -> None:
        self.last_flush = time.monotonic()
        if not self.rows:
            return
        columns = list(zip(*self.rows))
        table = pa.Table.from_arrays(
            [pa.array(col, type=field.type) for col, field in zip(columns, SCHEMA)],
            schema=SCHEMA,
        )
        now = datetime.now(timezone.utc)
        part_dir = self.dir / f"date={now:%Y-%m-%d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        stamp = f"{now:%H%M%S%f}"
        tmp = part_dir / f".part-{stamp}.tmp"
        final = part_dir / f"part-{stamp}.parquet"
        pq.write_table(table, tmp, compression="zstd")
        tmp.rename(final)  # rename last, so readers never see half-written files
        logging.info("wrote %d rows -> %s", len(self.rows), final)
        self.rows.clear()


def parse(msg: str, recv_ns: int) -> tuple | None:
    data = json.loads(msg)["data"]
    event = data.get("e")
    if event == "depthUpdate":
        return (recv_ns, "depth", data["E"], data["U"], data["u"], msg)
    if event == "trade":
        return (recv_ns, "trade", data["E"], data["t"], data["t"], msg)
    return None


def fetch_snapshot(symbol: str) -> str:
    url = SNAPSHOT_URL.format(S=symbol.upper())
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read().decode()


async def take_snapshot(symbol: str, writer: Writer) -> None:
    try:
        text = await asyncio.to_thread(fetch_snapshot, symbol)
        row = (time.time_ns(), "snapshot", 0, 0, json.loads(text)["lastUpdateId"], text)
        writer.add(row)
        logging.info("snapshot stored (lastUpdateId=%s)", row[4])
    except Exception:  # noqa: BLE001 - keep recording even if the snapshot fails
        logging.exception("snapshot failed")


async def record(symbol: str, writer: Writer) -> None:
    uri = WS_URL.format(s=symbol.lower())
    # "async for ... in connect()" reconnects automatically with backoff.
    async for ws in websockets.connect(uri, max_size=None):
        try:
            logging.info("connected: %s", uri)
            snapshot = asyncio.create_task(take_snapshot(symbol, writer))
            async for msg in ws:
                row = parse(msg, time.time_ns())
                if row is not None:
                    writer.add(row)
                if writer.due():
                    writer.flush()
            await snapshot
        except websockets.ConnectionClosed as exc:
            logging.warning("connection closed (%s), reconnecting", exc)


async def main(symbol: str, out_dir: Path) -> None:
    writer = Writer(out_dir, symbol)
    task = asyncio.create_task(record(symbol, writer))
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
    except NotImplementedError:  # Windows
        pass
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        writer.flush()
        logging.info("stopped, buffer flushed")


def cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(main(args.symbol, args.out))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
