"""
consumer.py
-----------
Kafka consumer that reads messages from the raw topic and writes them
to the data lake as Parquet files.  This file provides the streaming
path when Kafka is enabled.

Run (after starting Kafka + producer with Kafka enabled):
  python ingestion/consumer.py

Design notes
------------
- Groups messages into micro-batches (FLUSH_EVERY rows) before writing
  Parquet, reducing small-file overhead.
- Uses manual offset commits so no message is lost if the process
  crashes mid-batch.
- In production, replace the flat list buffer with a proper queue
  (e.g. multiprocessing.Queue) for concurrent consumers.
"""

import json
import signal
import sys
from collections import deque
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import get_config, get_logger

logger = get_logger("consumer")
cfg    = get_config()

FLUSH_EVERY = cfg["ingestion"]["batch_size"]
RUNNING     = True


def _shutdown(sig, frame):
    global RUNNING
    logger.info("Shutdown signal received — draining buffer…")
    RUNNING = False


signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def write_batch(buffer: list, batch_num: int) -> None:
    """Persist a list of dicts as a Parquet partition in the data lake."""
    ts  = datetime.utcnow()
    partition = ts.strftime(cfg["ingestion"]["partition_format"])
    dest = Path(cfg["paths"]["raw_data_lake"]) / partition
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / f"batch_{batch_num:04d}.parquet"

    df = pd.DataFrame(buffer)
    df["_ingested_at"] = ts.isoformat()
    df["_batch_id"]    = batch_num
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("Written batch %d → %s (%d rows)", batch_num, path, len(df))


def consume() -> None:
    """
    Main consumer loop.

    Uncomment the kafka-python block and comment out the simulation
    block once Kafka is running.
    """
    # ── Real Kafka path ───────────────────────────────────────────────────
    # from kafka import KafkaConsumer
    # consumer = KafkaConsumer(
    #     cfg["kafka"]["topic"],
    #     bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
    #     group_id=cfg["kafka"]["group_id"],
    #     auto_offset_reset=cfg["kafka"]["auto_offset_reset"],
    #     enable_auto_commit=False,
    #     value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    # )

    logger.info("Consumer started (simulation mode — swap for real Kafka consumer)")
    buffer: list[dict] = []
    batch_num = 0

    # ── Simulation: replay parquet files already on disk ─────────────────
    raw_files = sorted(Path(cfg["paths"]["raw_data_lake"]).rglob("*.parquet"))
    for pf in raw_files:
        df = pd.read_parquet(pf)
        for record in df.to_dict(orient="records"):
            buffer.append(record)
            if len(buffer) >= FLUSH_EVERY:
                write_batch(buffer, batch_num)
                buffer.clear()
                batch_num += 1

    if buffer:                        # flush remainder
        write_batch(buffer, batch_num)

    # ── Real Kafka loop (uncomment) ───────────────────────────────────────
    # for message in consumer:
    #     buffer.append(message.value)
    #     if len(buffer) >= FLUSH_EVERY or not RUNNING:
    #         write_batch(buffer, batch_num)
    #         consumer.commit()
    #         buffer.clear()
    #         batch_num += 1
    #     if not RUNNING:
    #         break
    # consumer.close()

    logger.info("Consumer finished.")


if __name__ == "__main__":
    consume()
