import argparse
import json
import time
from datetime import datetime
from pathlib import Path
import pandas as pd

# ── local imports ───────────────────────────────────────────────────────────
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import get_config, get_logger

logger = get_logger("producer")
cfg    = get_config()


def get_partition_path(base: Path, ts: datetime, batch_num: int) -> Path:
    """Return an hourly-partitioned output path for the given timestamp."""
    partition = ts.strftime(cfg["ingestion"]["partition_format"])
    dest = base / partition
    dest.mkdir(parents=True, exist_ok=True)
    return dest / f"batch_{batch_num:04d}.parquet"


def produce(source_path: str | None = None) -> None:
    """
    Read source CSV in batches; write each batch to the data lake.
    Simulates a streaming source by sleeping between batches.
    """
    source = Path(source_path or cfg["paths"]["source_dataset"])
    raw_base = Path(cfg["paths"]["raw_data_lake"])
    batch_size = cfg["ingestion"]["batch_size"]
    interval   = cfg["ingestion"]["batch_interval_seconds"]

    logger.info("Producer starting | source=%s | batch_size=%d", source, batch_size)

    if not source.exists():
        raise FileNotFoundError(f"Source dataset not found: {source}")

    # Read full dataset — in production this would be a DB/API/socket read
    df = pd.read_csv(source, dtype=str)   # read as str; typing happens in processing
    total_rows    = len(df)
    total_batches = (total_rows + batch_size - 1) // batch_size

    logger.info("Loaded %d rows → %d batches", total_rows, total_batches)

    for batch_num, start in enumerate(range(0, total_rows, batch_size)):
        batch  = df.iloc[start : start + batch_size].copy()
        ts     = datetime.utcnow()
        out    = get_partition_path(raw_base, ts, batch_num)

        # ── Write immutable Parquet ───────────────────────────────────────
        batch["_ingested_at"] = ts.isoformat()
        batch["_batch_id"]    = batch_num
        batch.to_parquet(out, index=False, engine="pyarrow")

        logger.info(
            "Batch %d/%d → %s  (%d rows)",
            batch_num + 1, total_batches, out, len(batch),
        )

        # ── Optional: send to Kafka ───────────────────────────────────────
        # for record in batch.to_dict(orient="records"):
        #     kafka_producer.send(cfg["kafka"]["topic"], value=record)
        # kafka_producer.flush()

        # Simulate streaming delay (skip on last batch)
        if batch_num < total_batches - 1:
            time.sleep(interval)

    logger.info("Producer finished. All %d batches written.", total_batches)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Data Lake Producer")
    parser.add_argument("--source", help="Override source CSV path")
    args = parser.parse_args()
    produce(args.source)
