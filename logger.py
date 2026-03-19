"""
logger.py — Hilo consumidor (persistencia a Parquet)
=====================================================
Contiene: consumer_thread — escribe los mensajes del buffer a disco en formato Parquet.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import BUFFER_SIZE


def consumer_thread(
    msg_queue: queue.Queue,
    worker_id: int,
    stop_event: threading.Event,
    parquet_dir: Path,
) -> None:
    """
    Hilo secundario que drena la cola y persiste chunks a Parquet (zstd).

    Estrategia:
      - Acumula mensajes en un buffer de lista.
      - Al alcanzar BUFFER_SIZE o al recibir señal de parada, vuelca a Parquet.
      - Serializa campos dinámicos (marketData, instrumentId) con json.dumps
        para prevenir errores de esquema en PyArrow.
    """
    logger = logging.getLogger(f"consumer.w{worker_id}")
    buffer: list[dict[str, Any]] = []
    chunk_index: int = 0

    def flush_buffer() -> None:
        nonlocal chunk_index
        if not buffer:
            return
        try:
            df = pd.DataFrame(buffer)

            # Serializar campos de esquema dinámico/anidado a string plano
            for col in ("marketData", "instrumentId"):
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda v: json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (dict, list)) else str(v)
                    )

            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            fname = f"chunk_w{worker_id}_{timestamp_str}_{chunk_index:05d}.parquet"
            filename = parquet_dir / fname

            df.to_parquet(
                filename,
                engine="pyarrow",
                compression="zstd",
                index=False,
            )
            logger.info(
                "💾 Chunk guardado: %s | Filas: %d | Cola restante: %d",
                filename.name,
                len(buffer),
                msg_queue.qsize(),
            )
        except RuntimeError as rex:
            if "interpreter shutdown" in str(rex):
                logger.info("ℹ️ Parquet omitido: Intérprete cerrando.")
            else:
                logger.error("❌ RuntimeError al guardar Parquet: %s", rex)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("❌ Error al guardar Parquet: %s", exc, exc_info=True)
        finally:
            buffer.clear()
            chunk_index += 1

    while not stop_event.is_set():
        try:
            msg = msg_queue.get(timeout=0.5)
            buffer.append(msg)
            if len(buffer) >= BUFFER_SIZE:
                flush_buffer()
        except queue.Empty:
            continue

    # ── Graceful drain: vaciar lo que quede en la cola antes de terminar ──
    logger.info("🔄 Worker %d: vaciando cola residual (%d msgs)...", worker_id, msg_queue.qsize())
    while True:
        try:
            msg = msg_queue.get_nowait()
            buffer.append(msg)
            if len(buffer) >= BUFFER_SIZE:
                flush_buffer()
        except queue.Empty:
            break

    flush_buffer()  # Último chunk parcial
    logger.info("✅ Worker %d: consumidor terminado. Chunks escritos: %d", worker_id, chunk_index)
