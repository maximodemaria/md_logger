"""
worker.py — Proceso hijo (Productor WebSocket + Consumidor Parquet)
===================================================================
Contiene:
  - worker_process: Coordinador principal del proceso hijo.
"""

from __future__ import annotations

import logging
import multiprocessing
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pyRofex

from config import BUFFER_SIZE, PARQUET_DIR, TIMEOUT_INACTIVO
from conexion import iniciar_conexion, suscribir_tickers
from handlers import WebSocketHandler
from logger import consumer_thread
from stats import WorkerStats, metrics_thread


def worker_process(
    worker_id: int,
    symbols: list[str],
    credentials: dict[str, str],
    stop_event: multiprocessing.Event,  # type: ignore[type-arg]
    metrics_central_queue: multiprocessing.Queue | None = None,
) -> None:
    """
    Coordinador del proceso hijo. Cada worker tiene su propio espacio de memoria.

    Flujo:
      1. Configura logging local.
      2. Autentica con el broker (iniciar_conexion).
      3. Inicia hilos de Consumo (Parquet) y Monitoreo (Métricas).
      4. Suscribe tickers.
      5. Bucle de salud hasta recibir stop_event o inactividad.
      6. Cierre ordenado.
    """
    # ── 1. Setup ──────────────────────────────────────────────────────────
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [W{worker_id}] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger(f"worker.{worker_id}")
    logger.info("🚀 Worker %d iniciando. Tickers: %d", worker_id, len(symbols))

    # Colas y directorios
    parquet_dir = Path(PARQUET_DIR)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    data_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=BUFFER_SIZE * 5)
    metrics_queue: queue.Queue[tuple[float, float]] = queue.Queue(maxsize=1000)

    # ── 2. Conexión ───────────────────────────────────────────────────────
    try:
        iniciar_conexion(
            credentials["user"],
            credentials["password"],
            credentials["broker"]
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("💥 Worker %d error de conexión: %s", worker_id, exc)
        return

    # ── 3. Operativa ──────────────────────────────────────────────────────
    stats = WorkerStats()
    handler = WebSocketHandler(worker_id, logger, data_queue, metrics_queue)

    try:
        pyRofex.init_websocket_connection(
            market_data_handler=handler.on_message,
            error_handler=handler.on_error,
        )
        logger.info("🔌 WebSocket conectado en Worker %d.", worker_id)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("💥 Worker %d no pudo iniciar WebSocket: %s", worker_id, exc)
        return

    stop_threads = threading.Event()
    consumer = threading.Thread(
        target=consumer_thread,
        args=(data_queue, worker_id, stop_threads, parquet_dir),
        name=f"consumer-w{worker_id}",
    )
    metrics_monitor = threading.Thread(
        target=metrics_thread,
        args=(metrics_queue, stats, worker_id, logger, stop_threads, metrics_central_queue),
        name=f"metrics-w{worker_id}",
    )

    try:
        consumer.start()
        metrics_monitor.start()

        # Suscripción
        suscribir_tickers(symbols, worker_id, logger)

        # ── 4. Ciclo de Vida ──────────────────────────────────────────────────
        last_health_check = time.monotonic()
        while not stop_event.is_set():
            time.sleep(1.0)
            if time.monotonic() - last_health_check > 15:
                if stats.is_inactive(TIMEOUT_INACTIVO):
                    logger.warning("⚠️ Worker %d inactivo por timeout.", worker_id)
                    break
                last_health_check = time.monotonic()
    except KeyboardInterrupt:
        logger.warning("⚠️ Worker %d interrumpido por señal (Ctrl+C).", worker_id)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("💥 Worker %d error en ejecución: %s", worker_id, exc)
    finally:
        # ── 5. Shutdown ───────────────────────────────────────────────────────
        logger.info("🛑 Worker %d: iniciando apagado...", worker_id)
        try:
            pyRofex.close_websocket_connection()
        except Exception:  # pylint: disable=broad-except
            pass

        stop_threads.set()
        # Join con timeouts para evitar bloqueos eternos
        consumer.join(timeout=10)
        metrics_monitor.join(timeout=5)
        logger.info("✅ Worker %d apagado correctamente.", worker_id)
