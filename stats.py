"""
stats.py — Monitoreo de métricas en hilo separado
======================================================
Contiene:
  - WorkerStats:    Encapsula métricas de rendimiento del worker.
  - metrics_thread: Hilo que procesa las métricas de forma asíncrona.
"""

from __future__ import annotations

import logging
import multiprocessing
import queue
import threading
import time
from datetime import datetime
from pathlib import Path

from config import GAP_WARN_SECS, LOGS_DIR, METRICS_INTERVAL_SECS


class WorkerStats:
    """
    Encapsula las métricas de rendimiento de un worker.
    """

    def __init__(self) -> None:
        self.msgs: int = 0
        self.total_latency_ms: float = 0.0
        self.last_msg_time: float = time.monotonic()
        self.max_gap: float = 0.0

    def update(self, latency_ms: float, gap: float) -> None:
        """Actualiza las estadísticas con los datos del último mensaje recibido."""
        self.msgs += 1
        self.total_latency_ms += latency_ms
        self.last_msg_time = time.monotonic()
        if gap > self.max_gap:
            self.max_gap = gap

    def avg_latency(self) -> float:
        """Retorna la latencia promedio en ms."""
        return self.total_latency_ms / self.msgs if self.msgs > 0 else 0.0

    def is_inactive(self, timeout: float) -> bool:
        """Verifica si el worker está inactivo por el tiempo especificado."""
        if self.msgs == 0:
            return False
        return (time.monotonic() - self.last_msg_time) > timeout


def metrics_thread(
    metrics_queue: queue.Queue[tuple[float, float]],
    stats: WorkerStats,
    worker_id: int,
    logger: logging.Logger,
    stop_event: threading.Event,
    central_queue: queue.Queue | None = None,
) -> None:
    """
    Hilo que consume métricas locales y reporta periódicamente a la cola central.
    """
    logger.info("📊 Hilo de métricas iniciado para Worker %d.", worker_id)
    last_report_time = time.monotonic()

    while not stop_event.is_set():
        try:
            latency_ms, gap = metrics_queue.get(timeout=0.2)
            if gap > GAP_WARN_SECS and stats.msgs > 0:
                logger.warning("⚠️ Gap de %.3fs detectado en W%d", gap, worker_id)

            stats.update(latency_ms, gap)
        except queue.Empty:
            pass
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("❌ Error en hilo de métricas W%d: %s", worker_id, exc)

        # Reporte temporal al agregador central
        if time.monotonic() - last_report_time >= METRICS_INTERVAL_SECS:
            if central_queue:
                central_queue.put({
                    "worker_id": worker_id,
                    "msgs": stats.msgs,
                    "avg_lat": stats.avg_latency(),
                    "max_gap": stats.max_gap,
                    "last_msg_mono": stats.last_msg_time
                })
            last_report_time = time.monotonic()

    logger.info("✅ Hilo de métricas W%d terminado.", worker_id)


def central_metrics_aggregator(
    central_queue: queue.Queue,
    stop_event: threading.Event
) -> None:
    """
    Hilo agregador (corre en el proceso principal).
    Consolida métricas de todos los workers y escribe un log unificado.
    """
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    fecha = datetime.now().strftime("%Y-%m-%d")
    log_file = Path(LOGS_DIR) / f"metrics_{fecha}.log"

    workers_data: dict[int, dict] = {}
    last_write_time = time.monotonic()
    last_total_msgs = 0

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"\n--- Sesión iniciada: {datetime.now().isoformat()} ---\n")

        while not stop_event.is_set() or not central_queue.empty():
            try:
                # Timeout dinámico: más corto si ya estamos en fase de apagado
                timeout = 0.05 if stop_event.is_set() else 0.2
                data = central_queue.get(timeout=timeout)
                workers_data[data["worker_id"]] = data
            except queue.Empty:
                if stop_event.is_set():
                    break # Salida rápida si no hay más datos y recibimos señal

            # Escribir consolidado cada segundo
            now = time.monotonic()
            if now - last_write_time >= METRICS_INTERVAL_SECS:
                if workers_data:
                    total_msgs = sum(d["msgs"] for d in workers_data.values())
                    msgs_diff = total_msgs - last_total_msgs

                    # Gap Global: Tiempo desde el mensaje más reciente de CUALQUIER worker
                    last_msg_global = max(d["last_msg_mono"] for d in workers_data.values())
                    global_gap = now - last_msg_global

                    # Latencia promedio del sistema (promedio simple de workers)
                    avg_lat = sum(d["avg_lat"] for d in workers_data.values()) / len(workers_data)

                    line = (
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        f"Msgs/s: {msgs_diff:,} | Total: {total_msgs:,} | "
                        f"Lat: {avg_lat:.1f}ms | Gap: {global_gap:.3f}s\n"
                    )
                    f.write(line)
                    f.flush()

                    last_total_msgs = total_msgs
                last_write_time = now

        f.write(f"--- Sesión finalizada: {datetime.now().isoformat()} ---\n")


def setup_central_metrics(stop_event: threading.Event) -> queue.Queue:
    """
    Configura la cola y el hilo agregador.
    """
    central_queue = multiprocessing.Queue()
    aggregator_thread = threading.Thread(
        target=central_metrics_aggregator,
        args=(central_queue, stop_event),
        name="MetricsAggregator",
        daemon=True
    )
    aggregator_thread.start()
    return central_queue
