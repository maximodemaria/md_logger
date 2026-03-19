"""
monitores.py — Monitoreo de métricas en hilo separado
======================================================
Contiene:
  - WorkerStats:    Encapsula métricas de rendimiento del worker.
  - metrics_thread: Hilo que procesa las métricas de forma asíncrona.
"""

from __future__ import annotations

import logging
import queue
import threading
import time

from config import GAP_WARN_SECS, HEALTH_LOG_EVERY


class WorkerStats:
    """
    Encapsula las métricas de rendimiento de un worker.
    Centraliza la lógica de actualización y verificación de inactividad.
    """

    def __init__(self) -> None:
        self.msgs: int = 0
        self.total_latency_ms: float = 0.0
        self.last_msg_time: float = time.monotonic()
        self.max_gap: float = 0.0

    def update(self, latency_ms: float, gap: float) -> None:
        """Actualiza contadores tras recibir un mensaje."""
        self.msgs += 1
        self.total_latency_ms += latency_ms
        self.last_msg_time = time.monotonic()
        if gap > self.max_gap:
            self.max_gap = gap

    def avg_latency(self) -> float:
        """Retorna la latencia promedio en ms."""
        return self.total_latency_ms / self.msgs if self.msgs > 0 else 0.0

    def is_inactive(self, timeout: float) -> bool:
        """Retorna True si el worker lleva más de 'timeout' segundos sin mensajes."""
        if self.msgs == 0:
            return False
        return (time.monotonic() - self.last_msg_time) > timeout


def metrics_thread(
    metrics_queue: queue.Queue[tuple[float, float]],
    stats: WorkerStats,
    worker_id: int,
    logger: logging.Logger,
    stop_event: threading.Event,
) -> None:
    """
    Hilo que consume (latency, gap) de la cola y actualiza las estadísticas.
    También se encarga de loguear el estado de salud periódicamente.
    """
    logger.info("📊 Hilo de métricas iniciado para Worker %d.", worker_id)

    while not stop_event.is_set():
        try:
            # Esperar una métrica (timeout corto para chequear stop_event)
            latency_ms, gap = metrics_queue.get(timeout=0.5)
            # Advertencia de gap si es necesario
            if gap > GAP_WARN_SECS and stats.msgs > 0:
                logger.warning(
                    "⚠️ Gap de %.3fs detectado en W%d", gap, worker_id
                )

            stats.update(latency_ms, gap)

            # Reporte de salud periódico
            if stats.msgs % HEALTH_LOG_EVERY == 0:
                logger.info(
                    "📊 [STATUS W%d] Msgs: %d | LatProm: %.1fms | Gap Máx: %.3fs | "
                    "Cola Métricas: %d",
                    worker_id,
                    stats.msgs,
                    stats.avg_latency(),
                    stats.max_gap,
                    metrics_queue.qsize(),
                )
        except queue.Empty:
            continue
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("❌ Error en hilo de métricas W%d: %s", worker_id, exc)

    logger.info("✅ Hilo de métricas W%d terminado.", worker_id)
