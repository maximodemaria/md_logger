"""
parquet_unifier.py — Consolidación y ordenamiento de chunks diarios.
===================================================================
Este script unifica los múltiples archivos Parquet generados por los workers
en un único archivo por día, garantizando el orden cronológico por timestamp
de llegada (_arrival_ts_ms) y eliminando los archivos temporales una vez validado.
"""

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("unifier")

def unify_day(date_str: str, base_dir: str, delete_chunks: bool = True):
    """
    Unifica los chunks de una fecha dada de forma incremental por lotes para ahorrar memoria.
    """
    batch_size = 100
    data_path = Path(base_dir)
    if not data_path.exists():
        logger.error("❌ El directorio %s no existe.", data_path)
        return

    # 1. Escaneo de Chunks
    pattern = f"chunk_w*_{date_str}_*.parquet"
    chunk_files = sorted(list(data_path.glob(pattern)))
    output_name = f"marketdata_{date_str}.parquet"
    output_path = data_path / output_name

    if not chunk_files:
        logger.warning("⚠️ No se encontraron nuevos chunks para la fecha %s.", date_str)
        return

    total_chunks = len(chunk_files)
    logger.info("📂 %d chunks encontrados para el %s. Iniciando unificación incremental...", total_chunks, date_str)

    # Dividir en lotes
    batches = [chunk_files[i : i + batch_size] for i in range(0, total_chunks, batch_size)]
    
    try:
        for idx, batch in enumerate(batches, 1):
            logger.info("📦 Procesando lote %d/%d (%d archivos nuevos)...", idx, len(batches), len(batch))
            all_dfs = []

            # A. Cargar archivo base existente
            if output_path.exists():
                try:
                    df_base = pd.read_parquet(output_path)
                    all_dfs.append(df_base)
                    logger.debug("   + Base cargada (%d registros)", len(df_base))
                except Exception as exc:
                    logger.error("   ❌ Error leyendo base: %s", exc)

            # B. Cargar chunks del lote
            for f in batch:
                try:
                    df_chunk = pd.read_parquet(f)
                    all_dfs.append(df_chunk)
                except Exception as exc:
                    logger.warning("   ⚠️ Error leyendo chunk %s: %s", f.name, exc)

            if not all_dfs:
                continue

            # C. Consolidación, Deduplicación y Ordenamiento
            full_df = pd.concat(all_dfs, ignore_index=True)
            mem_usage = full_df.memory_usage(deep=True).sum() / (1024 * 1024)
            logger.info("   ⚡ Memoria en RAM: %.2f MB | Registros totales: %d", mem_usage, len(full_df))
            
            full_df = full_df.drop_duplicates(subset=["_arrival_ts_ms", "instrumentId"])
            full_df = full_df.sort_values(by="_arrival_ts_ms", ascending=True)

            # D. Guardar (Sobrescribir parcial)
            full_df.to_parquet(
                output_path,
                engine="pyarrow",
                compression="zstd",
                index=False
            )
            logger.info("   💾 Guardado parcial en disk: %s", output_name)

            # Liberar memoria explícitamente
            del all_dfs
            del full_df
            import gc
            gc.collect()

        # 2. Validación de Seguridad (Post-procesamiento completo)
        logger.info("🛡️  Iniciando validación final de integridad...")
        
        # Leemos solo las claves para ahorrar memoria en la validación
        unified_keys = pd.read_parquet(output_path, columns=["_arrival_ts_ms", "instrumentId"])
        all_chunks_verified = True
        
        for f in chunk_files:
            df_chunk = pd.read_parquet(f, columns=["_arrival_ts_ms", "instrumentId"])
            merged = pd.merge(
                df_chunk,
                unified_keys,
                on=["_arrival_ts_ms", "instrumentId"],
                how="inner"
            )
            if len(merged) != len(df_chunk):
                logger.error("   ❌ El chunk %s no está totalmente contenido en el maestro.", f.name)
                all_chunks_verified = False
                break
        
        if all_chunks_verified:
            logger.info("✅ Validación exitosa. Todos los datos están asegurados.")
            if delete_chunks:
                logger.info("🧹 Eliminando %d chunks temporales...", total_chunks)
                for f in chunk_files:
                    f.unlink()
                logger.info("✅ Limpieza completada.")
        else:
            logger.warning("⚠️ No se eliminaron los chunks debido a fallas en la validación.")

    except Exception as exc:
        logger.error("💥 Error crítico (%s): %s", type(exc).__name__, exc, exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unificador de chunks MD Logger")
    parser.add_argument(
        "--date", type=str, help="Formato YYYYMMDD",
        default=datetime.now().strftime("%Y%m%d")
    )
    parser.add_argument(
        "--dir", type=str, help="Carpeta base de datos",
        default=os.path.join("data", "marketdata")
    )
    parser.add_argument(
        "--delete", action="store_true", help="Elimina los chunks tras el éxito"
    )

    args = parser.parse_args()

    unify_day(args.date, args.dir, args.delete)
