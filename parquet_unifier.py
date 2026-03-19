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

def unify_day(date_str: str, base_dir: str, delete_chunks: bool = False):
    """
    Unifica los chunks de una fecha dada.
    date_str: Formato YYYYMMDD
    """
    data_path = Path(base_dir)
    if not data_path.exists():
        logger.error("❌ El directorio %s no existe.", data_path)
        return

    # 1. Escaneo de Chunks
    # Ejemplo: chunk_w0_20260319_103113_662508_00000.parquet
    pattern = f"chunk_w*_{date_str}_*.parquet"
    chunk_files = list(data_path.glob(pattern))

    if not chunk_files:
        logger.warning("⚠️ No se encontraron chunks para la fecha %s.", date_str)
        return

    logger.info(
        "📂 Encontrados %d chunks para el %s. Iniciando unificación...",
        len(chunk_files), date_str
    )

    # 2. Carga y Consolidación
    all_dfs = []
    total_rows_original = 0

    try:
        for f in chunk_files:
            df = pd.read_parquet(f)
            total_rows_original += len(df)
            all_dfs.append(df)
            logger.debug("   + Leído %s (%d filas)", f.name, len(df))

        if not all_dfs:
            return

        full_df = pd.concat(all_dfs, ignore_index=True)

        # 3. Ordenamiento Determinista
        # Priorizamos el timestamp de llegada al sistema para la cronología
        logger.info("⚖️ Ordenando registros por '_arrival_ts_ms'...")
        full_df = full_df.sort_values(by="_arrival_ts_ms", ascending=True)

        # 4. Exportación
        output_name = f"marketdata_{date_str}.parquet"
        output_path = data_path / output_name

        logger.info("💾 Guardando archivo unificado: %s", output_name)
        full_df.to_parquet(
            output_path,
            engine="pyarrow",
            compression="zstd",
            index=False
        )

        # 5. Validación y Limpieza
        unified_df = pd.read_parquet(output_path)
        rows_unified = len(unified_df)

        if rows_unified == total_rows_original:
            logger.info("✅ Validación exitosa: %d filas procesadas.", rows_unified)

            if delete_chunks:
                logger.info("🧹 Eliminando %d chunks originales...", len(chunk_files))
                for f in chunk_files:
                    f.unlink()
                logger.info("✅ Limpieza completada: Chunks eliminados definitivamente.")
        else:
            logger.error(
                "❌ Error de validación: Filas unificadas (%d) != Originales (%d).",
                rows_unified, total_rows_original
            )
            logger.warning("⚠️ No se eliminaron los chunks por error de validación.")

    except Exception as exc:  # pylint: disable=broad-except
        logger.error("💥 Error crítico durante la unificación: %s", exc, exc_info=True)

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
