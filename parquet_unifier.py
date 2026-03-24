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
    Unifica los chunks de una fecha dada, soportando anexión incremental.
    date_str: Formato YYYYMMDD
    """
    data_path = Path(base_dir)
    if not data_path.exists():
        logger.error("❌ El directorio %s no existe.", data_path)
        return

    # 1. Escaneo de Chunks
    pattern = f"chunk_w*_{date_str}_*.parquet"
    chunk_files = list(data_path.glob(pattern))
    output_name = f"marketdata_{date_str}.parquet"
    output_path = data_path / output_name

    if not chunk_files:
        logger.warning("⚠️ No se encontraron nuevos chunks para la fecha %s.", date_str)
        return

    logger.info(
        "📂 Encontrados %d chunks para el %s. Iniciando unificación...",
        len(chunk_files), date_str
    )

    # 2. Carga y Consolidación
    all_dfs = []
    total_rows_input = 0

    # A. Cargar archivo consolidado previo si existe (Lógica de Append)
    if output_path.exists():
        try:
            df_base = pd.read_parquet(output_path)
            all_dfs.append(df_base)
            total_rows_input += len(df_base)
            logger.info("📚 Archivo base cargado: %s (%d filas)", output_name, len(df_base))
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("❌ No se pudo leer el archivo base existente: %s", exc)

    # B. Cargar nuevos chunks
    try:
        for f in chunk_files:
            df = pd.read_parquet(f)
            total_rows_input += len(df)
            all_dfs.append(df)
            logger.debug("   + Leído %s (%d filas)", f.name, len(df))

        if not all_dfs:
            return

        full_df = pd.concat(all_dfs, ignore_index=True)

        # 3. Deduplicación y Ordenamiento
        initial_count = len(full_df)
        logger.info("💎 Eliminando duplicados (basado en '_arrival_ts_ms' e 'instrumentId')...")
        full_df = full_df.drop_duplicates(subset=["_arrival_ts_ms", "instrumentId"])
        final_count = len(full_df)
        if initial_count > final_count:
            logger.info("✨ Se eliminaron %d registros duplicados.", initial_count - final_count)

        logger.info("⚖️ Re-ordenando registros por '_arrival_ts_ms'...")
        full_df = full_df.sort_values(by="_arrival_ts_ms", ascending=True)

        # 4. Exportación
        logger.info("💾 Guardando archivo unificado (incremental): %s", output_name)
        full_df.to_parquet(
            output_path,
            engine="pyarrow",
            compression="zstd",
            index=False
        )

        # 5. Validación y Limpieza
        unified_df = pd.read_parquet(output_path)
        rows_unified = len(unified_df)

        if rows_unified >= final_count:
            # --- Chequeo de Seguridad Adicional ---
            logger.info("🛡️  Iniciando chequeo de seguridad de datos...")
            all_chunks_verified = True
            for f in chunk_files:
                df_chunk = pd.read_parquet(f)
                # Obtenemos llaves únicas del chunk
                chunk_keys = df_chunk[["_arrival_ts_ms", "instrumentId"]].drop_duplicates()
                # Verificamos si existen en el maestro
                merged = pd.merge(
                    chunk_keys,
                    unified_df[["_arrival_ts_ms", "instrumentId"]],
                    on=["_arrival_ts_ms", "instrumentId"],
                    how="inner"
                )
                if len(merged) != len(chunk_keys):
                    logger.error(
                        "❌ Falla de seguridad: El chunk %s no está totalmente "
                        "contenido en el maestro.",
                        f.name
                    )
                    all_chunks_verified = False
                    break
            if not all_chunks_verified:
                logger.warning(
                    "⚠️ No se eliminarán los chunks debido a falla en el chequeo de seguridad."
                )
                return

            logger.info("✅ Chequeo de seguridad completado. Datos confirmados en disco.")
            if delete_chunks:
                logger.info("🧹 Eliminando %d chunks procesados...", len(chunk_files))
                for f in chunk_files:
                    f.unlink()
                logger.info("✅ Limpieza completada.")
        else:
            logger.error(
                "❌ Error de validación: Filas en disco (%d) inconsistentes con el proceso.",
                rows_unified
            )
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
