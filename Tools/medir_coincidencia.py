"""
medir_coincidencia.py - Auditoría de integridad de captura entre dos fuentes.
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime
import pandas as pd

def normalize_symbol(val):
    """Extrae el símbolo de un campo que puede ser string o dict/JSON."""
    if isinstance(val, dict):
        return val.get('symbol', '')
    if isinstance(val, str):
        try:
            d = json.loads(val)
            # Manejar formato Rofex {"symbol": "...", "marketId": "..."}
            if isinstance(d, dict):
                return d.get('symbol', '')
        except json.JSONDecodeError:
            return val
    return str(val)

def process_df(path):
    """Lee y prepara el DataFrame para auditoría."""
    ext = os.path.splitext(path)[1].lower()
    if ext == '.parquet':
        df = pd.read_parquet(path)
    elif ext == '.db':
        conn = sqlite3.connect(path)
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        # Buscar tablas comunes: market_data, marketdata o la primera que aparezca
        if 'market_data' in tables['name'].values:
            table_name = 'market_data'
        elif 'marketdata' in tables['name'].values:
            table_name = 'marketdata'
        else:
            table_name = tables.iloc[0]['name']

        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
    else:
        raise ValueError(f"Formato no soportado: {ext}")

    # 1. Asegurar columna Timestamp
    if 'timestamp' not in df.columns:
        raise ValueError(f"Columna 'timestamp' faltante en {path}")

    # 2. Asegurar columna Symbol (Normalizada)
    if 'symbol' in df.columns:
        df['symbol_norm'] = df['symbol'].apply(normalize_symbol)
    elif 'instrumentId' in df.columns:
        df['symbol_norm'] = df['instrumentId'].apply(normalize_symbol)
    else:
        df['symbol_norm'] = 'UNKNOWN'

    # 3. Crear firma universal (TS + SYMBOL)
    # Redondeamos a 1 segundo para tolerar derivas mayores entre loggers
    df['ts_rounded'] = (df['timestamp'] // 1000) * 1000
    df['signature'] = df['ts_rounded'].astype(str) + "_" + df['symbol_norm'].astype(str)

    return df

def main():
    """
    Función principal para auditar la coincidencia de datos.
    """
    parser = argparse.ArgumentParser(description="Auditoría de coincidencia entre dos fuentes")
    parser.add_argument("path_a", help="Ruta de la fuente A (Referencia)")
    parser.add_argument("path_b", help="Ruta de la fuente B (A auditar)")
    args = parser.parse_args()

    try:
        print("--- Iniciando Auditoría ---")
        print(f"Cargando Fuente A: {args.path_a}...")
        df_a = process_df(args.path_a)
        print(f"Cargando Fuente B: {args.path_b}...")
        df_b = process_df(args.path_b)

        # 1. Rango Compartido
        start_ts = max(df_a['timestamp'].min(), df_b['timestamp'].min())
        end_ts = min(df_a['timestamp'].max(), df_b['timestamp'].max())

        # Logs de rango en hora local (aproximada para referencia rápida)
        start_dt = datetime.fromtimestamp(start_ts/1000).strftime('%H:%M:%S')
        end_dt = datetime.fromtimestamp(end_ts/1000).strftime('%H:%M:%S')

        print(f"\nHorario de solapamiento: {start_dt} a {end_dt} (UTC aprox)")

        # 2. Filtrar por rango compartido
        mask_a = (df_a['timestamp'] >= start_ts) & (df_a['timestamp'] <= end_ts)
        mask_b = (df_b['timestamp'] >= start_ts) & (df_b['timestamp'] <= end_ts)

        df_a_sync = df_a[mask_a].copy()
        df_b_sync = df_b[mask_b].copy()

        # 3. Filtrar por Símbolos Comunes (Auditoría Justa)
        symbols_b = set(df_b_sync['symbol_norm'].unique())
        symbols_a = set(df_a_sync['symbol_norm'].unique())
        common_symbols = symbols_a.intersection(symbols_b)

        print(f"Símbolos en Fuente A: {len(symbols_a)}")
        print(f"Símbolos en Fuente B: {len(symbols_b)}")
        print(f"Símbolos en COMÚN:     {len(common_symbols)}")

        if not common_symbols:
            print("❌ No hay símbolos en común entre las dos fuentes.")
            return

        # Restringimos la auditoría solo a los instrumentos que están en AMBOS
        df_a_final = df_a_sync[df_a_sync['symbol_norm'].isin(common_symbols)]
        df_b_final = df_b_sync[df_b_sync['symbol_norm'].isin(common_symbols)]

        count_a = len(df_a_final)
        count_b = len(df_b_final)

        if count_a == 0:
            print("❌ No hay mensajes en el subconjunto de símbolos comunes.")
            return

        # 4. Medir Coincidencia (usando conteo por firmas)
        stats_a = df_a_final.groupby('signature').size()
        stats_b = df_b_final.groupby('signature').size()

        # Unimos los conteos
        comparison = pd.concat([stats_a, stats_b], axis=1, keys=['count_a', 'count_b']).fillna(0)

        # La coincidencia real es el mínimo de mensajes que ambas fuentes coinciden tener
        comparison['matched'] = comparison[['count_a', 'count_b']].min(axis=1)

        n_match = int(comparison['matched'].sum())

        p_match = (n_match / count_a) * 100
        p_perdida = 100 - p_match

        # 5. Resultados
        print("\n" + "="*45)
        print("       RESULTADOS DE LA AUDITORÍA (FILTRADA)")
        print("="*45)
        print(f"Mensajes A (Comunes):  {count_a:15,}")
        print(f"Mensajes B (Comunes):  {count_b:15,}")
        print("-" * 45)
        print(f"COINCIDENTES:          {n_match:15,}")
        print(f"DIFERENCIA (A - B):    {abs(count_a - count_b):15,}")
        print("-" * 45)
        print(f"COINCIDENCIA REAL:     {p_match:14.2f}%")
        print(f"BRECHA DE CAPTURA:     {p_perdida:14.2f}%")
        print("="*45)

        if p_match > 99.99:
            print("✅ EXCELENTE: Las fuentes son idénticas en el rango.")
        elif p_match > 99.0:
            print("✔️ MUY BUENO: Coincidencia casi total.")
        elif p_match > 95:
            print("⚠️ ADVERTENCIA: Se detectó una pequeña pérdida de mensajes.")
        else:
            print("🚨 CRÍTICO: Discrepancia significativa detectada.")

    except Exception as e:  # pylint: disable=broad-except
        print(f"Error durante la auditoría: {e}")

if __name__ == "__main__":
    main()
