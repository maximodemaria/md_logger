"""
medir_latencia_ms.py - Herramienta para medir latencia milisegundo a milisegundo.
"""

import json
import os
import sqlite3
import pandas as pd

PATH_A = r"c:\Users\max\Desktop\Drive\md_logger\data\marketdata\marketdata_20260319.parquet"
PATH_B = r"C:\Users\max\Downloads\2026-03-19.db"

def normalize_symbol(val):
    """Extrae el símbolo de un campo que puede ser string o dict/JSON."""
    if isinstance(val, dict):
        return val.get('symbol', '')
    if isinstance(val, str):
        try:
            d = json.loads(val)
            if isinstance(d, dict):
                return d.get('symbol', '')
        except json.JSONDecodeError:
            pass
    return str(val)

def load_data(path):
    """Lee y prepara el DataFrame para el análisis de latencia."""
    ext = os.path.splitext(path)[1].lower()
    if ext == '.parquet':
        df = pd.read_parquet(path)
    else:
        conn = sqlite3.connect(path)
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        table_name = (
            'market_data' if 'market_data' in tables['name'].values
            else tables.iloc[0]['name']
        )
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

    if 'instrumentId' in df.columns:
        df['symbol_norm'] = df['instrumentId'].apply(normalize_symbol)
    else:
        df['symbol_norm'] = df['symbol'].apply(normalize_symbol)

    # Asegurar que esté ordenado para merge_asof
    return df.sort_values('timestamp')

def main():
    """
    Función principal para medir la latencia entre loggers usando merge_asof.
    """
    print("Cargando datos...")
    df_a = load_data(PATH_A)
    df_b = load_data(PATH_B)

    # Rango compartido
    start_ts = max(df_a['timestamp'].min(), df_b['timestamp'].min())
    end_ts = min(df_a['timestamp'].max(), df_b['timestamp'].max())

    df_a = df_a[(df_a['timestamp'] >= start_ts) & (df_a['timestamp'] <= end_ts)]
    df_b = df_b[(df_b['timestamp'] >= start_ts) & (df_b['timestamp'] <= end_ts)]

    # Top 10 de B (los que están en ambos)
    common_symbols = set(df_a['symbol_norm'].unique()).intersection(set(df_b['symbol_norm'].unique()))
    df_b_common = df_b[df_b['symbol_norm'].isin(common_symbols)]
    top_10_symbols = df_b_common['symbol_norm'].value_counts().head(10).index.tolist()

    results = []
    print("\nCalculando desfases temporales (ms)...")

    for sym in top_10_symbols:
        # Filtrar por símbolo
        a_sub = df_a[df_a['symbol_norm'] == sym][['timestamp']].reset_index(drop=True)
        b_sub = (df_b[df_b['symbol_norm'] == sym][['timestamp']]
                 .reset_index(drop=True)
                 .rename(columns={'timestamp': 'timestamp_b'}))

        # Merge ASOF para encontrar el mensaje más cercano en B para cada mensaje de A
        merged = pd.merge_asof(
            a_sub, b_sub,
            left_on='timestamp',
            right_on='timestamp_b',
            direction='nearest',
            tolerance=500  # Max 500ms
        )

        matched = merged.dropna()

        if not matched.empty:
            diffs = (matched['timestamp'] - matched['timestamp_b']).abs()

            results.append({
                "Símbolo": sym,
                "Emparejados": len(matched),
                "Avg Diff (ms)": diffs.mean(),
                "Mediana (ms)": diffs.median(),
                "Max Diff (ms)": diffs.max(),
                "Min Diff (ms)": diffs.min()
            })

    report = pd.DataFrame(results)
    print("\n--- ANÁLISIS DE PRECISIÓN TEMPORAL (ms) ---")
    print(report.to_string(index=False, formatters={'Avg Diff (ms)': '{:,.2f}'.format}))

if __name__ == "__main__":
    main()
