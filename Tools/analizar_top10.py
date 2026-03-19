"""
analizar_top10.py - Comparativa de volumen del Top 10 de instrumentos comunes.
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
    """Lee y prepara el DataFrame para el análisis."""
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
    return df

def main():
    """
    Función principal para analizar el volumen de los 10 instrumentos más activos.
    """
    print("Cargando datos...")
    df_a = load_data(PATH_A)
    df_b = load_data(PATH_B)

    # Rango compartido
    start_ts = max(df_a['timestamp'].min(), df_b['timestamp'].min())
    end_ts = min(df_a['timestamp'].max(), df_b['timestamp'].max())

    df_a = df_a[(df_a['timestamp'] >= start_ts) & (df_a['timestamp'] <= end_ts)]
    df_b = df_b[(df_b['timestamp'] >= start_ts) & (df_b['timestamp'] <= end_ts)]

    # Símbolos comunes
    symbols_a = set(df_a['symbol_norm'].unique())
    symbols_b = set(df_b['symbol_norm'].unique())
    common_symbols = symbols_a.intersection(symbols_b)

    # Filtrar ambas por los comunes
    df_a_common = df_a[df_a['symbol_norm'].isin(common_symbols)]
    df_b_common = df_b[df_b['symbol_norm'].isin(common_symbols)]

    # Top 10 de los comunes (basado en el volumen de B para asegurar relevancia)
    top_10_symbols = df_b_common['symbol_norm'].value_counts().head(10).index.tolist()

    results = []
    for sym in top_10_symbols:
        count_a = len(df_a_common[df_a_common['symbol_norm'] == sym])
        count_b = len(df_b_common[df_b_common['symbol_norm'] == sym])
        diff = count_a - count_b
        p_diff = (diff / count_a * 100) if count_a > 0 else 0
        results.append({
            "Símbolo": sym,
            "Msgs Fuente A": count_a,
            "Msgs Fuente B": count_b,
            "Dif (A-B)": diff,
            "% Dif": f"{p_diff:.2f}%"
        })

    report = pd.DataFrame(results)
    print("\n--- COMPARATIVA TOP 10 INSTRUMENTOS COMUNES ---")
    print(report.to_string(index=False))

if __name__ == "__main__":
    main()
