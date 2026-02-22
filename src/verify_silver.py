import pandas as pd
import glob
import os

# Configuração de caminhos base
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, 'data', 'silver')

# Busca todos os arquivos Parquet na camada Silver
files = glob.glob(os.path.join(SILVER_DIR, '**', '*.parquet'), recursive=True)
print(f'Arquivos Parquet encontrados: {len(files)}')

if files:
    # Consolida todos os arquivos em um único DataFrame para verificação
    df = pd.concat([pd.read_parquet(f) for f in files])
    print(f'Total de registros: {len(df)}')
    print(f'Colunas: {list(df.columns)}')
    print(f'IDs Nulos: {df["id"].isnull().sum()}')
    print(f'IDs Duplicados: {df["id"].duplicated().sum()}')
    print(f'Valores de brewery_type: {sorted(df["brewery_type"].unique())}')
    print()
    print("Contagem por tipo de cervejaria:")
    print(df.groupby("brewery_type").size().sort_values(ascending=False).to_string())
    print()

    # Estatísticas por cidade e país
    if 'city' in df.columns:
        print(f'Cidades únicas: {df["city"].nunique()}')
    if 'country' in df.columns:
        print(f'Países únicos: {df["country"].nunique()}')
        print()
        print("Contagem por país:")
        print(df.groupby("country").size().sort_values(ascending=False).to_string())
else:
    print("Nenhum arquivo encontrado para verificação.")
