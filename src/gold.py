import os
import glob
import logging
from datetime import datetime, timezone

import pandas as pd
import numpy as np


from config import SILVER_DIR, GOLD_DIR, LOGS_DIR
import data_quality as dq
import documentation as doc


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    """
    Configura o logger para a etapa de agregação (Gold).
    
    Returns:
        logging.Logger: Instância do logger configurada.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(
        LOGS_DIR, f"gold_aggregation_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


logger = setup_logger()


# ---------------------------------------------------------------------------
# Carga da Camada Silver
# ---------------------------------------------------------------------------

def load_silver(silver_dir: str = SILVER_DIR) -> pd.DataFrame:
    """
    Lê todos os arquivos Parquet da camada Silver (particionados por brewery_type)
    e retorna um único DataFrame consolidado.
    
    Args:
        silver_dir (str): Caminho para o diretório da camada Silver.
        
    Returns:
        pd.DataFrame: DataFrame consolidado.
        
    Raises:
        FileNotFoundError: Se nenhum arquivo Parquet for encontrado.
    """
    parquet_files = glob.glob(
        os.path.join(silver_dir, "brewery_type=*", "*.parquet")
    )
    if not parquet_files:
        raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado na camada Silver: {silver_dir}")

    dfs = []
    for fp in parquet_files:
        df_part = pd.read_parquet(fp, engine="pyarrow")
        # Recupera o valor da partição do nome da pasta se a coluna estiver ausente
        if "brewery_type" not in df_part.columns:
            btype = os.path.basename(os.path.dirname(fp)).split("=", 1)[-1]
            df_part["brewery_type"] = btype
        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Carregados {len(df)} registros de {len(parquet_files)} arquivo(s) Silver.")
    return df


# ---------------------------------------------------------------------------
# Agregações
# ---------------------------------------------------------------------------

def agg_breweries_by_type_and_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 1 – Quantidade de cervejarias por tipo e estado.
    """
    result = (
        df.groupby(["brewery_type", "state_province"], dropna=False)
        .agg(brewery_count=("id", "count"))
        .reset_index()
        .sort_values(["state_province", "brewery_count"], ascending=[True, False])
    )
    logger.info(f"[agg_by_type_and_state] Produzidas {len(result)} linhas.")
    return result


def agg_breweries_by_country_and_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 2 – Quantidade de cervejarias por país e tipo.
    """
    result = (
        df.groupby(["country", "brewery_type"], dropna=False)
        .agg(brewery_count=("id", "count"))
        .reset_index()
        .sort_values(["country", "brewery_count"], ascending=[True, False])
    )
    logger.info(f"[agg_by_country_and_type] Produzidas {len(result)} linhas.")
    return result


def agg_top_cities(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Agregação 3 – Top N cidades por contagem total de cervejarias.
    """
    result = (
        df.groupby(["city", "state_province", "country"], dropna=False)
        .agg(brewery_count=("id", "count"))
        .reset_index()
        .sort_values("brewery_count", ascending=False)
        .head(top_n)
    )
    logger.info(f"[agg_top_cities] Top {top_n} cidades calculadas.")
    return result


def agg_geo_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 4 – Quantidade de cervejarias por país, estado e cidade.
    """
    result = (
        df.groupby(["country", "state_province", "city"], dropna=False)
        .agg(brewery_count=("id", "count"))
        .reset_index()
        .sort_values(["country", "state_province", "brewery_count"], ascending=[True, True, False])
    )
    logger.info(f"[agg_per_location] Produzidas {len(result)} linhas.")
    return result


def agg_digital_maturity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 5 – Maturidade Digital: Cervejarias com site e telefone por estado.
    """
    df["has_website"] = df["website_url"].notnull()
    df["has_phone"] = df["phone"].notnull()
    df["digitally_ready"] = df["has_website"] & df["has_phone"]

    result = (
        df.groupby("state_province")
        .agg(
            total_breweries=("id", "count"),
            digitally_ready_count=("digitally_ready", "sum")
        )
        .assign(maturity_score=lambda x: (x["digitally_ready_count"] / x["total_breweries"] * 100).round(2))
        .reset_index()
        .sort_values("maturity_score", ascending=False)
    )
    logger.info(f"[agg_digital_maturity] Calculada maturidade para {len(result)} estados.")
    return result


def agg_regional_diversity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 6 – Diversidade Regional: Quantidade de tipos únicos de cervejaria por estado.
    """
    result = (
        df.groupby("state_province")["brewery_type"]
        .nunique()
        .rename("unique_brewery_types")
        .reset_index()
        .sort_values("unique_brewery_types", ascending=False)
    )
    logger.info(f"[agg_regional_diversity] Calculada diversidade para {len(result)} estados.")
    return result


def agg_market_specialization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 7 – Especialização de Mercado: Contagem total e 'micro' por estado.
    """
    micro_counts = df[df["brewery_type"] == "micro"].groupby("state_province").size().rename("micro_brewery_count")
    total_counts = df.groupby("state_province").size().rename("total_brewery_count")

    result = (
        pd.concat([total_counts, micro_counts], axis=1)
        .fillna(0)
        .astype(int)
        .reset_index()
        .sort_values("total_brewery_count", ascending=False)
    )
    logger.info(f"[agg_market_specialization] Calculada especialização para {len(result)} estados.")
    return result


def agg_data_trust_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agregação 8 – Data Trust Score: Completude de campos críticos (Address, Phone, Website) por estado.
    """
    critical_cols = ["address_1", "phone", "website_url"]
    df["completeness"] = df[critical_cols].notnull().sum(axis=1) / len(critical_cols)
    
    result = (
        df.groupby("state_province")["completeness"]
        .mean()
        .rename("trust_score")
        .reset_index()
        .assign(trust_score=lambda x: (x["trust_score"] * 100).round(2))
        .sort_values("trust_score", ascending=False)
    )
    logger.info(f"[agg_data_trust_score] Calculado trust score para {len(result)} estados.")
    return result


# ---------------------------------------------------------------------------
# Qualidade de Dados para Camada Gold
# ---------------------------------------------------------------------------

def run_gold_dq(aggregations: dict):
    """
    Executa verificações básicas de qualidade de dados nas tabelas Gold.
    
    Args:
        aggregations (dict): Dicionário contendo os DataFrames das agregações.
    """
    logger.info("Executando verificações de qualidade da camada Gold...")
    all_checks = []

    for name, df in aggregations.items():
        # Verifica se as tabelas estão vazias
        all_checks.append(dq.check_volume(df, min_rows=1))
        # Verifica nulidade na coluna de contagem
        if "brewery_count" in df.columns:
            all_checks.append(dq.check_nulls(df, ["brewery_count"]))

    dq.run_suite(all_checks, raise_on_failure=True)


# ---------------------------------------------------------------------------
# Escrita dos Dados
# ---------------------------------------------------------------------------

def save_gold(df: pd.DataFrame, name: str, gold_dir: str = GOLD_DIR) -> str:
    """
    Salva um DataFrame de agregação Gold nos formatos Parquet e CSV.
    Salva uma versão com timestamp e uma versão 'latest' para cada formato.
    
    Args:
        df (pd.DataFrame): DataFrame de agregação.
        name (str): Nome da tabela/agregação.
        gold_dir (str): Caminho para o diretório da camada Gold.
        
    Returns:
        str: Caminho do arquivo Parquet com timestamp gerado.
    """
    os.makedirs(gold_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # --- Parquet ---
    # Salva versão com timestamp
    ts_parquet_path = os.path.join(gold_dir, f"{name}_{timestamp}.parquet")
    df.to_parquet(ts_parquet_path, index=False, engine="pyarrow")
    
    # --- CSV ---
    # Salva versão com timestamp
    ts_csv_path = os.path.join(gold_dir, f"{name}_{timestamp}.csv")
    df.to_csv(ts_csv_path, index=False)
    
    logger.info(f"Tabela Gold '{name}' salva (Parquet & CSV) com timestamp {timestamp}")
    print(f"  OK {name}: {len(df)} linhas salvas (Parquet & CSV).")
    return ts_parquet_path




# ---------------------------------------------------------------------------
# Lógica Principal (Orquestração local)
# ---------------------------------------------------------------------------

def process_gold():
    """Lógica principal de processamento para a camada Gold."""
    logger.info("=== Início da agregação Gold ===")
    
    # 1. Carrega todos os dados da Silver
    silver_df = load_silver()
    
    # 2. Executa as agregações
    aggregations = {
        "breweries_by_type_and_state": agg_breweries_by_type_and_state(silver_df),
        "breweries_by_country_and_type": agg_breweries_by_country_and_type(silver_df),
        "top_cities_by_brewery_count": agg_top_cities(silver_df, top_n=20),
        "geo_coverage_by_state": agg_geo_coverage(silver_df),
        "digital_maturity": agg_digital_maturity(silver_df),
        "regional_diversity": agg_regional_diversity(silver_df),
        "market_specialization": agg_market_specialization(silver_df),
        "data_trust_score": agg_data_trust_score(silver_df),
    }

    # 3. Verificações de Qualidade
    run_gold_dq(aggregations)

    # 4. Atualiza Documentação e gera PDFs
    doc.run_documentation_pipeline(aggregations=aggregations)

    # 5. Salva os resultados
    print(f"\nSalvando {len(aggregations)} tabelas Gold:")
    for table_name, agg_df in aggregations.items():
        save_gold(agg_df, table_name)

    logger.info("=== Agregação Gold finalizada com sucesso ===")


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        print("Iniciando agregacao da camada Gold...")
        process_gold()
        print("\nCamada Gold completa!")
    except Exception as e:
        logger.error(f"Falha na agregacao Gold: {e}")
        print(f"Falha na agregacao Gold: {e}")
        sys.exit(1)
