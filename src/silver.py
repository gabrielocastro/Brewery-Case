import os
import json
import glob
import logging
import re
from datetime import datetime, timezone

import pandas as pd

from config import BRONZE_DIR, SILVER_DIR, LOGS_DIR


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    """
    Configura o logger para a etapa de transformação (Silver).
    
    Returns:
        logging.Logger: Instância do logger configurada.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(
        LOGS_DIR, f"silver_transformation_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


logger = setup_logger()


# ---------------------------------------------------------------------------
# Tipos de cervejarias conhecidos (documentação da Open Brewery DB)
# ---------------------------------------------------------------------------

KNOWN_BREWERY_TYPES = {
    "micro", "nano", "regional", "brewpub", "large",
    "planning", "bar", "contract", "proprietor", "taproom", "closed",
}


# ---------------------------------------------------------------------------
# Carga de Dados
# ---------------------------------------------------------------------------

def load_latest_bronze(bronze_dir: str = BRONZE_DIR) -> pd.DataFrame:
    """
    Carrega todos os arquivos JSON da partição 'ingestion_date' mais recente
    na camada Bronze e retorna um único DataFrame.
    
    Args:
        bronze_dir (str): Caminho para o diretório da camada Bronze.
        
    Returns:
        pd.DataFrame: DataFrame contendo os registros brutos consolidados.
        
    Raises:
        FileNotFoundError: Se nenhuma partição ou arquivo JSON for encontrado.
    """
    partitions = sorted(glob.glob(os.path.join(bronze_dir, "ingestion_date=*")))
    if not partitions:
        raise FileNotFoundError(f"Nenhuma partição Bronze encontrada em: {bronze_dir}")

    latest_partition = partitions[-1]
    logger.info(f"Carregando partição Bronze: {latest_partition}")

    json_files = glob.glob(os.path.join(latest_partition, "*.json"))
    if not json_files:
        raise FileNotFoundError(f"Nenhum arquivo JSON encontrado na partição: {latest_partition}")

    records = []
    for file_path in json_files:
        with open(file_path, encoding="utf-8") as f:
            records.extend(json.load(f))

    df = pd.DataFrame(records)
    logger.info(f"Carregados {len(df)} registros brutos de {len(json_files)} arquivo(s).")
    return df


# ---------------------------------------------------------------------------
# Transformações
# ---------------------------------------------------------------------------

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove registros de cervejarias duplicados, mantendo a última ocorrência com base no 'id'.
    """
    before = len(df)
    df = df.drop_duplicates(subset="id", keep="last")
    dropped = before - len(df)
    logger.info(f"Deduplicação: removidos {dropped} duplicado(s). Restantes: {len(df)}")
    return df


def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços em branco no início e no fim de todas as colunas do tipo string.
    """
    str_cols = df.select_dtypes(include="str").columns
    for col in str_cols:
        df[col] = df[col].str.strip()
    logger.info("Colunas de texto limpas (espaços removidos).")
    return df


def normalize_phone(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas os dígitos na coluna de telefone.
    """
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(
            lambda x: re.sub(r"\D", "", str(x)) if pd.notna(x) else None
        )
        # Substitui strings vazias por None
        df["phone"] = df["phone"].replace("", None)
    logger.info("Números de telefone normalizados (apenas dígitos).")
    return df


def normalize_postal_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o código postal: remove espaços e converte para maiúsculo.
    """
    if "postal_code" in df.columns:
        df["postal_code"] = df["postal_code"].str.strip().str.upper()
    logger.info("Códigos postais normalizados.")
    return df


def validate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Define longitude/latitude como NaN se estiverem fora dos intervalos geográficos válidos.
    Válido: longitude [-180, 180], latitude [-90, 90].
    """
    if "longitude" in df.columns:
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        invalid_lon = ~df["longitude"].between(-180, 180, inclusive="both")
        df.loc[invalid_lon, "longitude"] = None
        logger.info(f"Coordenadas: {invalid_lon.sum()} longitude(s) inválida(s) anulada(s).")

    if "latitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        invalid_lat = ~df["latitude"].between(-90, 90, inclusive="both")
        df.loc[invalid_lat, "latitude"] = None
        logger.info(f"Coordenadas: {invalid_lat.sum()} latitude(s) inválida(s) anulada(s).")

    return df


def standardize_brewery_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza o tipo de cervejaria: minúsculo e mapeia valores desconhecidos para 'unknown'.
    """
    if "brewery_type" in df.columns:
        df["brewery_type"] = df["brewery_type"].str.lower().str.strip()
        df["brewery_type"] = df["brewery_type"].apply(
            lambda x: x if x in KNOWN_BREWERY_TYPES else "unknown"
        )
    logger.info("Tipo de cervejaria (brewery_type) padronizado.")
    return df


def drop_redundant_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas que são majoritariamente nulas ou redundantes.
    """
    cols_to_drop = [c for c in ["address_2", "address_3", "street"] if c in df.columns]
    df = df.drop(columns=cols_to_drop)
    logger.info(f"Colunas redundantes removidas: {cols_to_drop}")
    return df


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona uma coluna 'processed_at' com o timestamp do processamento.
    """
    df["processed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Metadados (processed_at) adicionados.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as transformações da camada Silver em ordem.
    
    Args:
        df (pd.DataFrame): DataFrame bruto.
        
    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    df = deduplicate(df)
    df = clean_strings(df)
    df = normalize_phone(df)
    df = normalize_postal_code(df)
    df = validate_coordinates(df)
    df = standardize_brewery_type(df)
    df = drop_redundant_columns(df)
    df = add_metadata(df)
    return df


# ---------------------------------------------------------------------------
# Escrita dos Dados
# ---------------------------------------------------------------------------

def save_silver(df: pd.DataFrame, silver_dir: str = SILVER_DIR) -> None:
    """
    Grava o DataFrame transformado em arquivos Parquet particionados por 'brewery_type'.
    
    Args:
        df (pd.DataFrame): DataFrame transformado.
        silver_dir (str): Caminho para o diretório da camada Silver.
    """
    os.makedirs(silver_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_written = 0

    for brewery_type, group in df.groupby("brewery_type"):
        partition_dir = os.path.join(silver_dir, f"brewery_type={brewery_type}")
        os.makedirs(partition_dir, exist_ok=True)

        file_path = os.path.join(partition_dir, f"breweries_{timestamp}.parquet")
        group.to_parquet(file_path, index=False, engine="pyarrow")

        csv_path = file_path.replace(".parquet", ".csv")
        group.to_csv(csv_path, index=False)

        logger.info(
            f"Salvos {len(group)} registros -> {file_path} (Parquet & CSV)"
        )
        total_written += len(group)

    logger.info(f"Camada Silver concluida. Total de registros gravados: {total_written}")
    print(f"Camada Silver concluida! {total_written} registros gravados em: {silver_dir}")


# ---------------------------------------------------------------------------
# Ponto de Entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        logger.info("=== Início da transformação Silver ===")
        raw_df = load_latest_bronze()
        clean_df = transform(raw_df)
        save_silver(clean_df)
        logger.info("=== Transformação Silver finalizada com sucesso ===")
    except Exception as e:
        logger.error(f"Falha na transformacao Silver: {e}")
        print(f"Falha na transformacao Silver: {e}")
        raise e
