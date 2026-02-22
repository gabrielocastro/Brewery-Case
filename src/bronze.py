import requests
import json
import os
import time
import logging
from datetime import datetime
from config import BRONZE_DIR, API_URL, LOGS_DIR

def setup_logger():
    """
    Configura o logger para a etapa de ingestão (Bronze).
    
    Returns:
        logging.Logger: Instância do logger configurada.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(LOGS_DIR, f"bronze_ingestion_{datetime.now().strftime('%Y%m%d')}.log")
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger("bronze")


def fetch_data(url):
    """
    Busca dados da API tratando a paginação.
    
    Args:
        url (str): URL base da API para busca dos dados.
        
    Returns:
        list: Lista contendo todos os registros coletados da API.
        
    Raises:
        Exception: Caso ocorra erro na requisição à API.
    """
    all_breweries = []
    page = 1
    per_page = 200 # Limite máximo permitido pela API
    
    logger = setup_logger()
    logger.info(f"Iniciando a ingestão de dados de {url}")
    
    while True:
        try:
            params = {'page': page, 'per_page': per_page}
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Falha ao buscar página {page}. Status: {response.status_code}")
                raise Exception(f"Requisição falhou: {response.status_code} - {response.text}")
            
            data = response.json()
            
            if not data:
                logger.info("Nenhum dado adicional encontrado. Paginação concluída.")
                break
                
            all_breweries.extend(data)
            logger.info(f"Página {page} coletada com {len(data)} registros. Total: {len(all_breweries)}")
            
            page += 1
            time.sleep(0.5) # Pausa para respeitar os limites da API
            
        except Exception as e:
            logger.error(f"Erro ocorrido: {str(e)}")
            raise e
            
    return all_breweries

def save_raw_data(data):
    """
    Salva os dados brutos na camada Bronze com particionamento por data de ingestão.
    
    Args:
        data (list): Lista de dados a serem salvos.
        
    Raises:
        Exception: Caso ocorra erro ao gravar o arquivo em disco.
    """
    logger = setup_logger()
    timestamp = datetime.now()
    partition_date = timestamp.strftime('%Y-%m-%d')
    
    # Cria o caminho da partição
    save_path = os.path.join(BRONZE_DIR, f"ingestion_date={partition_date}")
    os.makedirs(save_path, exist_ok=True)
    
    file_name = f"breweries_raw_{timestamp.strftime('%H%M%S')}.json"
    full_path = os.path.join(save_path, file_name)
    
    try:
        # 1. Salva como JSON
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Sucesso ao salvar {len(data)} registros em {full_path}")
        
        # 2. Salva como CSV (para facilitar visualização rápida)
        import pandas as pd
        csv_path = full_path.replace('.json', '.csv')
        pd.DataFrame(data).to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"Sucesso ao salvar {len(data)} registros em {csv_path}")

        print(f"Sucesso! {len(data)} registros salvos em {full_path} (JSON & CSV)")
    except Exception as e:
        logger.error(f"Falha ao salvar dados: {str(e)}")
        raise e

if __name__ == "__main__":
    try:
        data = fetch_data(API_URL)
        save_raw_data(data)
    except Exception as e:
        print(f"Falha na ingestão: {e}")
