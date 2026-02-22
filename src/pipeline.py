import logging
import os
import sys
from datetime import datetime

from config import LOGS_DIR

# Adiciona o diretório 'src' ao path se necessário
sys.path.append(os.path.join(os.path.dirname(__file__)))

import bronze
import silver
import gold
from cleanup import run_cleanup

# ---------------------------------------------------------------------------
# Configuração de Logging
# ---------------------------------------------------------------------------

def setup_pipeline_logger() -> logging.Logger:
    """
    Configura o logger principal para a execução do pipeline completo.
    
    Returns:
        logging.Logger: Instância do logger configurada.
    """
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(
        LOGS_DIR, f"full_pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    )
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger("pipeline")

def run_pipeline():
    """
    Orquestra o pipeline do Data Lake Open Brewery:
    Limpeza de Logs -> Bronze (Ingestão) -> Silver (Transformação) -> Gold (Agregação)
    """
    # Limpeza automática apenas de logs antes de iniciar para preservar o histórico de dados
    # run_cleanup(force=True, include_data=False, include_logs=True)
    
    logger = setup_pipeline_logger()
    logger.info("=== EXECUCAO DO PIPELINE COMPLETO INICIADA ===")
    print("\nIniciando Pipeline Open Brewery Data Lake...\n")

    try:
        # 1. Camada Bronze
        print("Passo 1: Camada Bronze (Ingestao)")
        data = bronze.fetch_data(bronze.API_URL)
        bronze.save_raw_data(data)
        logger.info("Camada Bronze concluida com sucesso.")

        # 2. Camada Silver
        print("\nPasso 2: Camada Silver (Transformacao)")
        raw_df = silver.load_latest_bronze()
        clean_df = silver.transform(raw_df)
        silver.save_silver(clean_df)
        logger.info("Camada Silver concluida com sucesso.")

        # 3. Camada Gold
        print("\nPasso 3: Camada Gold (Agregacao & Qualidade de Dados)")
        gold.process_gold()
        logger.info("Camada Gold concluida com sucesso.")

        print("\nPipeline executado com sucesso!")
        logger.info("=== EXECUCAO DO PIPELINE COMPLETO FINALIZADA COM SUCESSO ===")

    except Exception as e:
        logger.error(f"O pipeline falhou em alguma etapa: {e}", exc_info=True)
        print(f"\nPipeline falhou: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
