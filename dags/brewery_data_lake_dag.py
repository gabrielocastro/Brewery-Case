import os
import sys
from datetime import datetime

# Adiciona os diretórios 'src' e a raiz do projeto ao path para que os módulos sejam encontrados
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PROJECT_ROOT, "src"))
sys.path.append(PROJECT_ROOT)

# Importa a lógica do pipeline existente
import bronze
import silver
import gold
from cleanup import run_cleanup
import documentation

def run_bronze():
    """
    Executa a camada Bronze: busca dados da API e salva como JSON bruto.
    """
    print("Iniciando Camada Bronze (Ingestao)...")
    data = bronze.fetch_data(bronze.API_URL)
    bronze.save_raw_data(data)
    print("Camada Bronze concluida.")

def run_silver():
    """
    Executa a camada Silver: carrega os dados brutos, aplica transformações e salva em Parquet.
    """
    print("\nIniciando Camada Silver (Transformacao)...")
    raw_df = silver.load_latest_bronze()
    clean_df = silver.transform(raw_df)
    silver.save_silver(clean_df)
    print("Camada Silver concluida.")

def run_gold():
    """
    Executa a camada Gold: realiza agregações e verificações de qualidade.
    """
    print("\nIniciando Camada Gold (Agregacao & Qualidade)...")
    gold.process_gold()
    print("Camada Gold concluida.")


def main():
    """
    Função principal que orquestra a execução de todas as etapas do pipeline.
    """
    start_time = datetime.now()
    print(f"Iniciando Pipeline Open Brewery Data Lake as {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Limpeza
        # print("\nLimpando logs antigos...")
        # run_cleanup(force=True, include_data=False, include_logs=True)
        
        # 2. Bronze
        run_bronze()
        
        # 3. Silver
        run_silver()
        
        # 4. Gold (Processamento + Documentação)
        run_gold()
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\nPipeline executado com sucesso em {duration}!")
        
    except Exception as e:
        print(f"\nErro durante a execucao do pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
