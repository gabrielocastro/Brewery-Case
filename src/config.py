import os

# Caminhos Base do Projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")


# Caminhos das Camadas do Data Lake (Arquitetura Medalhão)
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

# Configuração da API
API_URL = "https://api.openbrewerydb.org/v1/breweries"

