import os
import shutil
import logging
import re
from datetime import datetime
from config import BASE_DIR

# Configuração de logging para limpeza
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Diretórios que NUNCA devem ser limpos por este script
PROTECTED_DIRS = {
    'src', 
    'dags', 
    '.git', 
    '.venv', 
    'venv', 
    '__pycache__', 
    '.vscode',
    '.idea'
}

def get_file_info(filename):
    """
    Extrai o nome base e o timestamp de um arquivo.
    Exemplo: 'breweries_by_type_20260222_014208.csv' -> ('breweries_by_type', '20260222_014208', '.csv')
    """
    basename, ext = os.path.splitext(filename)
    
    # Procura por padrões de timestamp: _YYYYMMDD_HHMMSS ou _YYYYMMDD
    timestamp_pattern = r'(_\d{8}_\d{6}|_\d{8})$'
    match = re.search(timestamp_pattern, basename)
    
    if match:
        timestamp_str = match.group(1).strip('_')
        name_without_ts = basename[:match.start()]
        return name_without_ts, timestamp_str, ext
    
    return basename, "", ext

def smart_cleanup(directory_path):
    """
    Percorre o diretório e mantém apenas o arquivo mais recente para cada grupo de nomes base.
    """
    if not os.path.exists(directory_path):
        return

    logger.info(f"Analisando diretório para limpeza inteligente: {directory_path}")
    
    # Dicionário para agrupar arquivos: {(base_name, ext): [lista de (timestamp, full_path, mtime)]}
    groups = {}

    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            full_path = os.path.join(root, filename)
            base_name, timestamp, ext = get_file_info(filename)
            mtime = os.path.getmtime(full_path)
            
            key = (base_name, ext)
            if key not in groups:
                groups[key] = []
            
            groups[key].append({
                'timestamp': timestamp,
                'path': full_path,
                'mtime': mtime
            })

    for (base_name, ext), files in groups.items():
        if len(files) <= 1:
            continue
        
        # Ordena: primeiro por timestamp (string), depois por mtime
        # Arquivos com timestamp mais recente ou mtime maior ficam por último
        sorted_files = sorted(files, key=lambda x: (x['timestamp'], x['mtime']))
        
        # Mantém o último (mais recente)
        keep = sorted_files.pop()
        
        # Remove os outros
        for to_delete in sorted_files:
            try:
                os.remove(to_delete['path'])
                logger.info(f"Removido versão antiga: {os.path.basename(to_delete['path'])}")
            except Exception as e:
                logger.error(f"Erro ao remover {to_delete['path']}: {e}")

def run_cleanup(force=False):
    """
    Executa a limpeza inteligente em todos os diretórios raiz não protegidos.
    """
    root_items = os.listdir(BASE_DIR)
    directories_to_process = []
    
    for item in root_items:
        item_path = os.path.join(BASE_DIR, item)
        if os.path.isdir(item_path) and item not in PROTECTED_DIRS:
            directories_to_process.append(item_path)
    
    if not directories_to_process:
        print("Nenhum diretório elegível para limpeza encontrado.")
        return

    if not force:
        print("\nDiretórios que serão verificados para limpeza inteligente:")
        for d in directories_to_process:
            print(f"  - {os.path.relpath(d, BASE_DIR)}")
        
        confirm = input("\n⚠️ Deseja prosseguir com a limpeza (mantendo apenas os arquivos mais recentes)? (s/n): ")
        if confirm.lower() != 's':
            print("Operação cancelada.")
            return

    print("\nIniciando Limpeza Inteligente...")
    for directory in directories_to_process:
        smart_cleanup(directory)
    
    print("\nLimpeza finalizada com sucesso!")
    print("Nota: O código fonte, DAGs e arquivos de sistema foram preservados.")

if __name__ == "__main__":
    run_cleanup(force=False)
