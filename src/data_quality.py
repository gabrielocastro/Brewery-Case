"""
data_quality.py – Verificações reutilizáveis de qualidade de dados para o Data Lake Open Brewery.

Cada função de verificação retorna um dicionário com:
    - check_name  : str (nome da verificação)
    - passed      : bool (se passou ou não)
    - details     : str (detalhes legíveis sobre o resultado)
"""

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Verificações Individuais
# ---------------------------------------------------------------------------

def check_nulls(df: pd.DataFrame, columns: List[str], threshold: float = 0.0) -> dict:
    """
    Falha se a taxa de valores nulos em qualquer uma das colunas especificadas exceder o 'threshold' (0–1).
    O threshold padrão (0.0) significa que não são permitidos valores nulos.
    
    Args:
        df (pd.DataFrame): DataFrame a ser verificado.
        columns (List[str]): Lista de colunas para verificação de nulos.
        threshold (float): Limite tolerável de nulos (ex: 0.1 para 10%).
        
    Returns:
        dict: Resultado da verificação.
    """
    issues = []
    for col in columns:
        if col not in df.columns:
            issues.append(f"Coluna '{col}' não encontrada no DataFrame.")
            continue
        null_rate = df[col].isna().mean()
        if null_rate > threshold:
            issues.append(
                f"Taxa de nulos em '{col}' ({null_rate:.2%}) excede o limite de {threshold:.2%}."
            )

    passed = len(issues) == 0
    details = "Todas as verificações de nulos passaram." if passed else " | ".join(issues)
    result = {"check_name": "check_nulls", "passed": passed, "details": details}
    _log(result)
    return result


def check_unique(df: pd.DataFrame, columns: List[str]) -> dict:
    """
    Falha se a combinação das colunas especificadas não for única em todas as linhas.
    
    Args:
        df (pd.DataFrame): DataFrame a ser verificado.
        columns (List[str]): Colunas que devem formar uma chave única.
        
    Returns:
        dict: Resultado da verificação.
    """
    total = len(df)
    unique = df.drop_duplicates(subset=columns).shape[0]
    duplicates = total - unique
    passed = duplicates == 0
    details = (
        "Verificação de unicidade passou."
        if passed
        else f"{duplicates} linha(s) duplicada(s) encontradas nas colunas {columns}."
    )
    result = {"check_name": "check_unique", "passed": passed, "details": details}
    _log(result)
    return result


def check_volume(df: pd.DataFrame, min_rows: int, max_rows: int = None) -> dict:
    """
    Falha se o DataFrame tiver menos que 'min_rows' ou mais que 'max_rows' linhas.
    
    Args:
        df (pd.DataFrame): DataFrame a ser verificado.
        min_rows (int): Quantidade mínima de linhas esperada.
        max_rows (int, opcional): Quantidade máxima de linhas esperada.
        
    Returns:
        dict: Resultado da verificação.
    """
    n = len(df)
    issues = []
    if n < min_rows:
        issues.append(f"Contagem de linhas {n} está abaixo do mínimo de {min_rows}.")
    if max_rows is not None and n > max_rows:
        issues.append(f"Contagem de linhas {n} excede o máximo de {max_rows}.")

    passed = len(issues) == 0
    details = f"Volume OK: {n} linhas." if passed else " | ".join(issues)
    result = {"check_name": "check_volume", "passed": passed, "details": details}
    _log(result)
    return result


def check_allowed_values(
    df: pd.DataFrame, column: str, allowed: set, null_ok: bool = True
) -> dict:
    """
    Falha se houver valores na coluna que não estejam no conjunto de valores permitidos ('allowed').
    
    Args:
        df (pd.DataFrame): DataFrame a ser verificado.
        column (str): Nome da coluna.
        allowed (set): Conjunto de valores permitidos.
        null_ok (bool): Se True, ignora valores nulos na verificação.
        
    Returns:
        dict: Resultado da verificação.
    """
    if column not in df.columns:
        result = {
            "check_name": "check_allowed_values",
            "passed": False,
            "details": f"Coluna '{column}' não encontrada.",
        }
        _log(result)
        return result

    series = df[column] if null_ok else df[column].dropna()
    invalid = series.dropna()[~series.dropna().isin(allowed)]
    passed = len(invalid) == 0
    details = (
        f"Todos os valores em '{column}' são válidos."
        if passed
        else f"'{column}' possui {len(invalid)} valor(es) inválido(s): {invalid.unique().tolist()[:10]}"
    )
    result = {
        "check_name": "check_allowed_values",
        "passed": passed,
        "details": details,
    }
    _log(result)
    return result


def check_schema(df: pd.DataFrame, expected_columns: List[str]) -> dict:
    """
    Falha se qualquer uma das colunas esperadas estiver ausente no DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame a ser verificado.
        expected_columns (List[str]): Lista de colunas obrigatórias.
        
    Returns:
        dict: Resultado da verificação.
    """
    missing = [c for c in expected_columns if c not in df.columns]
    passed = len(missing) == 0
    details = (
        "Verificação de schema passou."
        if passed
        else f"Colunas ausentes: {missing}"
    )
    result = {"check_name": "check_schema", "passed": passed, "details": details}
    _log(result)
    return result


# ---------------------------------------------------------------------------
# Executor de Suíte
# ---------------------------------------------------------------------------

def run_suite(checks: List[dict], raise_on_failure: bool = False) -> bool:
    """
    Executa uma lista de resultados de verificação (dict) e imprime um resumo.
    Retorna True se todas as verificações passaram.

    Args:
        checks: Lista de dicionários retornados pelas funções check_*.
        raise_on_failure: Se True, lança ValueError se qualquer verificação falhar.
    """
    all_passed = all(c["passed"] for c in checks)
    print("\nRelatorio de Qualidade de Dados")
    print("=" * 50)
    for c in checks:
        status = "PASS" if c["passed"] else "FAIL"
        print(f"  {status}  [{c['check_name']}] {c['details']}")
    print("=" * 50)
    overall = "TODAS AS VERIFICACOES PASSARAM" if all_passed else "ALGUMAS VERIFICACOES FALHARAM"
    print(f"  Geral: {overall}\n")

    if not all_passed and raise_on_failure:
        raise ValueError("A suíte de qualidade de dados falhou. Veja o relatório acima.")

    return all_passed


# ---------------------------------------------------------------------------
# Auxiliar Interno
# ---------------------------------------------------------------------------

def _log(result: dict) -> None:
    """Gera log baseado no resultado da verificação."""
    level = logging.INFO if result["passed"] else logging.WARNING
    logger.log(level, f"[{result['check_name']}] {result['details']}")
