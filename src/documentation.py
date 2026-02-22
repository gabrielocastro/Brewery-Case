import os
import logging
import pandas as pd
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from config import BASE_DIR

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Templates de Documentação (GITHUB README STYLE)
# ---------------------------------------------------------------------------

DESCRIPTIONS_EN = """
## 📊 Analytical Insights

### 1. Digital Maturity Score
- **Definition**: Percentage of breweries in a state that have both a registered website and phone number.
- **Insight**: High scores indicate a digitally advanced and reachable market.

### 2. Regional Diversity Index
- **Definition**: Count of unique brewery types present in the state.
- **Insight**: High variety indicates a heterogeneous and mature market.

### 3. Market Specialization (Micro Count)
- **Definition**: Total breweries vs. "micro" breweries per state.
- **Insight**: Highlights centers of craft beer production.

### 4. Data Trust Score
- **Definition**: Average completeness of Address, Phone, and Website.
- **Insight**: Indicator of business database reliability.

### 5. Geographic Hubs
- **Definition**: Cities with the highest concentration of breweries.
- **Insight**: Identifies competitive hotspots.

### 6. Breweries by Type and State
- **Definition**: Breakdown of brewery counts by category (micro, nano, brewpub, etc.) for each state.
- **Insight**: Useful for understanding the industrial profile of each region.

### 7. Global Distribution by Country and Type
- **Definition**: Comparison of brewery types across different countries.
- **Insight**: Reveals international trends in the brewing industry.

### 8. Geographic Coverage by State
- **Definition**: Deep dive into the distribution of breweries across cities within states.
- **Insight**: Essential for logistics and regional market penetration planning.
"""

DESCRIPTIONS_PT = """
## 📊 Insights Analíticos

### 1. Score de Maturidade Digital
- **Definição**: Porcentagem de cervejarias em um estado que possuem site e telefone registrados.
- **Insight**: Pontuações altas indicam um mercado digitalmente avançado e acessível.

### 2. Índice de Diversidade Regional
- **Definição**: Quantidade de tipos únicos de cervejaria presentes no estado.
- **Insight**: Alta variedade indica um mercado heterogêneo e maduro.

### 3. Especialização de Mercado (Contagem Micro)
- **Definição**: Total de cervejarias vs. cervejarias "micro" por estado.
- **Insight**: Destaca centros de produção artesanal.

### 4. Data Trust Score (Confiança de Dados)
- **Definição**: Média de completude de campos críticos (Endereço, Telefone e Website).
- **Insight**: Indicador de confiabilidade da base de dados comercial.

### 5. Hubs Geográficos
- **Definição**: Cidades com a maior concentração de cervejarias.
- **Insight**: Identifica pontos estratégicos e competitivos.

### 6. Cervejarias por Tipo e Estado
- **Definição**: Detalhamento da contagem de cervejarias por categoria (micro, nano, brewpub, etc.) para cada estado.
- **Insight**: Útil para entender o perfil industrial de cada região.

### 7. Distribuição Global por País e Tipo
- **Definição**: Comparação dos tipos de cervejaria entre diferentes países.
- **Insight**: Revela tendências internacionais na estrutura do mercado cervejeiro.

### 8. Cobertura Geográfica por Estado
- **Definição**: Mergulho profundo na distribuição de cervejarias entre as cidades dentro dos estados.
- **Insight**: Essencial para planejamento logístico e penetração de mercado regional.
"""

# ---------------------------------------------------------------------------
# Classe PDF
# ---------------------------------------------------------------------------

class DocPDF(FPDF):
    def __init__(self, font_name="ArialUni", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_font_name = font_name

    def header(self):
        self.set_font(self.custom_font_name, "B", 12)
        self.cell(self.epw, 10, "Open Brewery Data Lake - Relatório Estratégico", border=0, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font(self.custom_font_name, "I", 8)
        self.cell(self.epw, 10, f"Gerado em {pd.Timestamp.now().strftime('%d/%m/%Y')} - Página {self.page_no()}", border=0, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

# ---------------------------------------------------------------------------
# Templates e Lógica de Escrita
# ---------------------------------------------------------------------------

DEFAULT_DOC_EN = """# Open Brewery Data Lake Documentation

## Project Overview
This project establishes a modern Data Lake using the Medallion Architecture (Bronze, Silver, Gold). The objective is to ingest, clean, and analyze global brewery data from the Open Brewery DB API to derive business insights.

## Data Architecture
### 1. Bronze Layer (Raw)
- **Process**: Automated ingestion from Open Brewery DB via REST API.
- **Storage**: JSON format, preserving raw state.

### 2. Silver Layer (Cleaned)
- **Process**: Data cleaning, deduplication, and standardization.
- **Storage**: Optimized Parquet format partitioned by `brewery_type`.

### 3. Gold Layer (Analytical)
- **Process**: Complex aggregations and business metrics calculation.
- **Quality**: Automated checks verify data integrity.

<!-- ANALYTICAL_RESULTS_EN -->
"""

DEFAULT_DOC_PT = """# Documentação do Data Lake Open Brewery

## Visão Geral do Projeto
Este projeto estabelece um Data Lake moderno utilizando a Arquitetura Medalhão (Bronze, Silver, Gold). O objetivo é ingerir, limpar e analisar dados globais de cervejarias da API Open Brewery DB para gerar insights de negócios.

## Arquitetura de Dados
### 1. Camada Bronze (Raw)
- **Processo**: Ingestão automatizada da Open Brewery DB via API REST.
- **Armazenamento**: Formato JSON original preservando o estado bruto.

### 2. Camada Silver (Limpa)
- **Processo**: Limpeza, deduplicação e padronização dos dados.
- **Armazenamento**: Formato Parquet otimizado, particionado por `brewery_type`.

### 3. Camada Gold (Analítica)
- **Processo**: Agregações complexas e cálculo de métricas de negócio.
- **Qualidade**: Verificações automatizadas garantem a integridade dos dados.

<!-- ANALYTICAL_RESULTS_PT -->
"""

def update_markdowns():
    """Sobrescreve os arquivos MD com a documentação atualizada no estilo README."""
    doc_en_path = os.path.join(BASE_DIR, "doc_en.md")
    doc_pt_path = os.path.join(BASE_DIR, "doc_pt.md")
    
    # Gerar Inglês
    content_en = DEFAULT_DOC_EN.replace("<!-- ANALYTICAL_RESULTS_EN -->", DESCRIPTIONS_EN)
    with open(doc_en_path, "w", encoding="utf-8") as f:
        f.write(content_en)
    
    # Gerar Português
    content_pt = DEFAULT_DOC_PT.replace("<!-- ANALYTICAL_RESULTS_PT -->", DESCRIPTIONS_PT)
    with open(doc_pt_path, "w", encoding="utf-8") as f:
        f.write(content_pt)
    
    logger.info("Arquivos Markdown (estilo README) atualizados com sucesso.")

def render_table(pdf, df, title):
    """Renderiza um DataFrame como tabela no PDF."""
    pdf.set_font(pdf.custom_font_name, "B", 12)
    pdf.multi_cell(pdf.epw, 10, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(2)
    
    # Limita a 10 linhas para não estourar páginas demais, mantendo o PDF executivo
    display_df = df.head(10).copy()
    
    pdf.set_font("Courier", "", 7)
    with pdf.table(borders_layout="SINGLE_TOP_LINE", cell_fill_color=240, cell_fill_mode="ROWS", line_height=5) as table:
        header = table.row()
        for col in display_df.columns:
            header.cell(str(col))
        for _, row_data in display_df.iterrows():
            row = table.row()
            for val in row_data:
                row.cell(str(val))
    pdf.ln(5)

def create_pdf(input_md, output_pdf, aggregations=None):
    pdf = DocPDF(font_name="ArialUni")
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Fontes do Windows para UTF-8 real
    font_paths = {
        "": r"C:\Windows\Fonts\arial.ttf",
        "B": r"C:\Windows\Fonts\arialbd.ttf",
        "I": r"C:\Windows\Fonts\ariali.ttf"
    }
    
    if os.path.exists(font_paths[""]):
        pdf.add_font("ArialUni", "", font_paths[""])
        if os.path.exists(font_paths["B"]): pdf.add_font("ArialUni", "B", font_paths["B"])
        if os.path.exists(font_paths["I"]): pdf.add_font("ArialUni", "I", font_paths["I"])
    else:
        pdf.custom_font_name = "Helvetica"
        logger.warning("Fonte Arial não encontrada, usando Helvetica.")

    pdf.add_page()
    with open(input_md, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line or line.startswith("<!--"): continue

        if line.startswith("# "):
            pdf.set_font(pdf.custom_font_name, "B", 16)
            pdf.multi_cell(pdf.epw, 10, line[2:], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.ln(5)
        elif line.startswith("## "):
            pdf.set_font(pdf.custom_font_name, "B", 14)
            pdf.multi_cell(pdf.epw, 10, line[3:], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.ln(3)
        elif line.startswith("### "):
            pdf.set_font(pdf.custom_font_name, "B", 12)
            pdf.multi_cell(pdf.epw, 10, line[4:], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            pdf.ln(2)
        elif line.startswith("- "):
            pdf.set_font(pdf.custom_font_name, "", 10)
            pdf.multi_cell(pdf.epw, 6, f"  • {line[2:]}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        else:
            line = line.replace("**", "")
            pdf.set_font(pdf.custom_font_name, "", 10)
            pdf.multi_cell(pdf.epw, 6, line, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Adiciona tabelas de resultados reais ao PDF
    if aggregations:
        pdf.add_page()
        pdf.set_font(pdf.custom_font_name, "B", 16)
        title = "Detailed Data Tables (Top Results)" if "en" in input_md.lower() else "Tabelas de Dados Detalhadas (Principais Resultados)"
        pdf.multi_cell(pdf.epw, 10, title, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(5)

        for name, df in aggregations.items():
            nice_name = name.replace("_", " ").title()
            render_table(pdf, df, nice_name)

    pdf.output(output_pdf)
    logger.info(f"PDF consolidado com tabelas gerado: {output_pdf}")

def run_documentation_pipeline(aggregations=None):
    """Executa a atualização de Markdown e geração de PDF."""
    logger.info("Iniciando pipeline de documentação executiva...")
    update_markdowns()
    
    doc_en = os.path.join(BASE_DIR, "doc_en.md")
    doc_pt = os.path.join(BASE_DIR, "doc_pt.md")
    
    create_pdf(doc_en, os.path.join(BASE_DIR, "Project_Documentation_EN.pdf"), aggregations=aggregations)
    create_pdf(doc_pt, os.path.join(BASE_DIR, "Project_Documentation_PT.pdf"), aggregations=aggregations)
    logger.info("Pipeline de documentação finalizado.")

if __name__ == "__main__":
    run_documentation_pipeline()
