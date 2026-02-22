# testeCase
# Open Brewery Data Lake Documentation

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

-------------------------------------------------------------------------------------

# Documentação do Data Lake Open Brewery

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
