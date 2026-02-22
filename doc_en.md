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

