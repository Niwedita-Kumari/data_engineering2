# Data Engineering Project - Medallion Architecture -  ADF - DATABRICKS - POWER BI

***Implement **Medallion Architecture** using **Azure Data Factory (ADF)**, **Azure Databricks**, **ADLS Gen2**, **Azure SQL**, and **Power BI**.

---

## 🎯 Project Overview

This repository contains the complete code and resources for a **production-grade Azure Data Engineering project** that demonstrates:

- **Multi-source data ingestion** (CSV/json files)
- **Medallion Architecture** (Bronze → Silver → Gold layers)
- **Automated ETL pipelines** with Azure Data Factory
- **Advanced PySpark transformations** in Databricks
- **Star schema data modeling** in Azure SQL
- **Interactive Power BI dashboards** with DAX

---

## 🏗️ Architecture Overview

Raw Data Sources (CSV/json)
↓
Azure Data Factory (Ingestion)
↓
ADLS Gen2 (Bronze Layer - Raw)
↓
Azure Databricks (PySpark)
├── Bronze → Silver (Cleansed)
├── Silver → Gold (Aggregated)
↓
Azure SQL Database (Star Schema)
↓
Power BI (Dashboards)

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Ingestion** | Azure Data Factory | Automated ETL pipelines |
| **Processing** | Azure Databricks (PySpark) | Medallion transformations |
| **Storage** | ADLS Gen2 + Delta Lake | Layered data lake |
| **Data Warehouse** | Azure SQL Database | Star schema modeling |
| **Visualization** | Power BI (DAX) | Interactive dashboards |

---

## 📁 Project Structure

azure-data-engineer-multi-source/
├── /data/
│ ├── raw/ # Bronze layer (ingested files)
│ ├── silver/ # Cleansed & validated data
│ └── gold/ # Aggregated business tables
├── /databricks/
│ ├── bronze/ # Raw data processing
│ ├── silver/ # Data cleansing & validation
│ └── gold/ # Business logic & aggregations
├── /adf/
│ ├── pipelines/ # ADF pipeline JSON
│ └── linked-services/ # Connection configs
├── /sql/
│ ├── dim_tables.sql # Dimension tables
│ └── fact_tables.sql # Fact tables
├── /powerbi/
│ └── dashboard.pbix # Power BI report
└── README.md

---

## 🚀 Setup Instructions

### Prerequisites
- Azure subscription
- Power BI Desktop
- VS Code / Azure Data Studio (optional)

### Azure Resource Setup

- Create ADLS Gen2 storage account
- Provision Azure SQL Database
- Deploy Azure Data Factory
- Create Databricks workspace

### Configure Connections
- Update ADF linked services with your storage keys
- Set Databricks cluster configurations
- Update SQL connection strings

---

## 🔄 Pipeline Flow

1. **Data Ingestion** (ADF): CSV/json → ADLS Bronze
2. **Bronze Processing** (Databricks): Raw → Delta format
3. **Silver Layer** (Databricks): Data cleansing & validation
4. **Gold Layer** (Databricks): Business aggregations
5. **Data Warehouse** (SQL): Star schema population
6. **Visualization** (Power BI): Interactive dashboards

---

## 🎓 Learning Outcomes

✅ **Production-grade Medallion Architecture** implementation  
✅ **Automated ETL pipelines** with ADF + Databricks  
✅ **Delta Lake** for ACID transactions & time travel  
✅ **Star schema design** in Azure SQL  
✅ **DAX calculated measures** in Power BI  
✅ **Data validation** & error handling patterns  
✅ **Cloud cost optimization** strategies  

