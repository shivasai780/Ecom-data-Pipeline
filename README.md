# E-commerce Data Pipeline using Azure and Databricks

## Overview

This project demonstrates the implementation of an end-to-end data pipeline for managing and transforming e-commerce data using Azure Data Lake Storage (ADLS), Azure Data Factory (ADF), and Databricks. The pipeline follows the medallion architecture, which includes Bronze, Silver, and Gold layers for data processing and transformation.

## Architecture

### Steps

1. **Data Ingestion to Azure Data Lake Storage (ADLS)**
    - **Landing Zone 1:** Ingest raw data into ADLS Blob storage.

2. **Data Transformation using Azure Data Factory (ADF)**
    - Convert CSV files to Parquet format.
    - Store transformed data back into ADLS in **Landing Zone 2**.

3. **Data Processing with Databricks and Medallion Architecture**
    - **Bronze Layer:** Read Parquet files, create Delta tables, and store raw data.
    - **Silver Layer:** Perform data transformations to clean and enrich the data.
    - **Gold Layer:** Create a consolidated "one-big table" for analytics.

4. **Optional Step:** Use Azure Synapse Analytics for advanced data warehousing and analysis.

## Detailed Steps

### 1. Create the Azure Data Factory

- Create a new Resource Group to store all resources (ADF, Databricks, etc.).
- Create a new Storage Account.
    - Select the created Resource Group.
    - Enable Hierarchical Namespace (converts object storage to file storage).

### 2. Organize Storage Account Directories

- Create directories in the Storage Account:
    - **Landing Zone 1:** 
        - Subfolders: `User Raw1`, `Sellers Raw1`, `Countries Raw1`, `Buyers Raw1`
    - **Landing Zone 2:** 
        - Subfolders: `User Raw2`, `Sellers Raw2`, `Countries Raw2`, `Buyers Raw2`

### 3. Configure Azure Data Factory

- Launch ADF Studio.
- Create pipelines to copy and transform data.
    - Connect to the Storage Account using Azure Data Lake Gen 2.
    - Create linked services for Landing Zone 1 (source: CSV) and Landing Zone 2 (sink: Parquet).
    - Create pipelines for `User`, `Sellers`, `Buyers`, and `Countries`.

### 4. Set Up Databricks

- Launch Databricks Workspace.
- Mount the Storage Account to Databricks.
    - Register an application in Azure Active Directory (AAD).
    - Copy App ID, Tenant ID, and create a Client Secret.
    - Configure Databricks with these credentials.
    - Assign `Storage Blob Contributor` role to the application.

### 5. Implement Medallion Architecture in Databricks

- **Bronze Layer:**
    - Read Parquet files from Landing Zone 2.
    - Create Delta tables and store raw data.
- **Silver Layer:**
    - Transform and clean data.
    - Store enriched data.
- **Gold Layer:**
    - Create a consolidated table for analytics.

### 6. Automate Workflows

- Set up tasks in Databricks Workflows.
    - Create tasks for Bronze, Silver, and Gold layers.
    - Schedule and trigger workflows based on file arrival.

### 7. (Optional) Use Azure Synapse Analytics

- Serve the consolidated table for advanced analytics.

## Prerequisites

- Azure Subscription
- Basic knowledge of Azure services (ADLS, ADF, Databricks)
- Understanding of medallion architecture

## Technologies Used

- **Azure Data Lake Storage (ADLS)**
- **Azure Data Factory (ADF)**
- **Azure Databricks**
- **Azure Synapse Analytics (optional)**

## Conclusion

This project highlights the capabilities of Azure and Databricks in building a scalable and efficient data pipeline. By leveraging the medallion architecture, we ensure that the data is processed and transformed in a structured manner, enabling robust analytics and insights.
