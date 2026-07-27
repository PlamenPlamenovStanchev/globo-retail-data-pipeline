USE WAREHOUSE COMPUTE_WH;

-- creating the necessary database. The GLOBO_RETAIL_DB database will contain the necessary schemas and tables for the star schema design. --
CREATE DATABASE IF NOT EXISTS GLOBO_RETAIL_DB;


-- use the GLOBO_RETAIL_DB database to create the necessary schemas and tables for the star schema design. The CLEANSED schema will contain the cleansed data, the STAR schema will contain the star schema tables, and the PRESENTATION schema will contain the presentation layer tables. --
USE DATABASE GLOBO_RETAIL_DB;

-- creating the necessary schemas. The CLEANSED schema will contain the cleansed data, the STAR schema will contain the star schema tables, and the PRESENTATION schema will contain the presentation layer tables. --
CREATE SCHEMA IF NOT EXISTS CLEANSED;
CREATE SCHEMA IF NOT EXISTS STAR;
CREATE SCHEMA IF NOT EXISTS PRESENTATION;


-- showing the created schemas. 
SHOW SCHEMAS IN DATABASE GLOBO_RETAIL_DB;


-- creating external stage to access the processed data in S3 bucket. The external stage will allow for easy access to the processed data for further analysis and reporting. --
CREATE SCHEMA IF NOT EXISTS GLOBO_RETAIL_DB.EXTERNAL_STAGE;


-- creating a file format for the external stage to specify the format of the processed data in S3 bucket. The file format will allow for easy access to the processed data for further analysis and reporting. --
CREATE OR REPLACE FILE FORMAT
    GLOBO_RETAIL_DB.EXTERNAL_STAGE.PARQUET_FILE_FORMAT
    TYPE = PARQUET
    USE_VECTORIZED_SCANNER = TRUE;


-- creating an external stage to access the processed data in S3 bucket. The external stage will allow for easy access to the processed data for further analysis and reporting. --
CREATE OR REPLACE STAGE GLOBO_RETAIL_DB.EXTERNAL_STAGE.AWS_STAGE
    URL = 's3://etl-and-data-warehouse/globo-retail-data-pipeline/processed-zone/'
    CREDENTIALS = (
    AWS_KEY_ID = '<YOUR_AWS_KEY_ID>'
    AWS_SECRET_KEY = '<YOUR_AWS_SECRET_KEY>'
        );


--checking if the external stage is created successfully. The external stage will allow for easy access to the processed data for further analysis and reporting. --
LIST @GLOBO_RETAIL_DB.EXTERNAL_STAGE.AWS_STAGE;
