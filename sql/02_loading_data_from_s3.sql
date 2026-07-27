SELECT COUNT(*)
FROM @AWS_STAGE;

-- creating the CLEANSED.SALES_CLEAN table to store the cleansed sales data from the raw data in S3. This will allow for more efficient querying and analysis of the sales data. --
CREATE OR REPLACE TABLE GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN (
    sales_id NUMBER(38,0) NOT NULL,
    product_id NUMBER(38,0) NOT NULL,
    timestamp TIMESTAMP_NTZ NOT NULL,
    region VARCHAR NOT NULL,
    quantity NUMBER(38,0) NOT NULL,
    price FLOAT NOT NULL,
    discount FLOAT NOT NULL,
    order_status VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    rating FLOAT NOT NULL,
    in_stock BOOLEAN NOT NULL,
    launch_date TIMESTAMP_NTZ,
    gross_revenue FLOAT NOT NULL,
    discount_amount FLOAT NOT NULL,
    net_revenue FLOAT NOT NULL
);

-- describing the table structure--
DESC TABLE CLEANSED.SALES_CLEAN;


-- loading the cleansed sales data from the S3 bucket into the CLEANSED.SALES_CLEAN table. This will allow for more efficient querying and analysis of the sales data. --
COPY INTO GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN
FROM @GLOBO_RETAIL_DB.EXTERNAL_STAGE.AWS_STAGE
FILE_FORMAT = (
    FORMAT_NAME = 'GLOBO_RETAIL_DB.EXTERNAL_STAGE.PARQUET_FILE_FORMAT'
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- checking the number of rows in the CLEANSED.SALES_CLEAN table to ensure that the data has been loaded correctly. --
SELECT COUNT(*) FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN;

-- checking the first 10 rows of the CLEANSED.SALES_CLEAN table to ensure that the data has been loaded correctly. --
SELECT *
FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN
LIMIT 10;