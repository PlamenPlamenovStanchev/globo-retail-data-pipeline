-- create a star schema in Snowflake using the cleansed data from the previous step. The star schema will consist of a fact table and two dimension tables: DIM_DATE and DIM_PRODUCT.--
CREATE OR REPLACE TABLE STAR.DIM_DATE AS
SELECT DISTINCT
    TO_NUMBER(TO_CHAR(CAST(timestamp AS DATE), 'YYYYMMDD')) AS date_key,
    CAST(timestamp AS DATE) AS full_date,
    YEAR(timestamp) AS year,
    QUARTER(timestamp) AS quarter,
    MONTH(timestamp) AS month,
    MONTHNAME(timestamp) AS month_name,
    DAY(timestamp) AS day,
    DATE_PART(dayofweek_iso, timestamp) AS day_of_week,
    DAYNAME(timestamp) AS day_name
FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN;

-- check if the table is created successfully --
SELECT *
FROM STAR.DIM_DATE
ORDER BY full_date
LIMIT 20;


SELECT COUNT(*) AS date_rows
FROM STAR.DIM_DATE;

-- creating the DIM_PRODUCT table to include product category in the star schema. This will allow for more detailed analysis of sales data by product category. --
CREATE OR REPLACE TABLE STAR.DIM_PRODUCT AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    category,
    brand,
    rating,
    in_stock,
    CAST(launch_date AS DATE) AS launch_date
FROM (
    SELECT DISTINCT
        product_id,
        category,
        brand,
        rating,
        in_stock,
        launch_date
    FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN
);


-- check if the table is created successfully --
SELECT *
FROM STAR.DIM_PRODUCT
ORDER BY product_key
LIMIT 20;


SELECT
    COUNT(*) AS dimension_rows,
    COUNT(DISTINCT product_id) AS distinct_products
FROM STAR.DIM_PRODUCT;

-- creating the FACT_SALES table to include product category in the star schema. This will allow for more detailed analysis of sales data by product category. --
CREATE OR REPLACE TABLE STAR.FACT_SALES AS
SELECT
    s.sales_id,
    d.date_key,
    p.product_key,
    s.timestamp AS transaction_timestamp,
    s.region,
    s.order_status,
    p.category,
    s.quantity,
    s.price,
    s.discount,
    s.gross_revenue,
    s.discount_amount,
    s.net_revenue
FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN AS s
JOIN STAR.DIM_DATE AS d
    ON CAST(s.timestamp AS DATE) = d.full_date
JOIN STAR.DIM_PRODUCT AS p
    ON s.product_id = p.product_id;


-- check if the table is created successfully --
SELECT *
FROM STAR.FACT_SALES
ORDER BY sales_id
LIMIT 20;

SELECT COUNT(*) AS fact_rows
FROM STAR.FACT_SALES;

--performing some sanity checks to ensure that the data in the fact table matches the cleansed data
SELECT COUNT(*) AS cleansed_rows
FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN;

--performing some sanity checks to ensure that the data in the fact table matches the cleansed data
SELECT
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(net_revenue) AS net_revenue
FROM STAR.FACT_SALES;

SELECT
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(net_revenue) AS net_revenue
FROM GLOBO_RETAIL_DB.CLEANSED.SALES_CLEAN;
