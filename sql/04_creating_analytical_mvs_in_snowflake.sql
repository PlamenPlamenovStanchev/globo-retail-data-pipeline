--monthly sales by region--
CREATE OR REPLACE MATERIALIZED VIEW PRESENTATION.MV_SALES_BY_REGION_MONTH AS
SELECT
    region,
    DATE_TRUNC('MONTH', transaction_timestamp) AS sales_month,
    COUNT(*) AS sales_count,
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(net_revenue) AS net_revenue
FROM GLOBO_RETAIL_DB.STAR.FACT_SALES
GROUP BY
    region,
    DATE_TRUNC('MONTH', transaction_timestamp);


-- checking the results of the materialized view--
SELECT *
FROM PRESENTATION.MV_SALES_BY_REGION_MONTH
ORDER BY sales_month, region;


--monthly revenue trend--
CREATE OR REPLACE MATERIALIZED VIEW PRESENTATION.MV_REVENUE_TREND AS
SELECT
    DATE_TRUNC('MONTH', transaction_timestamp) AS sales_month,
    COUNT(*) AS sales_count,
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(net_revenue) AS net_revenue
FROM GLOBO_RETAIL_DB.STAR.FACT_SALES
GROUP BY
    DATE_TRUNC('MONTH', transaction_timestamp);


-- checking the results of the materialized view--
SELECT *
FROM PRESENTATION.MV_REVENUE_TREND
ORDER BY sales_month;


--top products by revenue--
CREATE OR REPLACE MATERIALIZED VIEW PRESENTATION.MV_TOP_PRODUCTS_BY_REVENUE AS
SELECT
    product_key,
    COUNT(*) AS sales_count,
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue
FROM GLOBO_RETAIL_DB.STAR.FACT_SALES
GROUP BY product_key;

-- checking the results of the materialized view--
SELECT *
FROM PRESENTATION.MV_TOP_PRODUCTS_BY_REVENUE
ORDER BY net_revenue DESC
LIMIT 20;


-- Category performance --
CREATE OR REPLACE MATERIALIZED VIEW
    GLOBO_RETAIL_DB.PRESENTATION.MV_CATEGORY_PERFORMANCE AS
SELECT
    category,
    COUNT(*) AS sales_count,
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(net_revenue) AS net_revenue
FROM GLOBO_RETAIL_DB.STAR.FACT_SALES
GROUP BY category;


-- checking the results of the materialized view--
SELECT *
FROM GLOBO_RETAIL_DB.PRESENTATION.MV_CATEGORY_PERFORMANCE
ORDER BY net_revenue DESC;


---showing all materialized views in the presentation schema--
SHOW MATERIALIZED VIEWS
IN SCHEMA GLOBO_RETAIL_DB.PRESENTATION;
