# GloboRetail Hybrid ETL & ELT Data Pipeline

A hybrid data-engineering pipeline for **GloboRetail**. Apache Airflow orchestrates the ETL workflow, while Snowflake provides the warehouse, star schema, and analytical presentation layer.

## Project Overview

The pipeline processes retail sales and product data from Amazon S3. Python and Pandera validate and transform the source data, preserve rejected records for traceability, and write validated data to the processed S3 zone as Parquet. Snowflake then loads the processed data into a cleansed layer, builds a dimensional model, and exposes materialized views for analysis.

## Architecture

```mermaid
flowchart TD
    SALES[Sales CSV] --> RAW[Amazon S3: Raw Zone]
    PRODUCTS[Products JSON] --> RAW

    RAW --> AIRFLOW
    subgraph AIRFLOW[Apache Airflow ETL]
        direction TB
        E[Extract] --> IV[Input validation] --> T[Transform] --> OV[Strict output validation] --> L[Load processed data]
    end

    T --> REJECTED[Amazon S3: Rejected Zone]
    L --> PROCESSED[Amazon S3: Processed Zone]
    PROCESSED --> STAGE[Snowflake external stage]

    subgraph SNOWFLAKE[Snowflake ELT]
        direction TB
        CLEANSED[CLEANSED layer] --> STAR[STAR schema] --> PRESENTATION[PRESENTATION layer]
    end

    STAGE --> CLEANSED
```

## Technology Stack

| Technology | Purpose |
| --- | --- |
| Python, Pandas, Pandera | ETL transformations and data validation |
| Apache Airflow / Astronomer Astro | Workflow orchestration and local development |
| Amazon S3 | Raw, processed, and rejected data zones |
| PyArrow / Parquet | Typed, columnar processed-data format |
| Snowflake / SQL | ELT, dimensional modelling, and analytics |
| Docker | Airflow runtime environment |

## Data Sources

Two datasets are stored in the S3 raw zone:

- **Sales CSV:** sales and product identifiers, region, quantity, price, timestamp, discount, and order status.
- **Products JSON:** product identifier, category, brand, rating, stock status, and launch date.

The source data intentionally includes quality inconsistencies such as mixed casing, missing values, and malformed values.

## Pipeline Flow

```mermaid
flowchart TD
    RAW[S3 raw zone] --> EXTRACT[Extract]
    EXTRACT --> INPUT[Input validation]
    INPUT --> TRANSFORM[Transform]
    TRANSFORM --> REJECTED[S3 rejected zone]
    TRANSFORM --> OUTPUT[Strict output validation]
    OUTPUT --> LOAD[Load Parquet]
    LOAD --> PROCESSED[S3 processed zone]
    PROCESSED --> SNOWFLAKE[Snowflake external stage]
    SNOWFLAKE --> CLEANSED[CLEANSED] --> STAR[STAR] --> PRESENTATION[PRESENTATION]
```

### Airflow orchestration

![Airflow DAG graph](docs/screenshots/airflow_dag_graph.png)

![Successful Airflow run](docs/screenshots/airflow_successfull_run.png)

## Data Quality

### Input validation

Input validation is lightweight and non-blocking. It checks expected columns, source-compatible data types, and structural consistency. Failures are logged so the transformation layer can determine whether values can be safely represented.

### Transformation-level rejection

Records are rejected only when a value cannot be safely converted to the analytical representation—for example, a price of `"twenty"`. Rejected sales and product records are retained in S3 for traceability rather than silently discarded.

### Strict output validation

The transformed retail dataset must satisfy a strict Pandera schema before it can enter the processed zone. Checks include final columns and types, nullability, unique sales identifiers, valid regions and statuses, positive quantities, non-negative prices, valid discounts, product completeness, rating ranges, and revenue consistency.

```text
gross_revenue   = quantity × price
discount_amount = gross_revenue × discount
net_revenue     = gross_revenue − discount_amount
```

Output validation is a hard quality gate: if it fails, `load_processed` is not executed.

### S3 zones and outputs

![S3 raw, processed, and rejected zones](docs/screenshots/s3_zones.png)

| Raw input data | Processed output | Rejected output |
| --- | --- | --- |
| ![S3 raw input](docs/screenshots/s3_raw_input_data.png) | ![S3 processed output](docs/screenshots/s3_processed_output.png) | ![S3 rejected output](docs/screenshots/s3_rejected_output.png) |

The final validated dataset is written as `processed-zone/sales_clean.parquet`. Rejected data is written as separate sales and products Parquet outputs in the rejected zone.

## Snowflake Model

```mermaid
flowchart TD
    S3[S3 processed zone: sales_clean.parquet] --> EXT[EXTERNAL_STAGE.AWS_STAGE]
    EXT --> CLEAN[CLEANSED.SALES_CLEAN]
    CLEAN --> DATE[STAR.DIM_DATE]
    CLEAN --> PRODUCT[STAR.DIM_PRODUCT]
    CLEAN --> FACT[STAR.FACT_SALES]
    DATE --> FACT
    PRODUCT --> FACT
    FACT --> VIEWS[PRESENTATION materialized views]
```

### Database structure

```text
GLOBO_RETAIL_DB
├── EXTERNAL_STAGE
│   ├── AWS_STAGE
│   └── PARQUET_FILE_FORMAT
├── CLEANSED
│   └── SALES_CLEAN
├── STAR
│   ├── DIM_DATE
│   ├── DIM_PRODUCT
│   └── FACT_SALES
└── PRESENTATION
    ├── MV_SALES_BY_REGION_MONTH
    ├── MV_TOP_PRODUCTS_BY_REVENUE
    ├── MV_REVENUE_TREND
    └── MV_CATEGORY_PERFORMANCE
```

### Star schema

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_SALES : date_key
    DIM_PRODUCT ||--o{ FACT_SALES : product_key
    DIM_DATE {
        NUMBER date_key PK
        DATE full_date
        NUMBER year
        NUMBER quarter
        NUMBER month
    }
    DIM_PRODUCT {
        NUMBER product_key PK
        NUMBER product_id
        VARCHAR category
        VARCHAR brand
        FLOAT rating
    }
    FACT_SALES {
        NUMBER sales_id PK
        NUMBER date_key FK
        NUMBER product_key FK
        NUMBER quantity
        FLOAT net_revenue
    }
```

`DIM_DATE` provides reusable calendar attributes, `DIM_PRODUCT` stores descriptive product metadata, and `FACT_SALES` contains retail transactions and their analytical measures.

![Snowflake cleansed layer](docs/screenshots/snowflake_sales_clean_loaded.png)

| Date dimension | Product dimension | Sales fact |
| --- | --- | --- |
| ![DIM_DATE](docs/screenshots/snowflake_star_schema_dim_date.png) | ![DIM_PRODUCT](docs/screenshots/snowflake_star_schema_dim_products.png) | ![FACT_SALES](docs/screenshots/snowflake_star_schema_fact_sales.png) |

### Presentation layer

The `PRESENTATION` schema exposes materialized views for monthly regional sales, top products by revenue, revenue trends, and category performance.

| Sales by region and month | Top products by revenue |
| --- | --- |
| ![Sales by region and month view](docs/screenshots/snowflake_presentation_view_sales_by_region_monthly.png) | ![Top products view](docs/screenshots/snowflake_presentation_view_top_products.png) |

| Revenue trend | Category performance |
| --- | --- |
| ![Revenue trend view](docs/screenshots/snowflake_presentation_view_revenue_trend.png) | ![Category performance view](docs/screenshots/snowflake_presentation_view_category_performance.png) |

![Category performance analytical result](docs/screenshots/snowflake_analytical_result_category_performance.png)

## SQL Structure

The Snowflake implementation is split into ordered scripts:

```text
sql/
├── 01_snowflake_setup.sql
├── 02_loading_cleansed_data_from_s3.sql
├── 03_building_star_schema.sql
└── 04_creating_analytical_mvs_in_snowflake.sql
```

They configure the database objects, load `CLEANSED.SALES_CLEAN`, build the star schema, and create the analytical materialized views.

## Testing

The project tests Pandera schemas, transformation logic, S3 loaders, and Airflow tasks. Warehouse checks compare cleansed and fact-table row counts, look for duplicate identifiers and missing dimension keys, and compare revenue totals.

Run tests and validate DAG parsing from the Astro environment:

```bash
astro dev pytest
astro dev parse
```

## Project Structure

```text
globo-retail-data-pipeline/
├── dags/                 # Airflow DAGs
├── include/              # ETL, pipelines, validation, configuration, and utilities
├── sql/                  # Snowflake setup and ELT scripts
├── tests/unit/           # Unit tests
├── docs/screenshots/     # Project screenshots used in this README
├── Dockerfile
├── requirements.txt
└── README.md
```

## Running the Project

Prerequisites: Docker, Astronomer Astro CLI, an AWS account and S3 bucket, a Snowflake account, and a configured Airflow AWS connection.

```bash
git clone https://github.com/PlamenPlamenovStanchev/globo-retail-data-pipeline.git
cd globo-retail-data-pipeline
astro dev start
```

Configure the required Airflow connection, place the source datasets in the configured S3 raw zone, and trigger the DAG. Execute the Snowflake scripts in numerical order after the ETL output is available.

## Configuration and Security

Runtime configuration is kept separate from credentials and includes S3 zones, Airflow connection IDs, and Snowflake object names. Do not commit authentication values; use placeholders or secret-management facilities instead.

## Key Design Decisions

- **Hybrid ETL + ELT:** Python handles source processing; Snowflake handles warehouse transformations.
- **No DataFrames in XCom:** tasks exchange lightweight metadata and S3 object references.
- **Explicit quality layers:** input validation is observational; output validation blocks invalid analytical data.
- **Preserved rejected data:** unsafe source values remain available for investigation.
- **Parquet interchange:** typed, efficient storage connects ETL with Snowflake.

## Future Improvements

- Add automated data-quality alerts and observability metrics for pipeline failures and rejected-record volumes.
- Introduce incremental Snowflake loads to reduce processing time as source volumes grow.

## Author

Plamen Stanchev

Data Engineering course project demonstrating a hybrid ETL and ELT retail analytics pipeline using Apache Airflow, Amazon S3, Pandera, Parquet, and Snowflake.
