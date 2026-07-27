# GloboRetail Hybrid ETL & ELT Data Pipeline

A hybrid Data Engineering pipeline developed for **GloboRetail**, combining an Apache Airflow-driven ETL workflow with an analytical ELT layer in Snowflake.

The project processes raw retail sales and product data from Amazon S3, validates and transforms the datasets using Python and Pandera, separates rejected records for traceability, stores validated analytical data back in S3 as Parquet, and uses Snowflake to build a cleansed warehouse layer, star schema, and analytical presentation layer.

---

## Project Overview

The pipeline follows a hybrid **ETL + ELT architecture**:

- **ETL** is orchestrated with Apache Airflow.
- Raw source data is extracted from Amazon S3.
- Input data is validated using Pandera.
- Sales and product datasets are transformed independently.
- Transformation-level rejected records are stored separately in S3.
- The transformed retail dataset passes strict output validation.
- Validated data is written to the S3 processed zone in Parquet format.
- **ELT** is performed inside Snowflake using SQL.
- Processed data is loaded into the `CLEANSED` layer.
- A dimensional star schema is built in the `STAR` layer.
- Analytical materialized views are exposed through the `PRESENTATION` layer.

The separation between ETL and ELT keeps Python responsible for source processing and data quality, while warehouse transformations remain inside Snowflake.

---

## Architecture

```mermaid
flowchart TD
    SALES[Sales CSV]
    PRODUCTS[Products JSON]

    SALES --> RAW
    PRODUCTS --> RAW

    RAW[Amazon S3<br/>Raw Zone]

    RAW --> AIRFLOW

    subgraph AIRFLOW[Apache Airflow ETL]
        direction TB

        E[Extract]
        IV[Input Validation]
        T[Transform]
        OV[Strict Output Validation]
        L[Load Processed Data]

        E --> IV
        IV --> T
        T --> OV
        OV --> L
    end

    T --> REJECTED[Amazon S3<br/>Rejected Zone]
    L --> PROCESSED[Amazon S3<br/>Processed Zone]

    PROCESSED --> STAGE[Snowflake External Stage]

    subgraph SNOWFLAKE[Snowflake ELT]
        direction TB

        CLEANSED[CLEANSED.SALES_CLEAN]
        STAR[STAR Schema]
        PRESENTATION[Presentation Layer]

        CLEANSED --> STAR
        STAR --> PRESENTATION
    end

    STAGE --> CLEANSED



## Technology Stack


| Technology        | Purpose                                            |
| ----------------- | -------------------------------------------------- |
| Python            | Core ETL processing                                |
| Apache Airflow    | Pipeline orchestration                             |
| Astronomer Astro  | Local Airflow development environment              |
| Pandas            | Dataset processing                                 |
| Pandera           | Data validation                                    |
| PyArrow / Parquet | Processed data storage                             |
| Amazon S3         | Raw, intermediate, processed, and rejected storage |
| Snowflake         | Data warehouse and ELT processing                  |
| SQL               | Warehouse transformations and analytics            |
| Docker            | Airflow runtime environment                        |
| Git / GitHub      | Version control and project hosting                |


## Data Sources

The pipeline processes two source datasets stored in the Amazon S3 raw zone.

Sales

The sales dataset contains transactional information including:

sales identifier
product identifier
region
quantity
price
timestamp
discount
order status

The source intentionally contains data-quality inconsistencies such as mixed casing, missing values, and malformed or invalid values that are handled by the pipeline.

Products

The product dataset contains product metadata including:

product identifier
category
brand
rating
stock status
launch date

## Pipeline Flow
Source Data
    ↓
S3 RAW Zone
    ↓
Apache Airflow ETL
    ├── Extract
    ├── Input validation
    ├── Transform
    │     └── Rejected records → S3 Rejected Zone
    ├── Strict output validation
    └── Load
          ↓
S3 Processed Zone
    ↓
Snowflake External Stage
    ↓
CLEANSED
    ↓
STAR
    ↓
PRESENTATION


## Data Quality

Data validation is deliberately divided into two different levels.

Input Validation

Input validation is lightweight and non-blocking.

Its purpose is to inspect the structure of incoming source data before transformation.

Checks include:

expected columns
basic source-compatible data types
structural consistency

Normal Pandera input-validation failures are logged but do not automatically stop the pipeline.

Input validation does not perform business transformations or reject records.

This allows the transformation layer to decide whether a source value can be represented safely.

Transformation-Level Rejection

Transformation rejects records only when values cannot safely be converted into the analytical representation.

For example:

price = "twenty"

cannot safely become a numeric price and can therefore be rejected during transformation.

A technically valid but business-invalid value such as:

price = -20

can still be represented numerically.

It therefore proceeds to the strict output validation layer, where it fails the analytical data contract.

This distinction avoids silently modifying or discarding source data.

Strict Output Validation

The final transformed dataset passes through a strict Pandera output schema.

Validation includes checks for:

expected final columns
data types
nullability
unique sales identifiers
valid regions
positive quantities
non-negative prices
discount ranges
allowed order statuses
product metadata completeness
product rating ranges
revenue consistency

Revenue relationships are also validated:

gross_revenue = quantity × price

discount_amount = gross_revenue × discount

net_revenue = gross_revenue - discount_amount

Unlike input validation, output validation is a hard quality gate.

If it fails:

validate_output → FAILED
load_processed → NOT EXECUTED

Only data satisfying the final analytical contract can enter the processed zone.

Transformation Logic
Sales Transformation

The sales transformation normalizes source fields into a consistent analytical model.

Examples include:

standardized column names
normalized region values
normalized order statuses
numeric conversion
timestamp parsing
revenue calculations

Calculated measures include:

gross_revenue
discount_amount
net_revenue
Product Transformation

Product transformation prepares metadata including:

product identifier
category
brand
rating
stock status
launch date

Nullable launch dates are preserved.

Retail Transformation

The final retail transformation combines already transformed sales and product data.

The join follows a many-to-one relationship:

many sales rows
        ↓
one product record

The pipeline explicitly validates this relationship to prevent accidental multiplication of sales records caused by duplicate product metadata.

S3 Output

The final processed dataset is written directly to:

s3://etl-and-data-warehouse/globo-retail-data-pipeline/processed-zone/sales_clean.parquet

Rejected data is written to:

s3://etl-and-data-warehouse/globo-retail-data-pipeline/rejected-zone/rejected_sales.parquet

s3://etl-and-data-warehouse/globo-retail-data-pipeline/rejected-zone/rejected_products.parquet

The processed dataset uses Parquet because it provides:

columnar storage
explicit schema information
efficient analytical reads
good interoperability with Snowflake

Screenshot placeholder — S3 Zones

Add screenshot showing the raw, processed and rejected zones.

Suggested path:

docs/images/s3_zones.png

![Amazon S3 Zones](docs/images/s3_zones.png)

Screenshot placeholder — Processed / Rejected Files

Suggested path:

docs/images/s3_pipeline_outputs.png

![S3 Pipeline Outputs](docs/images/s3_pipeline_outputs.png)

## Snowflake Model

After the Airflow ETL pipeline finishes, the processed Parquet dataset is consumed by Snowflake.

The ELT phase is implemented inside Snowflake using SQL, keeping warehouse transformations separate from the Python ETL pipeline.

flowchart TD
    S3[S3 Processed Zone<br/>sales_clean.parquet]

    S3 --> EXT[EXTERNAL_STAGE.AWS_STAGE]
    EXT --> CLEAN[CLEANSED.SALES_CLEAN]

    CLEAN --> DD[STAR.DIM_DATE]
    CLEAN --> DP[STAR.DIM_PRODUCT]

    DD --> FACT[STAR.FACT_SALES]
    DP --> FACT
    CLEAN --> FACT

    FACT --> M1[PRESENTATION<br/>MV_SALES_BY_REGION_MONTH]
    FACT --> M2[PRESENTATION<br/>MV_TOP_PRODUCTS_BY_REVENUE]
    FACT --> M3[PRESENTATION<br/>MV_REVENUE_TREND]
    FACT --> M4[PRESENTATION<br/>MV_CATEGORY_PERFORMANCE]



##Snowflake Database Structure
GLOBO_RETAIL_DB
│
├── EXTERNAL_STAGE
│   ├── AWS_STAGE
│   └── PARQUET_FILE_FORMAT
│
├── CLEANSED
│   └── SALES_CLEAN
│
├── STAR
│   ├── DIM_DATE
│   ├── DIM_PRODUCT
│   └── FACT_SALES
│
└── PRESENTATION
    ├── MV_SALES_BY_REGION_MONTH
    ├── MV_TOP_PRODUCTS_BY_REVENUE
    ├── MV_REVENUE_TREND
    └── MV_CATEGORY_PERFORMANCE

##Star Schema

The warehouse analytical model follows a star schema.

erDiagram
    DIM_DATE ||--o{ FACT_SALES : "date_key"
    DIM_PRODUCT ||--o{ FACT_SALES : "product_key"

    DIM_DATE {
        NUMBER date_key
        DATE full_date
        NUMBER year
        NUMBER quarter
        NUMBER month
        VARCHAR month_name
        NUMBER day
        NUMBER day_of_week
        VARCHAR day_name
    }

    DIM_PRODUCT {
        NUMBER product_key
        NUMBER product_id
        VARCHAR category
        VARCHAR brand
        FLOAT rating
        BOOLEAN in_stock
        DATE launch_date
    }

    FACT_SALES {
        NUMBER sales_id
        NUMBER date_key
        NUMBER product_key
        TIMESTAMP transaction_timestamp
        VARCHAR region
        VARCHAR order_status
        VARCHAR category
        NUMBER quantity
        FLOAT price
        FLOAT discount
        FLOAT gross_revenue
        FLOAT discount_amount
        FLOAT net_revenue
    }

DIM_DATE

DIM_DATE provides reusable calendar attributes such as:

full date
year
quarter
month
month name
day
weekday
DIM_PRODUCT

DIM_PRODUCT stores descriptive product attributes:

source product ID
category
brand
rating
stock status
launch date

A surrogate product_key is used by the fact table.

FACT_SALES

FACT_SALES represents retail transactions.

It contains foreign keys to:

DIM_DATE
DIM_PRODUCT

and analytical measures including:

quantity
price
discount
gross revenue
discount amount
net revenue

Screenshot placeholder — Snowflake Star Schema Tables

Suggested path:

docs/images/snowflake_star_schema.png

![Snowflake Star Schema](docs/images/snowflake_star_schema.png)

Presentation Layer

The PRESENTATION schema exposes materialized analytical views intended for reporting and downstream analysis.

Sales by Region and Month
MV_SALES_BY_REGION_MONTH

Provides monthly regional performance including sales volume and revenue measures.

Top Products by Revenue
MV_TOP_PRODUCTS_BY_REVENUE

Aggregates product performance and allows products to be ranked by generated revenue.

Revenue Trend
MV_REVENUE_TREND

Provides monthly revenue metrics suitable for trend analysis.

Category Performance
MV_CATEGORY_PERFORMANCE

Aggregates sales and revenue measures at product-category level.

Screenshot placeholder — Presentation Materialized Views

Suggested path:

docs/images/snowflake_presentation_views.png

![Snowflake Presentation Views](docs/images/snowflake_presentation_views.png)

Screenshot placeholder — Analytical Query Result

Add one representative result, for example category performance or revenue trend.

Suggested path:

docs/images/snowflake_analytics_result.png

![Snowflake Analytical Result](docs/images/snowflake_analytics_result.png)
SQL Structure

The Snowflake implementation is separated into logical SQL scripts.

sql/
├── 01_snowflake_setup.sql
├── 02_load_cleansed.sql
├── 03_build_star_schema.sql
├── 04_create_materialized_views.sql
└── 05_validation_queries.sql
01_snowflake_setup.sql

Creates or configures:

database context
schemas
external stage
Parquet file format
02_loading_cleansed_data_from_s3.sql

Loads the processed Parquet dataset into:

CLEANSED.SALES_CLEAN
03_building_star_schema.sql

Builds:

STAR.DIM_DATE
STAR.DIM_PRODUCT
STAR.FACT_SALES
04_createing_analytical_mvs_in snowflake.sql

Creates the analytical presentation layer.

05_validation_queries.sql

Contains sanity and data-quality checks for the Snowflake warehouse.
## Testing

Validation and Testing

The project verifies data quality at multiple stages.

Python / ETL Validation

Pandera is used for:

lightweight input schemas
strict output schema
type validation
nullability rules
uniqueness
business constraints
revenue consistency
Snowflake Validation

Warehouse sanity checks include:

CLEANSED row count
FACT_SALES row count
duplicate sales identifiers
duplicate products
missing date keys
missing product keys
revenue total comparison
presentation object row counts

A key consistency check is:

CLEANSED.SALES_CLEAN row count
             =
STAR.FACT_SALES row count

Revenue totals are also compared between the cleansed dataset and fact table to ensure dimensional modelling has not altered measures.
## Project Structure

The repository is organized by responsibility.
globo-retail-data-pipeline/
│
├── dags/
│   └── retail_etl_dag.py
│
├── include/
│   ├── etl/
│   │   ├── extract_data/
│   │   ├── transform_data/
│   │   └── load_data/
│   │
│   ├── pipelines/
│   │   ├── retail_pipeline.py
│   │   └── task_groups.py
│   │
│   ├── validations/
│   │   ├── schemas/
│   │   ├── input_validator.py
│   │   └── output_validator.py
│   │
│   ├── exceptions/
│   ├── utils/
│   └── config.yaml
│
├── sql/
│   ├── 01_snowflake_setup.sql
│   ├── 02_loading_cleansed_data_from_s3.sql
│   ├── 03_building_star_schema.sql
│   ├── 04_analytical_mvs_in_snowflake.sql
│
│
├── tests/
├── Dockerfile
├── requirements.txt
├── packages.txt
└── README.md

Running the Project
Prerequisites

The project requires:

Docker
Astronomer Astro CLI
AWS account and S3 bucket
Snowflake account
configured Airflow AWS connection

Clone the repository:

git clone https://github.com/PlamenPlamenovStanchev/globo-retail-data-pipeline.git
cd globo-retail-data-pipeline

Start the Astro development environment:

astro dev start

Open the local Airflow UI and configure the required AWS connection if it is not already available.

Place the source datasets in the configured S3 raw zone and trigger the DAG.

Running Tests

Run the project tests from the Astro environment:

astro dev pytest

Validate DAG parsing with:

astro dev parse

A successful execution should produce:

S3 raw data
    ↓
successful Airflow DAG
    ↓
processed-zone/sales_clean.parquet
+
rejected-zone outputs where applicable

The SQL scripts can then be executed in Snowflake in numerical order.

Configuration and Security

Runtime configuration is stored separately from credentials.

The project uses configuration values for:

S3 bucket
raw zone
work zone
processed zone
rejected zone
Airflow connection IDs
Snowflake object names

Sensitive credentials should never be stored in Git.

The repository must use placeholders where authentication values are required.

Key Design Decisions
Hybrid ETL + ELT

Python performs source-oriented transformations, while analytical warehouse transformations remain inside Snowflake.

No DataFrames in Airflow XCom

Airflow tasks exchange lightweight metadata and S3 object references rather than serializing complete DataFrames through XCom.

Explicit Data Quality Layers

Input validation is observational and non-blocking.

Strict output validation is a blocking analytical quality gate.

Rejected Data Is Preserved

Records that cannot safely be transformed are stored rather than silently discarded.

Parquet for Processed Data

The processed zone uses Parquet to provide an efficient typed interchange format between the ETL pipeline and Snowflake.

Dimensional Modelling

The Snowflake warehouse separates descriptive dimensions from transactional measures through a star schema.

End-to-End Result

The completed solution implements the following architecture:
Sales CSV + Products JSON
            ↓
       Amazon S3 RAW
            ↓
      Apache Airflow
            ↓
 Extract / Validate / Transform
       ↙                 ↘
Rejected S3       Strict Validation
                           ↓
                    Processed S3
                           ↓
                  Snowflake Stage
                           ↓
                       CLEANSED
                           ↓
                         STAR
                           ↓
                     PRESENTATION

The result is a complete hybrid data pipeline demonstrating:

orchestration
cloud object storage
data validation
ETL
rejected-record handling
Parquet processing
Snowflake ingestion
ELT
dimensional modelling
analytical materialized views
data-quality verification

##Screenshots

Replace the placeholders below with final project screenshots.

Apache Airflow DAG
![Airflow DAG](docs/images/airflow_dag_graph.png)
Successful Airflow Execution
![Successful Airflow Run](docs/images/airflow_successful_run.png)
Amazon S3 Zones
![S3 Zones](docs/images/s3_zones.png)
Processed and Rejected Outputs
![S3 Outputs](docs/images/s3_pipeline_outputs.png)
Snowflake CLEANSED Layer
![Snowflake CLEANSED](docs/images/snowflake_sales_clean.png)
Snowflake STAR Schema
![Snowflake STAR Schema](docs/images/snowflake_star_schema.png)
Snowflake Presentation Layer
![Snowflake Presentation Layer](docs/images/snowflake_presentation_views.png)
Analytical Result
![Analytical Result](docs/images/snowflake_analytics_result.png)


## Future Improvements


##Author

Plamen Stanchev

Data Engineering course project demonstrating a hybrid ETL and ELT retail analytics pipeline using Apache Airflow, Amazon S3, Pandera, Parquet, and Snowflake.
