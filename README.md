Project Overview

The primary goal is to transform raw retail data (sales, inventory, and customer profiles) into an analysis-ready state that supports both historical reporting and real-time decision-making.
Architectural Layers

Bronze (Raw Layer): Ingests raw data from source systems (e.g., POS transactions, CRM records) in their original format using Databricks Auto Loader. This layer preserves the data's fidelity for auditing and reprocessing.

Silver (Validated & Enriched Layer): Performs data cleansing, deduplication, and schema enforcement. This is the "Enterprise view" where disparate sources are conformed into a unified structure.

Gold (Curated Layer): The final presentation layer is organized into a Star Schema with dimension and fact tables. It is optimized for high-performance querying by BI tools and data science teams.

Key Technical Implementations

Slowly Changing Dimensions (SCD):

SCD Type 1: Used for attributes where history is not required (e.g., correcting a customer's name spelling), simply overwriting the old value.

SCD Type 2: Critical for retail analytics (e.g., tracking a customer's address changes over time). It uses effective_start_date, effective_end_date, and an is_current flag to maintain a full history of changes.

Dimensional Modeling:

Fact Tables: Store quantitative retail metrics like total_sales, quantity_sold, and discount_amount.

Dimension Tables: Contain descriptive attributes like dim_product, dim_store, and dim_customer.

Delta Lake MERGE: Employs the Delta Lake MERGE command to handle UPSERTs and SCD Type 2 logic efficiently, ensuring ACID compliance during updates.


Retail Business Value:

Customer Insights: Analyze purchasing behavior over time, even as customer demographics or locations change.

Inventory Accuracy: Maintain historical stock levels across various store locations to optimize supply chain management.

Sales Performance: Power dashboards with aggregated data in the Gold layer to track regional sales trends and promotional effectiveness.
