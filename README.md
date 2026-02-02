# Cloud Data Warehousing with Snowflake

## Business Context
Modern supply chains generate massive volumes of data from ERP systems, IoT sensors, and transactional databases. A well-designed data warehouse enables organizations to consolidate this data for analytics, reporting, and AI/ML applications. This project demonstrates end-to-end data warehouse implementation using industry best practices.

## Project Highlights

### Architecture Implemented
Built a **production-ready data warehouse** following the Medallion Architecture:

| Layer | Purpose | Example Tables |
|-------|---------|----------------|
| **Bronze** | Raw data ingestion | `raw_books` (staging) |
| **Silver** | Cleaned & normalized | `dim_book`, `dim_author`, `fact_book_ratings` |
| **Gold** | Business aggregates | `gold_author_performance`, `gold_publication_trends` |

### Key Features
- **Star Schema Design**: Optimized dimensional model for query performance
- **ETL Pipeline**: Complete data transformation from raw to analytics-ready
- **AI Integration**: Snowflake Cortex for sentiment analysis and text summarization
- **Scalable Architecture**: Cloud-native design supporting enterprise workloads

## Technologies
| Category | Tools |
|----------|-------|
| Data Platform | Snowflake |
| Languages | SQL (DDL, DML, CTEs, Window Functions) |
| AI/ML | Snowflake Cortex |
| Architecture | Medallion (Bronze/Silver/Gold), Star Schema |

## Files
- `Books_Project.sql` - Complete warehouse setup and transformation scripts

## How to Run
Execute the SQL script in a Snowflake worksheet with appropriate warehouse and role permissions.

## Skills Demonstrated
- **Data Warehousing** - Dimensional modeling, star schema design, fact/dimension tables
- **ETL Development** - Data extraction, transformation, and loading pipelines
- **Cloud Platforms** - Snowflake administration, warehouse management
- **SQL Expertise** - Complex queries, CTEs, window functions, DDL/DML
- **Data Architecture** - Medallion architecture, data quality layers
- **AI/ML Integration** - Leveraging cloud AI services for text analytics

