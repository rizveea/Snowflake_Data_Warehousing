-- set context 
Use Database DB_TEAM_THEDATATEAM;
Use Schema INFORMATION_SCHEMA;
Use Warehouse ANIMAL_TASK_WH;
Use role ROLE_TEAM_THEDATATEAM;

-- Create Schemas for different layers 

CREATE SCHEMA IF NOT EXISTS Bronze_THE_DATA_TEAM;
CREATE SCHEMA IF NOT EXISTS Silver_THE_DATA_TEAM;
CREATE SCHEMA IF NOT EXISTS Gold_THE_DATA_TEAM;

 
-- Verify the schemas were created
SHOW SCHEMAS IN DATABASE DB_TEAM_THEDATATEAM;

-- Enter Data into Bronze Layer Part 1 
USE SCHEMA Bronze_THE_DATA_TEAM;

-- Create a file format for CSV files
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    FIELD_DELIMITER = ','
    ESCAPE_UNENCLOSED_FIELD = NONE;

-- Create the raw books table matching your CSV structure exactly
CREATE OR REPLACE TABLE raw_books (
    row_index INTEGER,
    book_id INTEGER,
    authors STRING,
    original_publication_year FLOAT,
    title STRING,
    language_code STRING,
    average_rating FLOAT,
    image_url STRING,
    description STRING
);

-- Create Books Stage
CREATE OR REPLACE STAGE books_stage
    DIRECTORY = (ENABLE = true)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT = csv_format;

-- Load data from stage into the table
COPY INTO raw_books
    FROM @books_stage
    FILE_FORMAT = csv_format
    ON_ERROR = 'CONTINUE';

-- Verify the data loaded correctly
SELECT COUNT(*) AS total_rows FROM raw_books;

-- Preview sample data
SELECT * FROM raw_books LIMIT 10;


-- Identify and design appropriate dims,facts,and comp tables within silver later

-- set schema 
USE SCHEMA Silver_THE_DATA_TEAM;


-- Dimension Tables 

-- 1. Dimension: Language
CREATE OR REPLACE TABLE dim_language (
    language_key INTEGER AUTOINCREMENT PRIMARY KEY,
    language_code STRING,
    language_name STRING
);

-- 2. Dimension: Publication Year
CREATE OR REPLACE TABLE dim_publication_year (
    year_key INTEGER AUTOINCREMENT PRIMARY KEY,
    publication_year INTEGER,
    decade STRING,
    century STRING
);

-- 3. Dimension: Book
CREATE OR REPLACE TABLE dim_book (
    book_key INTEGER AUTOINCREMENT PRIMARY KEY,
    book_id INTEGER,
    title STRING,
    image_url STRING,
    description STRING
);

-- 4. Dimension: Author
CREATE OR REPLACE TABLE dim_author (
    author_key INTEGER AUTOINCREMENT PRIMARY KEY,
    author_name STRING
);


-- BRIDGE TABLE (for many-to-many relationship)
CREATE OR REPLACE TABLE bridge_book_author (
    book_key INTEGER,
    author_key INTEGER,
    FOREIGN KEY (book_key) REFERENCES dim_book(book_key),
    FOREIGN KEY (author_key) REFERENCES dim_author(author_key)
);

-- FACT TABLE
CREATE OR REPLACE TABLE fact_book_ratings (
    fact_key INTEGER AUTOINCREMENT PRIMARY KEY,
    book_key INTEGER,
    language_key INTEGER,
    year_key INTEGER,
    average_rating FLOAT,
    FOREIGN KEY (book_key) REFERENCES dim_book(book_key),
    FOREIGN KEY (language_key) REFERENCES dim_language(language_key),
    FOREIGN KEY (year_key) REFERENCES dim_publication_year(year_key)
);

-- Populate silver layer from bronze layer


-- Populate dim_language
INSERT INTO dim_language (language_code, language_name)
SELECT DISTINCT
    language_code,
    CASE language_code
        WHEN 'eng' THEN 'English'
        WHEN 'en-US' THEN 'English (US)'
        WHEN 'en-GB' THEN 'English (UK)'
        WHEN 'en-CA' THEN 'English (Canada)'
        WHEN 'spa' THEN 'Spanish'
        WHEN 'fre' THEN 'French'
        WHEN 'ger' THEN 'German'
        WHEN 'jpn' THEN 'Japanese'
        WHEN 'por' THEN 'Portuguese'
        WHEN 'ita' THEN 'Italian'
        WHEN 'rus' THEN 'Russian'
        WHEN 'chi' THEN 'Chinese'
        WHEN 'ara' THEN 'Arabic'
        WHEN 'dut' THEN 'Dutch'
        WHEN 'swe' THEN 'Swedish'
        WHEN 'nor' THEN 'Norwegian'
        WHEN 'dan' THEN 'Danish'
        WHEN 'fin' THEN 'Finnish'
        WHEN 'pol' THEN 'Polish'
        WHEN 'tur' THEN 'Turkish'
        WHEN 'kor' THEN 'Korean'
        WHEN 'gre' THEN 'Greek'
        WHEN 'heb' THEN 'Hebrew'
        WHEN 'hun' THEN 'Hungarian'
        WHEN 'cze' THEN 'Czech'
        WHEN 'rum' THEN 'Romanian'
        WHEN 'ind' THEN 'Indonesian'
        WHEN 'hin' THEN 'Hindi'
        WHEN 'ben' THEN 'Bengali'
        WHEN 'vie' THEN 'Vietnamese'
        WHEN 'tha' THEN 'Thai'
        WHEN 'ukr' THEN 'Ukrainian'
        WHEN 'cat' THEN 'Catalan'
        WHEN 'lat' THEN 'Latin'
        WHEN 'wel' THEN 'Welsh'
        WHEN 'gla' THEN 'Scottish Gaelic'
        WHEN 'gle' THEN 'Irish'
        WHEN 'mul' THEN 'Multiple Languages'
        WHEN 'zxx' THEN 'No Linguistic Content'
        ELSE COALESCE(language_code, 'Unknown')
    END AS language_name
FROM Bronze_THE_DATA_TEAM.raw_books
WHERE language_code IS NOT NULL;


-- Populate dim_publication_year
INSERT INTO dim_publication_year (publication_year, decade, century)
SELECT DISTINCT
    CAST(original_publication_year AS INTEGER) AS publication_year,
    CONCAT(CAST(FLOOR(original_publication_year / 10) * 10 AS INTEGER), 's') AS decade,
    CASE 
        WHEN original_publication_year < 0 THEN 'BC'
        WHEN original_publication_year < 1000 THEN '1st Millennium'
        WHEN original_publication_year < 1100 THEN '11th'
        WHEN original_publication_year < 1200 THEN '12th'
        WHEN original_publication_year < 1300 THEN '13th'
        WHEN original_publication_year < 1400 THEN '14th'
        WHEN original_publication_year < 1500 THEN '15th'
        WHEN original_publication_year < 1600 THEN '16th'
        WHEN original_publication_year < 1700 THEN '17th'
        WHEN original_publication_year < 1800 THEN '18th'
        WHEN original_publication_year < 1900 THEN '19th'
        WHEN original_publication_year < 2000 THEN '20th'
        ELSE '21st'
    END AS century
FROM Bronze_THE_DATA_TEAM.raw_books
WHERE original_publication_year IS NOT NULL;


-- Populate dim_book
INSERT INTO dim_book (book_id, title, image_url, description)
SELECT DISTINCT
    book_id,
    title,
    image_url,
    description
FROM Bronze_THE_DATA_TEAM.raw_books;

-- Populate dim_author (split multiple authors)
INSERT INTO dim_author (author_name)
SELECT DISTINCT TRIM(a.value) AS author_name
FROM Bronze_THE_DATA_TEAM.raw_books,
LATERAL FLATTEN(input => SPLIT(authors, ',')) a
WHERE TRIM(a.value) IS NOT NULL AND TRIM(a.value) != '';


-- Populate bridge_book_author
INSERT INTO bridge_book_author (book_key, author_key)
SELECT DISTINCT
    db.book_key,
    da.author_key
FROM Bronze_THE_DATA_TEAM.raw_books rb
JOIN dim_book db ON rb.book_id = db.book_id
CROSS JOIN LATERAL FLATTEN(input => SPLIT(rb.authors, ',')) a
JOIN dim_author da ON TRIM(a.value) = da.author_name;


-- Populate fact_book_ratings
INSERT INTO fact_book_ratings (book_key, language_key, year_key, average_rating)
SELECT
    db.book_key,
    dl.language_key,
    dy.year_key,
    rb.average_rating
FROM Bronze_THE_DATA_TEAM.raw_books rb
JOIN dim_book db ON rb.book_id = db.book_id
LEFT JOIN dim_language dl ON rb.language_code = dl.language_code
LEFT JOIN dim_publication_year dy ON CAST(rb.original_publication_year AS INTEGER) = dy.publication_year;



-- verify data loaded correctly 
SELECT 'dim_language' AS table_name, COUNT(*) AS row_count FROM dim_language
UNION ALL
SELECT 'dim_publication_year', COUNT(*) FROM dim_publication_year
UNION ALL
SELECT 'dim_book', COUNT(*) FROM dim_book
UNION ALL
SELECT 'dim_author', COUNT(*) FROM dim_author
UNION ALL
SELECT 'bridge_book_author', COUNT(*) FROM bridge_book_author
UNION ALL
SELECT 'fact_book_ratings', COUNT(*) FROM fact_book_ratings;

-- Preview some data
SELECT * FROM dim_language LIMIT 10;
SELECT * FROM dim_publication_year ORDER BY publication_year DESC LIMIT 10;
SELECT * FROM dim_book LIMIT 5;
SELECT * FROM dim_author LIMIT 10;
SELECT * FROM bridge_book_author LIMIT 10;
SELECT * FROM fact_book_ratings LIMIT 10;


-- Design and populate gold later 

-- set schema for gold 
USE SCHEMA GOLD_THE_DATA_TEAM;

-- Author Performance Analysis  
CREATE OR REPLACE TABLE gold_author_performance AS
SELECT
    da.author_key,
    da.author_name,
    COUNT(DISTINCT db.book_key) AS total_books,
    ROUND(AVG(f.average_rating), 2) AS avg_rating,
    ROUND(MIN(f.average_rating), 2) AS min_rating,
    ROUND(MAX(f.average_rating), 2) AS max_rating,
    ROUND(MAX(f.average_rating) - MIN(f.average_rating), 2) AS rating_range,
    MIN(dy.publication_year) AS first_publication_year,
    MAX(dy.publication_year) AS last_publication_year,
    MAX(dy.publication_year) - MIN(dy.publication_year) AS career_span_years
FROM Silver_THE_DATA_TEAM.dim_author da
JOIN Silver_THE_DATA_TEAM.bridge_book_author bba ON da.author_key = bba.author_key
JOIN Silver_THE_DATA_TEAM.dim_book db ON bba.book_key = db.book_key
JOIN Silver_THE_DATA_TEAM.fact_book_ratings f ON db.book_key = f.book_key
LEFT JOIN Silver_THE_DATA_TEAM.dim_publication_year dy ON f.year_key = dy.year_key
GROUP BY da.author_key, da.author_name;


-- Language Market Analysis 
CREATE OR REPLACE TABLE gold_language_summary AS
SELECT
    dl.language_key,
    dl.language_code,
    dl.language_name,
    COUNT(DISTINCT f.book_key) AS total_books,
    ROUND(AVG(f.average_rating), 2) AS avg_rating,
    ROUND(MIN(f.average_rating), 2) AS min_rating,
    ROUND(MAX(f.average_rating), 2) AS max_rating,
    ROUND(STDDEV(f.average_rating), 2) AS rating_stddev,
    ROUND(AVG(f.average_rating) - 
          (SELECT AVG(average_rating) FROM Silver_THE_DATA_TEAM.fact_book_ratings), 2) AS rating_vs_overall_avg
FROM Silver_THE_DATA_TEAM.dim_language dl
JOIN Silver_THE_DATA_TEAM.fact_book_ratings f ON dl.language_key = f.language_key
GROUP BY dl.language_key, dl.language_code, dl.language_name;


-- publication era trends 
CREATE OR REPLACE TABLE gold_publication_trends AS
SELECT
    dy.year_key,
    dy.publication_year,
    dy.decade,
    dy.century,
    COUNT(DISTINCT f.book_key) AS total_books,
    ROUND(AVG(f.average_rating), 2) AS avg_rating,
    ROUND(MIN(f.average_rating), 2) AS min_rating,
    ROUND(MAX(f.average_rating), 2) AS max_rating,
    ROUND(STDDEV(f.average_rating), 2) AS rating_stddev
FROM Silver_THE_DATA_TEAM.dim_publication_year dy
JOIN Silver_THE_DATA_TEAM.fact_book_ratings f ON dy.year_key = f.year_key
GROUP BY dy.year_key, dy.publication_year, dy.decade, dy.century;

-- Decade Summary view so we can visualize trends 
CREATE OR REPLACE TABLE gold_decade_summary AS
SELECT
    decade,
    century,
    SUM(total_books) AS total_books,
    ROUND(AVG(avg_rating), 2) AS avg_rating,
    MIN(min_rating) AS min_rating,
    MAX(max_rating) AS max_rating
FROM gold_publication_trends
GROUP BY decade, century
ORDER BY MIN(publication_year);


-- verify gold layer data 
SELECT 'gold_author_performance' AS table_name, COUNT(*) AS row_count FROM gold_author_performance
UNION ALL
SELECT 'gold_language_summary', COUNT(*) FROM gold_language_summary
UNION ALL
SELECT 'gold_publication_trends', COUNT(*) FROM gold_publication_trends
UNION ALL
SELECT 'gold_decade_summary', COUNT(*) FROM gold_decade_summary;


-- Sample Queries to test verification - 
-- Use Case 1: Top 10 authors by average rating (with at least 3 books)
SELECT 
    author_name,
    total_books,
    avg_rating,
    career_span_years
FROM gold_author_performance
WHERE total_books >= 3
ORDER BY avg_rating DESC
LIMIT 10;

-- Use Case 2: Language market comparison
SELECT 
    language_name,
    total_books,
    avg_rating,
    rating_vs_overall_avg
FROM gold_language_summary
ORDER BY total_books DESC;

-- Use Case 3: Best decades for books
SELECT 
    decade,
    century,
    total_books,
    avg_rating
FROM gold_decade_summary
ORDER BY avg_rating DESC
LIMIT 10;


-- Synthetic Data file for new stage 
USE SCHEMA Bronze_THE_DATA_TEAM;

-- create new stage 
CREATE OR REPLACE STAGE books_incremental_stage
    DIRECTORY = (ENABLE = true)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT = csv_format;

-- After uploading book_incremental.csv to this new stage, load the data
COPY INTO raw_books
    FROM @books_incremental_stage/book_incremental.csv
    FILE_FORMAT = csv_format
    ON_ERROR = 'CONTINUE';


--counts to compare later
SELECT 'BEFORE LOAD' AS status, 'Bronze.raw_books' AS table_name, COUNT(*) AS row_count FROM Bronze_THE_DATA_TEAM.raw_books
UNION ALL SELECT 'BEFORE LOAD', 'Silver.dim_book', COUNT(*) FROM Silver_THE_DATA_TEAM.dim_book
UNION ALL SELECT 'BEFORE LOAD', 'Silver.dim_author', COUNT(*) FROM Silver_THE_DATA_TEAM.dim_author
UNION ALL SELECT 'BEFORE LOAD', 'Silver.fact_book_ratings', COUNT(*) FROM Silver_THE_DATA_TEAM.fact_book_ratings
UNION ALL SELECT 'BEFORE LOAD', 'Gold.gold_author_performance', COUNT(*) FROM Gold_THE_DATA_TEAM.gold_author_performance
UNION ALL SELECT 'BEFORE LOAD', 'Gold.gold_publication_trends', COUNT(*) FROM Gold_THE_DATA_TEAM.gold_publication_trends;


-- Verify new rows in Bronze
SELECT 'New records in Bronze:' AS status;
SELECT * FROM raw_books WHERE book_id >= 50001;


-- silver layer implementation 
USE SCHEMA Silver_THE_DATA_TEAM;

-- insert languages 
INSERT INTO dim_language (language_code, language_name)
SELECT DISTINCT
    rb.language_code,
    CASE rb.language_code
        WHEN 'eng' THEN 'English'
        WHEN 'spa' THEN 'Spanish'
        WHEN 'jpn' THEN 'Japanese'
        ELSE COALESCE(rb.language_code, 'Unknown')
    END AS language_name
FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books rb
WHERE rb.book_id >= 50001
  AND rb.language_code IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dim_language dl 
      WHERE dl.language_code = rb.language_code
  );


  -- Insert new publication years only
INSERT INTO dim_publication_year (publication_year, decade, century)
SELECT DISTINCT
    CAST(rb.original_publication_year AS INTEGER) AS publication_year,
    CONCAT(CAST(FLOOR(rb.original_publication_year / 10) * 10 AS INTEGER), 's') AS decade,
    CASE 
        WHEN rb.original_publication_year < 2000 THEN '20th'
        ELSE '21st'
    END AS century
FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books rb
WHERE rb.book_id >= 50001
  AND rb.original_publication_year IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dim_publication_year dy 
      WHERE dy.publication_year = CAST(rb.original_publication_year AS INTEGER)
  );


-- Insert new books only
INSERT INTO dim_book (book_id, title, image_url, description)
SELECT DISTINCT
    rb.book_id,
    rb.title,
    rb.image_url,
    rb.description
FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books rb
WHERE rb.book_id >= 50001
  AND NOT EXISTS (
      SELECT 1 FROM dim_book db 
      WHERE db.book_id = rb.book_id
  );

-- Insert new authors only
INSERT INTO dim_author (author_name)
SELECT DISTINCT TRIM(a.value) AS author_name
FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books rb,
LATERAL FLATTEN(input => SPLIT(rb.authors, ',')) a
WHERE rb.book_id >= 50001
  AND TRIM(a.value) IS NOT NULL 
  AND TRIM(a.value) != ''
  AND NOT EXISTS (
      SELECT 1 FROM dim_author da 
      WHERE da.author_name = TRIM(a.value)
  );

  -- Insert new book-author relationships
INSERT INTO bridge_book_author (book_key, author_key)
SELECT DISTINCT
    db.book_key,
    da.author_key
FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books rb
JOIN dim_book db ON rb.book_id = db.book_id
CROSS JOIN LATERAL FLATTEN(input => SPLIT(rb.authors, ',')) a
JOIN dim_author da ON TRIM(a.value) = da.author_name
WHERE rb.book_id >= 50001
  AND NOT EXISTS (
      SELECT 1 FROM bridge_book_author bba 
      WHERE bba.book_key = db.book_key 
        AND bba.author_key = da.author_key
  );

-- Insert new fact records
INSERT INTO fact_book_ratings (book_key, language_key, year_key, average_rating)
SELECT
    db.book_key,
    dl.language_key,
    dy.year_key,
    rb.average_rating
FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books rb
JOIN dim_book db ON rb.book_id = db.book_id
LEFT JOIN dim_language dl ON rb.language_code = dl.language_code
LEFT JOIN dim_publication_year dy ON CAST(rb.original_publication_year AS INTEGER) = dy.publication_year
WHERE rb.book_id >= 50001
  AND NOT EXISTS (
      SELECT 1 FROM fact_book_ratings f 
      WHERE f.book_key = db.book_key
  );

  -- gold layer 

  USE SCHEMA Gold_THE_DATA_TEAM;


-- Recreate gold_author_performance
CREATE OR REPLACE TABLE gold_author_performance AS
SELECT
    da.author_key,
    da.author_name,
    COUNT(DISTINCT db.book_key) AS total_books,
    ROUND(AVG(f.average_rating), 2) AS avg_rating,
    ROUND(MIN(f.average_rating), 2) AS min_rating,
    ROUND(MAX(f.average_rating), 2) AS max_rating,
    ROUND(MAX(f.average_rating) - MIN(f.average_rating), 2) AS rating_range,
    MIN(dy.publication_year) AS first_publication_year,
    MAX(dy.publication_year) AS last_publication_year,
    MAX(dy.publication_year) - MIN(dy.publication_year) AS career_span_years
FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_author da
JOIN DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.bridge_book_author bba ON da.author_key = bba.author_key
JOIN DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_book db ON bba.book_key = db.book_key
JOIN DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.fact_book_ratings f ON db.book_key = f.book_key
LEFT JOIN DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_publication_year dy ON f.year_key = dy.year_key
GROUP BY da.author_key, da.author_name;

-- Recreate gold_language_summary
CREATE OR REPLACE TABLE gold_language_summary AS
SELECT
    dl.language_key,
    dl.language_code,
    dl.language_name,
    COUNT(DISTINCT f.book_key) AS total_books,
    ROUND(AVG(f.average_rating), 2) AS avg_rating,
    ROUND(MIN(f.average_rating), 2) AS min_rating,
    ROUND(MAX(f.average_rating), 2) AS max_rating,
    ROUND(STDDEV(f.average_rating), 2) AS rating_stddev,
    ROUND(AVG(f.average_rating) - 
          (SELECT AVG(average_rating) FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.fact_book_ratings), 2) AS rating_vs_overall_avg
FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_language dl
JOIN DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.fact_book_ratings f ON dl.language_key = f.language_key
GROUP BY dl.language_key, dl.language_code, dl.language_name;

-- Recreate gold_publication_trends
CREATE OR REPLACE TABLE gold_publication_trends AS
SELECT
    dy.year_key,
    dy.publication_year,
    dy.decade,
    dy.century,
    COUNT(DISTINCT f.book_key) AS total_books,
    ROUND(AVG(f.average_rating), 2) AS avg_rating,
    ROUND(MIN(f.average_rating), 2) AS min_rating,
    ROUND(MAX(f.average_rating), 2) AS max_rating,
    ROUND(STDDEV(f.average_rating), 2) AS rating_stddev
FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_publication_year dy
JOIN DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.fact_book_ratings f ON dy.year_key = f.year_key
GROUP BY dy.year_key, dy.publication_year, dy.decade, dy.century;

-- Recreate gold_decade_summary
CREATE OR REPLACE TABLE gold_decade_summary AS
SELECT
    decade,
    century,
    SUM(total_books) AS total_books,
    ROUND(AVG(avg_rating), 2) AS avg_rating,
    MIN(min_rating) AS min_rating,
    MAX(max_rating) AS max_rating
FROM gold_publication_trends
GROUP BY decade, century
ORDER BY MIN(publication_year);


-- record counts afterwards 
SELECT 'AFTER LOAD' AS status, 'Bronze_THE_DATA_TEAM.raw_books' AS table_name, COUNT(*) AS row_count FROM DB_TEAM_THEDATATEAM.Bronze_THE_DATA_TEAM.raw_books
UNION ALL SELECT 'AFTER LOAD', 'Silver_THE_DATA_TEAM.dim_book', COUNT(*) FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_book
UNION ALL SELECT 'AFTER LOAD', 'Silver_THE_DATA_TEAM.dim_author', COUNT(*) FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.dim_author
UNION ALL SELECT 'AFTER LOAD', 'Silver_THE_DATA_TEAM.fact_book_ratings', COUNT(*) FROM DB_TEAM_THEDATATEAM.Silver_THE_DATA_TEAM.fact_book_ratings
UNION ALL SELECT 'AFTER LOAD', 'Gold_THE_DATA_TEAM.gold_author_performance', COUNT(*) FROM Gold_THE_DATA_TEAM.gold_author_performance
UNION ALL SELECT 'AFTER LOAD', 'Gold_THE_DATA_TEAM.gold_publication_trends', COUNT(*) FROM Gold_THE_DATA_TEAM.gold_publication_trends;


-- verify data is visible and synthetic data is loaded in

-- Check new authors in Gold layer
SELECT '--- New Authors in Gold ---' AS verification;
SELECT * FROM gold_author_performance 
WHERE author_name IN ('Sarah J. Mitchell', 'Marcus Chen', 'Isabella Romano', 'James Wright', 'Amanda Foster', 'Yuki Tanaka');

-- Check 2023/2024 publications now appear
SELECT '--- New Publication Years in Gold ---' AS verification;
SELECT * FROM gold_publication_trends 
WHERE publication_year IN (2023, 2024);

-- Check decade summary includes 2020s
SELECT '--- 2020s Decade in Gold ---' AS verification;
SELECT * FROM gold_decade_summary 
WHERE decade = '2020s';






-- AI SQL 
USE ROLE ROLE_Team_THEDATATEAM;
USE WAREHOUSE Animal_Task_WH;
USE DATABASE DB_TEAM_THEDATATEAM;
USE SCHEMA Bronze_THE_DATA_TEAM;

-- add new column for ouput
ALTER TABLE raw_books ADD COLUMN IF NOT EXISTS ai_sentiment FLOAT;
ALTER TABLE raw_books ADD COLUMN IF NOT EXISTS ai_summary STRING;

--Apply AI function - Sentiment
-- Analyze the sentiment of each book description (-1 = negative, 0 = neutral, 1 = positive)
UPDATE raw_books
SET ai_sentiment = SNOWFLAKE.CORTEX.SENTIMENT(description)
WHERE description IS NOT NULL;

-- Preview sentiment results
SELECT 
    title,
    LEFT(description, 100) AS description_preview,
    ai_sentiment
FROM raw_books
WHERE ai_sentiment IS NOT NULL
ORDER BY ai_sentiment DESC
LIMIT 10;

-- Apply AI Function - Summarize
-- Generate a short summary of each book description
UPDATE raw_books
SET ai_summary = SNOWFLAKE.CORTEX.SUMMARIZE(description)
WHERE description IS NOT NULL;

-- Preview summary results
SELECT 
    title,
    LEFT(description, 150) AS original_description,
    ai_summary
FROM raw_books
WHERE ai_summary IS NOT NULL
LIMIT 10;

-- Reuslt Verification
-- Check how many records were processed
SELECT 
    COUNT(*) AS total_rows,
    COUNT(ai_sentiment) AS rows_with_sentiment,
    COUNT(ai_summary) AS rows_with_summary
FROM raw_books;

-- View sample of complete AI-enhanced records
SELECT 
    book_id,
    title,
    LEFT(description, 100) AS description_preview,
    ai_sentiment,
    LEFT(ai_summary, 200) AS summary_preview
FROM raw_books
WHERE ai_sentiment IS NOT NULL 
  AND ai_summary IS NOT NULL
LIMIT 5;

-- Extra Insights
-- Most positive book descriptions
SELECT title, ai_sentiment, LEFT(description, 150) AS description_preview
FROM raw_books
WHERE ai_sentiment IS NOT NULL
ORDER BY ai_sentiment DESC
LIMIT 5;

-- Most negative book descriptions
SELECT title, ai_sentiment, LEFT(description, 150) AS description_preview
FROM raw_books
WHERE ai_sentiment IS NOT NULL
ORDER BY ai_sentiment ASC
LIMIT 5;

-- Average sentiment by language
SELECT 
    language_code,
    COUNT(*) AS book_count,
    ROUND(AVG(ai_sentiment), 3) AS avg_sentiment
FROM raw_books
WHERE ai_sentiment IS NOT NULL
GROUP BY language_code
ORDER BY avg_sentiment DESC;




-- Cortex Search section 

-- set context 
USE ROLE ROLE_Team_THEDATATEAM;
USE WAREHOUSE Animal_Task_WH;
USE DATABASE DB_TEAM_THEDATATEAM;
USE SCHEMA Bronze_THE_DATA_TEAM;

-- Search 1: Find books about magic and wizards
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'Search_3',
        '{"query": "magic wizards fantasy adventure", "columns": ["title", "authors", "description"], "limit": 5}'
    )
) AS search_results;

-- Search 2: Find books about love and romance
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'Search_3',
        '{"query": "love romance relationships", "columns": ["title", "authors", "description"], "limit": 5}'
    )
) AS search_results;

-- Search 3: Find books about war and history
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'Search_3',
        '{"query": "war history battles military", "columns": ["title", "authors", "description"], "limit": 5}'
    )
) AS search_results;



-- Cortex Analyst section 

-- Make stage for model and set context 
USE ROLE ROLE_Team_THEDATATEAM;
USE WAREHOUSE Animal_Task_WH;
USE DATABASE DB_TEAM_THEDATATEAM;
USE SCHEMA Silver_THE_DATA_TEAM;

-- Create a stage for the semantic model
CREATE OR REPLACE STAGE silver_stage_cortex_analyst
    DIRECTORY = (ENABLE = true)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');


-- Created semantic model via Snowflake UI:
-- Name: CORTEX_ANALYST_THE_DATA_TEAM
-- Location: SILVER_STAGE_CORTEX_ANALYST
-- Tables included:
--   - FACT_BOOK_RATINGS
--   - DIM_BOOK
--   - DIM_AUTHOR
--   - DIM_LANGUAGE
--   - DIM_PUBLICATION_YEAR
--   - BRIDGE_BOOK_AUTHOR

-- Sample business questions tested in Cortex Analyst Playground:

-- Question 1: What is the average book rating by language?
-- Question 2: Which decade has the highest rated books?
-- Question 3: How many books does each author have?

-- View the semantic model file
SELECT * FROM @SILVER_STAGE_CORTEX_ANALYST;


-- End of Project - Thank you for a great semester Professor Gadgil!
    -- The_Data_Team