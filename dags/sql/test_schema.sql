-- -- create pet table
-- CREATE TABLE IF NOT EXISTS pet (
--     constant INT
-- );

CREATE TABLE IF NOT EXISTS {{ params.tablename }} (
    id VARCHAR(20) PRIMARY KEY,
    username VARCHAR(20),
    name VARCHAR(15),
    portfolio_url VARCHAR(30),
    image_url VARCHAR(200),
    total_likes INT,
    total_photos INT
);

-- cp dags/sql/test_schema.sql ~/airflow/dags/sql