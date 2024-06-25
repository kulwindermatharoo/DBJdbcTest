# Databricks SQL warehouse concurrency test

Test the concurrency of executing multiple statements using JDBC connections in Databricks SQL.
Focus on using USE SCHEMA <schema> and CURRENT_SCHEMA() statements.

## Data Preparation:

Use the employee.csv file located in src/main/resource/data directory.

## Databricks Workspace/Step-by-Step Instructions::

-Create a table using the employee.csv file.

-Upload the CSV File to Databricks:

-- Log in to your Databricks workspace.

-- Navigate to the Data tab.

-- Click on "Create Table" and upload the employee.csv file.

-- Upload the CSV file to a location in DBFS (Databricks File System)

-- Example path: /FileStore/tables/employee.csv

-- Create a database if not already existing

CREATE DATABASE IF NOT EXISTS test_db;

-- Use the database

USE test_db;

-- Create the employee table

CREATE TABLE IF NOT EXISTS employee (
emp_id INT,
emp_name STRING,
emp_role STRING,
emp_salary DOUBLE
)
USING CSV
OPTIONS (path '/FileStore/tables/employee.csv', header 'true', inferSchema 'true');
