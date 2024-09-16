# Global GDP ETL Pipeline

This repository contains a Python-based ETL (Extract, Transform, Load) pipeline designed to process global GDP data. The project retrieves GDP information from a web archive of a Wikipedia page, transforms the data into billions of USD, and loads the results into both a CSV file and an SQLite database. It also includes a logging system to track each stage of the ETL process.

## Project Overview

The ETL pipeline processes GDP data for countries by:
- Extracting data from a Wikipedia page (archived for consistency).
- Converting GDP values from millions to billions of USD.
- Saving the processed data in CSV and SQLite database formats.
- Logging progress at each stage for easy debugging and tracking.

## Features

- **Automated Data Extraction**: Retrieves country names and GDP data.
- **Data Transformation**: Converts GDP values from millions to billions.
- **Data Storage**: Saves data to both CSV and SQLite formats.
- **Logging**: Tracks the pipeline’s progress, with timestamps for debugging.
- **SQL Query Execution**: Filters countries with a GDP of 100 billion USD or more.

## Technologies Used

- **Python**: Core programming language.
- **BeautifulSoup**: HTML parsing to extract GDP data.
- **Pandas**: Data manipulation.
- **SQLite**: Local database to store transformed data.
- **NumPy**: Efficient numerical operations.
- **Logging**: Custom logging for pipeline tracking.

## ETL Process

### 1. Extract

The `extract()` function scrapes the GDP data from a Wikipedia page (archived for consistency). It extracts country names and their GDP values in millions of USD.

- **Source URL**: `https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29`
- **Process**: 
  - Requests the webpage content.
  - Parses the HTML using BeautifulSoup.
  - Selects the correct table and extracts the country names and GDP values.
- **DataFrame**: The extracted data is stored in a Pandas DataFrame with the following columns:
  - `Country`
  - `GDP_USD_millions`

### 2. Transform

The `transform()` function converts the GDP values from millions to billions by:
- Removing commas from the GDP values.
- Converting the data type to a float.
- Dividing the values by 1000 to represent billions.
- Renaming the column to `GDP_USD_billions`.

The transformation ensures that all GDP values are rounded to two decimal places for consistency.

### 3. Load

The pipeline provides two methods for loading the data:

1. **CSV**: The `load_to_csv()` function saves the transformed data into a CSV file, `Countries_by_GDP.csv`, which contains two columns: `Country` and `GDP_USD_billions`.

2. **SQLite Database**: The `load_to_db()` function saves the transformed data into an SQLite database table named `Countries_by_GDP`, making it accessible for running SQL queries. The SQLite database is saved as `World_Economies.db`.

### 4. Query

After loading the data into the SQLite database, the script runs a SQL query that filters countries with a GDP of 100 billion USD or more:

```sql
SELECT * FROM Countries_by_GDP WHERE GDP_USD_billions >= 100;
