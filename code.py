# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
import logging
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

def log_progress(msg):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    time_stamp = datetime.now().strftime(timestamp_format)

    log_entry = f"{time_stamp} : {msg}\n"
    with open('code_log.txt','a') as log_file:
        log_file.write(log_entry)


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and savses it to a dataframe. The
    function returns the dataframe for further processing. '''

    log_progress('Starting data extraction')
    #Retreiving the Data from Website
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')

    #Initialising DataFrame with the required column names
    df = pd.DataFrame(columns = table_attribs)

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            if  '—' not in cols[2] and cols[0].find('a') != None:
                data_dict = {
                    "Country": cols[0].a.contents[0],
                    "GDP_USD_millions": cols[2].contents[0]
                }
                new_df = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df,new_df], ignore_index = True)
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',','')
    df['GDP_USD_millions'] = df['GDP_USD_millions'].astype(float)
    df['GDP_USD_millions'] = np.round(df['GDP_USD_millions']/1000,2)
    df = df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    log_progress("Tranformation Complete")
    return df
    

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    log_progress(f'Starting to load data to CSV: {csv_path}')
    df.to_csv(csv_path,index = False)
    log_progress('Data loading to CSV completed')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    log_progress(f'Starting to load data to database table: {table_name}')
    df.to_sql(table_name,sql_connection,if_exists='replace',index = False)
    log_progress('Data loading to database completed')


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df,csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement,sql_connection)

log_progress('Process Complete.')

sql_connection.close()



