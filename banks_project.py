"""
Code for ETL operations on Country-GDP data
"""
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests

# Initialised items
LOG_FILE = 'code_log.txt'
URL = ('https://web.archive.org/web/20230908091635/'
       'https://en.wikipedia.org/wiki/List_of_largest_banks')
TABLE_ATTRIBS = ["Name", "MC_USD_Billion"]
OUTPUT_CSV_PATH = './Largest_banks_data.csv'
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'

# Print the contents of the entire table
QUERY_1 = "SELECT * FROM Largest_banks"
# Print the average market capitalization of all the banks
QUERY_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
# in Billion USD.
QUERY_3 = "SELECT Name from Largest_banks LIMIT 5"  # Print only the names of the top 5 banks


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, 'a') as f:
        f.write(timestamp + ': ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name = col[1].text.strip()
            billion_dollars = col[2].text.strip()
            data_dict = {"Name": name, "MC_USD_Billion": float(billion_dollars)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    conversion_data = pd.read_csv(csv_path)
    exchange_rate = conversion_data.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    # print(df)
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')
dataframe = extract(URL, TABLE_ATTRIBS)
log_progress('Data extraction complete. Initiating Transformation process')
transform(dataframe, 'exchange_rate.csv')
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(dataframe, OUTPUT_CSV_PATH)
log_progress('Data saved to CSV file')
conn = sqlite3.connect(DB_NAME)
log_progress('SQL Connection initiated')
load_to_db(dataframe, conn, TABLE_NAME)
log_progress('Data loaded to Database as a table, Executing queries')
run_query(QUERY_1, conn)
run_query(QUERY_2, conn)
run_query(QUERY_3, conn)
log_progress('Process Complete')
conn.close()
log_progress('Server Connection closed')
