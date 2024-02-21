# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

log_file = 'code_log.txt'
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
output_csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ': ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')  # Ensure you're using the provided table attributes
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:  # Ensure there are at least three columns in the row
            name = col[1].text.strip()  # Access the bank name from the second column
            billion_dollars = col[2].text.strip()  # Access the billion-dollar value from the third column
            data_dict = {"Name": name, "MC_USD_Billion": billion_dollars}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
# log_progress('Data transformation complete. Initiating Loading process')
# log_progress('Data saved to CSV file')
# log_progress('SQL Connection initiated')
# log_progress('Data loaded to Database as a table, Executing queries')
# log_progress('Process Complete')
# log_progress('Server Connection closed')
