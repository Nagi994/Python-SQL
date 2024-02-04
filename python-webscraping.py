import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy as np


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', "a") as f:
        f.write(timestamp + ':' + message + '\n')



def extract():
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attribs = ["Name", "MC_USD_Billion"]

    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')

    # Accumulate rows in a list for later printing
    data_rows = []
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_rows.append([bank_name, float(market_cap)])

    # Create the DataFrame and print it in table format
    df = pd.DataFrame(data_rows, columns=table_attribs)
    return df                                       #print(df.to_string())  # Print the DataFrame as a visually appealing table

#extract()

def transform(df ):
   csv_path = '/home/project/exchange_rate.csv'
   exchange_rate = pd.read_csv(csv_path)
   exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']

   df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]          
   df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']] 
   df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
   return df


#df = extract()
#df = transform(df)  # Pass the extracted DataFrame to transform
#print (df)
            
def load_to_csv(df):
     output_path = '/home/project/tranformed_data.csv'
     df.to_csv(output_path)
def load_to_db(df):
    conn = sqlite3.connect('Banks.db')
    table_name = "Largest_banks"
    df.to_sql(table_name, conn,  index = False )



query1 = 'SELECT * FROM Largest_banks'
query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = 'SELECT Name from Largest_banks LIMIT 5'

def run_queries(query):
   
    conn = sqlite3.connect('Banks.db')
    cursor = conn.cursor()
    result1 = cursor.execute(query)
   
    print (result1)
    




def main():
    df = extract()
    df = transform(df)
    load_to_csv(df)
    load_to_db(df)  # Load data to database before running queries

    # Run queries
    run_queries(query3)