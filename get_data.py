# Author : Ria Thomas
# Description : The script is to get the British Columbia Covid cases data for the Tableau Dashboard. Also to clean and transform it as required.
# Source : https://resources-covid19canada.hub.arcgis.com/pages/open-data

import pandas as pd
import requests
import json
import datetime
import sqlalchemy as db
import keyring
import pymysql
import urllib.request

def load_table():

    engine_dest = db.create_engine('mysql+pymysql://root:'+keyring.get_password('mysql_db', 'root')+'@localhost/db_dash?host=localhost?port=3306')
    table_name = 'bc_covid_data'
    
    print('Getting data')
    source_file_name = 'http://www.bccdc.ca/Health-Info-Site/Documents/BCCDC_COVID19_Dashboard_Case_Details.csv'
    urllib.request.urlretrieve(source_file_name, "./data/BCCDC_COVID19_Dashboard_Case_Details.csv")
    df = pd.read_csv('./data/BCCDC_COVID19_Dashboard_Case_Details.csv')

    print('Got the data! Now cleaning..')
    df['Reported_Date'] = pd.to_datetime(df['Reported_Date']).dt.strftime('%Y/%m/%d')
    df['ObjectId'] = range(1, len(df.index)+1)
    df.drop('Classification_Reported', axis=1, inplace=True)

    print('All done! Getting ready to load it to the table')
    conn = engine_dest.connect()
    conn.execute("TRUNCATE TABLE " + table_name)
    df.to_sql(table_name, con=engine_dest, if_exists='append', index = False)
    print('Done loading! Ready to be used in the reports.')

def main():
    load_table()

if __name__ == "__main__":
    main()
