import os
import glob
import psycopg2 as pg2
import pandas as pd
from sql_queries import *
import datetime

def process_clientinfo(filepath):
    """Inserts records into the database from the tsv"""
    df = pd.read_csv('clientinfo.txt', sep='\t', dtype = object)
    df.columns = ['clientid', 'firstname', 'lastname', 'email', 'birthdate', 'gender', 'ethnicity']
    for value in range(df.values):
        print(df.values[0])
    return

    try:
        cur.execute(clientinfo_table_insert, clientinfo)
    except:
        print('Error inserting data for clientinfo.')
        pass

def porcess_clientbackground():
    try:
        cur.execute(clientbackground_table_insert, clientbackground)
    except:
        print('Error inserting data for .')
        pass

def process_clientpreference():

    try:
        cur.execute(clientpreference_table_insert, clientpreference)
    except:
        print('Error inserting data for clientinfo.')
        pass

def main():
    #conn = psycopg2.connect("host=127.0.0.1 dbname=hackforla user=postgres password=password") #connect to the hackforla database
    #cur = conn.cursor() 

    filepath = 'test'#'c:\user\ladbscid64\Project\postgres'
    process_clientinfo(filepath)
    #process_clientbackground(cur, filepath)
    #process_clientpreference(cur, filepath)

    #connection closed
    #conn.close()


if __name__ == "__main__":
    main()
