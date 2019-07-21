import os
import glob
import psycopg2 as pg2
import pandas as pd
from sql_queries import *


def process_clientinfo(cur, filepath):
    """Inserts records into the clientinfo table"""
    clientinfo_df = pd.read_csv(filepath, sep='\t', dtype=object)
    
    # replace all missing dates with "01/01/0001" so it can be entered into postgresql
    clientinfo_df['birthdate'] = clientinfo_df['birthdate'].fillna('01/01/0001')
    for i, row in clientinfo_df.iterrows():
        cur.execute(clientinfo_table_insert, row)


def porcess_clientbackground(cur, filepath):
    """Inserts records into the clientbackground table"""
    clientbackground_df = pd.read_csv(filepath, sep='\t', dtype=object)
    
    # replace nan with 0 so it can be entered into posgresql
    clientbackground_df['num_children'] = clientbackground_df['num_children'].fillna('0')
    for i, row in clientbackground_df.iterrows():
        cur.execute(clientbackground_table_insert, row)

def process_clientpreference(cur, filepath):
    """Inserts records into the clientbackpreference table"""
    clientpreference_df = pd.read_csv(filepath, sep='\t', dtype=object)
    
    # replace nan with 0 so it can be entered into postgresql
    clientpreference_df[['min_age', 'max_age']] = clientpreference_df[['min_age', 'max_age']].fillna('0')
    for i, row in clientpreference_df.iterrows():
        cur.execute(clientpreference_table_insert, row)
    
def get_files(filepath, filename):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, filename))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files
    
def process_data(cur, conn, filepath, filename, func):
    """Iterate through all data files"""
    #get all files
    all_files = get_files(filepath, filename)
    
    #total number of file found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate
    for i, datafile in enumerate(all_files, 1):
          func(cur, datafile)
          conn.commit()
          print('{}/{} files processed'.format(i, num_files))

def main():
    # connect to the hackforla database
    conn = pg2.connect() 
    cur = conn.cursor()
    
    # process files
    process_data(cur, conn, filepath='..\\', filename='CLIENTINFO.txt',  func=process_clientinfo)
    process_data(cur, conn, filepath='..\\', filename='CLIENTBACKGROUND.txt',  func=porcess_clientbackground)
    process_data(cur, conn, filepath='..\\', filename='roommatepreference.txt',  func=process_clientpreference)
    
    # connection closed
    conn.close()
   

if __name__ == "__main__":
    main()
