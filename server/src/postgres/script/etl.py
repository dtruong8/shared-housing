import os
import glob
import psycopg2 as pg2
import pandas as pd
from sql_queries import *


def process_clientinfo(cur, conn):
    """Inserts records into the database from the tsv"""
    clientinfo_df = pd.read_csv(f'shared-housing\\server\\src\\postgres\\tsv files\\CLIENTINFO.txt', sep='\t', dtype = object)
    clientinfo_data = clientinfo_df[clientinfo_df.columns]
    for i, row in clientinfo_data.iterrows():
        cur.execute(clientinfo_table_insert, row)
        conn.commit()

"""def porcess_clientbackground():
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

"""
def main():
    #connect to the hackforla database
    conn = pg2.connect() 
    cur = conn.cursor() 

    process_clientinfo(cur, conn)
    #connection closed
    conn.close()


"""process_clientbackground(cur)
    process_clientpreference(cur)
"""
    

if __name__ == "__main__":
    main()
