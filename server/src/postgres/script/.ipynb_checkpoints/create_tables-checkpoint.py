import psycopg2 as pg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    conn = psycopg2.connect(database = 'postgres', user = 'postgres', password = 'poop1234')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create hackforla database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS hackforla")
    cur.execute("CREATE DATABASE hackforla WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to hackforla database
    conn = pg2.connect(database = 'hackforla', user = 'postgres', password = 'poop1234')
    cur = conn.cursor()    
    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
