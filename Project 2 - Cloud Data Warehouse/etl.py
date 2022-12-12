import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data into staging tables from S3 bucket path for log and songs data with copy commands defined in sql_queries.py
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Done executing {query}")


def insert_tables(cur, conn):
    """
    Insert data from staging tables into analytics table with insert queries defined in sql_queries.py
    """
    for query in insert_table_queries:
        print(f"----Executing {query}")
        cur.execute(query)
        conn.commit()
        print(f"Done inserting")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Done connecting database")
    
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)
    

    conn.close()


if __name__ == "__main__":
    main()