import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Drop tables included in sql_queries.py file
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Done {query}")


def create_tables(cur, conn):
    """
    - Creates tables with definitions included in sql_queries.py file
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f"Done {query}")


def main():
    print("Starting....")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Done reading config file")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Done connecting database")

    drop_tables(cur, conn)
    
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()