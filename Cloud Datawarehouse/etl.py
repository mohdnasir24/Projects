import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load staging_events,staging_songs table from s3 json files based on
    path mentioned in dwh.cfg

            Parameters:
                    copy_table_queries (str): queries with copy command in sql_queries.py
                    cur (str): connection cursor
                    conn (str): connection 
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserting data into fact and dimension tables from  staging_events,staging_songs staging tables

            Parameters:
                    insert_table_queries (str): insert queries in sql_queries.py
                    cur (str): connection cursor
                    conn (str): connection 
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main function to call load_staging_tables and insert_tables function
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()