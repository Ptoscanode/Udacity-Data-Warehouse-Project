import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print('Query worked!')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('Query worked!')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to the cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to the cluster!\n')
    
    print('----Loading staging tables----\n')
    load_staging_tables(cur, conn)
    print('-------------------------------\n')
    
    print('----Inserting tables---\n')
    insert_tables(cur, conn)
    print('-------------------------------\n')
    
    print('Closing connection')
    conn.close()
    print('Connection closed. All data has been loaded!\n')
    print('################# End of ETL Pipeline ########################')


if __name__ == "__main__":
    main()