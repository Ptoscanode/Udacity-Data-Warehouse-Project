import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    print('Reading credentials')
    config.read('dwh.cfg')
    
    print('Connecting to the cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected!')

    print('Dropping tables')
    drop_tables(cur, conn)
    print('Creating tables')
    create_tables(cur, conn)
    print('Closing connection')
    conn.close()
    print('Connection closed. Tables created!')


if __name__ == "__main__":
    main()