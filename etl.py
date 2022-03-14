import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This loads the data from S3 to staging tables
    :param cur: the cursor that is open
    :param conn: the connection string
    :return: none
    """
    for query in copy_table_queries:
        print('Loading Staging Table')
        cur.execute(query)
        print('Completed Loading Staging Table')
        conn.commit()


def insert_tables(cur, conn):
    """
    This inserts the data from staging tables into the dimension tables and final the reporting table
    :param cur: cursor
    :param conn: connection string
    :return: None
    """
    for query in insert_table_queries:
        print('Inserting data into Reporting Table')
        cur.execute(query)
        print(' Done Inserting data')
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()