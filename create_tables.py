import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    this method drops the tables that are in the Amazon Redshift using the queries defined in the sql_queries.py file
    :param cur: cursor for the  database
    :param conn: connection to the database
    :returns : none
    """
    for query in drop_table_queries:
        print('Running SQL DROP Statements')
        cur.execute(query)
        conn.commit()
        print('Tables Dropped')


def create_tables(cur, conn):
    """
    this method creates the tables needed in the Amazon Redshift
    :param cur: cursor for the  database
    :param conn: connection to the database
    :returns : none
    """
    for query in create_table_queries:
        print('Running SQL Create Statements')
        cur.execute(query)
        conn.commit()
        print('Tables Created')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()