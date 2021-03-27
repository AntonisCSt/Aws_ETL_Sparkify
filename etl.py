import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,analytical_queries,transform_queries


def load_staging_tables(cur, conn):
    """This function is responsible for loading the staging tables by running the queries: copy_table_queries in sql_queries.py
    
    Arguments:
        cur: DB cursor connection
        filepath: path to files 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """This function is responsible for insterting the tables contents to the starch schema database by the staging tables
    
    Arguments:
        cur: DB cursor connection
        filepath: path to files 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def anal_queries(cur, conn):
    """This function is responsible for running analytical queries (that we desire) from the star schema db
    
    Arguments:
        cur: DB cursor connection
        filepath: path to files 
    """
    for query in analytical_queries:
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()

def trans_queries(cur, conn):
    """This function is responsible for transforming the time value from one of the stating tables columns to a timestamp
    
    Arguments:
        cur: DB cursor connection
        filepath: path to files 
    """
    for query in transform_queries:
        cur.execute(query)
        conn.commit()        
        
        
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('loading staging tables....')
    #load tables from S3 buckets to redshift staging env
    load_staging_tables(cur, conn)
    
    print('I am going to transform')
    #Transform column ts to timestamp type before creating the star schema table
    trans_queries(cur, conn)
    print('I am going to insert the tables to a star schema db')
    #Insert the tables from staging to star schema tables
    insert_tables(cur, conn)

    print('I am running analytical queries :)')
    #Run analytical queries
    anal_queries(cur, conn)
    
    print('Finished! gonna close the database connection, BYEEEE :)')
    conn.close()


if __name__ == "__main__":
    main()