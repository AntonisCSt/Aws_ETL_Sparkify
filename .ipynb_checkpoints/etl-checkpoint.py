import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,analytical_queries,transform_queries


def load_staging_tables(cur, conn):
    """This function is responsible
    
    Arguments:
        cur: DB cursor connection
        filepath: path to files
    Return:   
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def anal_queries(cur, conn):
    for query in analytical_queries:
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
        while row:
            print(row)
            row = cur.fetchone()

def trans_queries(cur, conn):
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
    print('I am going to tnsert the tables to a star schema')
    #Insert the tables from staging to star schema tables
    insert_tables(cur, conn)

    print('I am running analytical queries :)')
    #Run analytical queries
    anal_queries(cur, conn)
    
    
    conn.close()


if __name__ == "__main__":
    main()