import psycopg2


from sql_queries import create_table_queries,drop_table_queries

def create_database():
    """- Creates and connectes to the databaseDB
       - Retrun the connection and cursor to sparkifydb
    """
    # Connect to default database
    conn = psycopg2.connect('host = 127.0.0.1 dbname = spark user = postgres password = 1234')
    conn.set_session(autocommit = True)
    cur = conn.cursor()
    
    # Create DATABASE WITH UTF8 ENCODING
    
    cur.execute('DROP DATABASE IF EXISTS sparkifydb')
    cur.execute('CREATE DATABASE ')
    
    #Close the connection
    conn.close()
    
    # Connect to sparkifydb
    conn = psycopg2.connect('host = 127.0.0.1 dbname = sparkifydb user = postgres password = 1234')
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in 'drop_table_queries'
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Create each table using the queries in 'Create_table_queries'
 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
    
def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establiches connection with the sparkify database and gets cursor to its
    - drop all the tables.
    - Create all tables needed
    - Finally, closes the connection.
    """
    cur,conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur,conn)
    
    conn.close()
    

if __name__ == '__main__':
    main()
    