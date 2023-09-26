# This is a sample Python script.

from random import random

import psycopg2
import random
from datetime import date, timedelta

DATABASE_NAME = 'assignment1'
SALES_REGION_TABLE = 'sales_region'
LONDON_TABLE = 'london'
SYDNEY_TABLE = 'sydney'
BOSTON_TABLE = 'boston'
SALES_TABLE = 'sales'
SALES_2020_TABLE = 'sales_2020'
SALES_2021_TABLE = 'sales_2021'
SALES_2022_TABLE = 'sales_2022'
REGIONS = ["Boston", "Sydney", "London"]
PRODUCT_NAMES = ["Product_A", "Product_B", "Product_C", "Product_D", "Product_E"]

def create_database(dbname):
    """Create a new database named {DATABASE_NAME}"""
    try:
        conn = connect_postgres('postgres')
        if conn:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE {dbname};")
            print(f"Database {dbname} created successfully")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        print(f"Error: Unable to create database - {e}")

def connect_postgres(dbname):
    """Connect to PostgreSQL and return the connection"""
    try:
        conn = psycopg2.connect(user='postgres', dbname=dbname, host='localhost', password='postgres')
        print(f"Connection successful to {dbname}")
        return conn
    except Exception as e:
        print(f"Error: Unable to connect to PostgreSQL - {e}")
        return None

def list_partitioning(conn):
    """Function to create partitions of {SALES_REGION_TABLE} based on list of REGIONS.
       Create {SALES_REGION_TABLE} table and its list partition tables {LONDON_TABLE}, {SYDNEY_TABLE}, {BOSTON_TABLE}
       Commit the changes to the database"""
    try:
        # Create the sales_region table
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {SALES_REGION_TABLE} (id serial PRIMARY KEY, amount int, region text);")
        conn.commit()
        
        # Create list partitions for each region
        for region in REGIONS:
            cur.execute(f"CREATE TABLE {SALES_REGION_TABLE}_{region.lower()} PARTITION OF {SALES_REGION_TABLE} "
                        f"FOR VALUES IN ('{region}');")
            conn.commit()
        
        print("List partitioning for sales_region table created successfully.")
    
    except Exception as e:
        print(f"Error: Unable to create list partitions - {e}")

def insert_list_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
    try:
        cur = conn.cursor()

        # Generate and execute INSERT statements for 50 rows with random data
        for i in range(1, 51):
            amount = random.randint(100, 1000)
            region = random.choice(REGIONS)

            insert_query = f"""
            INSERT INTO {SALES_REGION_TABLE} (id, amount, region)
            VALUES ({i}, {amount}, '{region}');
            """
            cur.execute(insert_query)

        conn.commit()
        print("Inserted 50 rows of data into sales_region table.")
    except Exception as e:
        print(f"Error: Unable to insert data - {e}")


def select_list_data(conn):
    """Select data from {SALES_REGION_TABLE}, {BOSTON_TABLE}, {LONDON_TABLE}, {SYDNEY_TABLE} seperately.
       Print each tables' data.
       Commit the changes to the database
    """
    try:
        cur = conn.cursor()

        # Select data from sales_region table
        select_sales_region_query = f"SELECT * FROM {SALES_REGION_TABLE};"
        cur.execute(select_sales_region_query)
        sales_region_data = cur.fetchall()
        
        # Select data from Boston table
        select_boston_query = f"SELECT * FROM {BOSTON_TABLE};"
        cur.execute(select_boston_query)
        boston_data = cur.fetchall()
        
        # Select data from London table
        select_london_query = f"SELECT * FROM {LONDON_TABLE};"
        cur.execute(select_london_query)
        london_data = cur.fetchall()
        
        # Select data from Sydney table
        select_sydney_query = f"SELECT * FROM {SYDNEY_TABLE};"
        cur.execute(select_sydney_query)
        sydney_data = cur.fetchall()
        
        # Print the data from each table
        print("Data from sales_region table:")
        for row in sales_region_data:
            print(row)
        
        print("\nData from Boston table:")
        for row in boston_data:
            print(row)
        
        print("\nData from London table:")
        for row in london_data:
            print(row)
        
        print("\nData from Sydney table:")
        for row in sydney_data:
            print(row)

    except Exception as e:
        print(f"Error: Unable to select data - {e}")

def range_partitioning(conn):
    """Function to create partitions of {SALES_TABLE} based on range of sale_date.
       Create {SALES_REGION_TABLE} table and its range partition tables {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE}
       Commit the changes to the database
    """

def insert_range_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""


def select_range_data(conn):
    """Select data from {SALES_TABLE}, {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE} seperately.
           Print each tables' data.
           Commit the changes to the database
        """

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    create_database(DATABASE_NAME)
    conn = connect_postgres(dbname=DATABASE_NAME)
    if conn:

        list_partitioning(conn)
        insert_list_data(conn)
        select_list_data(conn)

        range_partitioning(conn)
        insert_range_data(conn)
        select_range_data(conn)
        conn.close()
        print('Done')