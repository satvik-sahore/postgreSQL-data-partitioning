# This is a sample Python script.

from random import random
from random import choice, randint

import psycopg2
import random
from datetime import datetime, timedelta

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
        cur.execute(f"CREATE TABLE {SALES_REGION_TABLE} (id serial not null, amount int, region text not null) "
            "PARTITION BY LIST (region);")
        conn.commit()
        cur.execute(f"CREATE TABLE {LONDON_TABLE} PARTITION OF {SALES_REGION_TABLE} FOR VALUES IN ('London');")
        conn.commit()
        cur.execute(f"CREATE TABLE {BOSTON_TABLE} PARTITION OF {SALES_REGION_TABLE} FOR VALUES IN ('Boston');")
        conn.commit()
        cur.execute(f"CREATE TABLE {SYDNEY_TABLE} PARTITION OF {SALES_REGION_TABLE} FOR VALUES IN ('Sydney');")
        conn.commit()
        """
        # Create list partitions for each region
        for region in REGIONS:
            cur.execute(f"CREATE TABLE {SALES_REGION_TABLE}_{region.lower()} PARTITION OF {SALES_REGION_TABLE} "
                        f"FOR VALUES IN ('{region}');")
            conn.commit()
        """
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
    try:
        # Create the sales table
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {SALES_TABLE} (id serial not null, product_name text, amount int, sale_date date) "
                    "PARTITION BY RANGE (sale_date);")
        conn.commit()

        # Create range partitions for each year
        for year in range(2020, 2023):
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            partition_name = f"{SALES_TABLE}_{year}"

            create_partition_query = f"""
            CREATE TABLE {partition_name} PARTITION OF {SALES_TABLE}
            FOR VALUES FROM ('{start_date}') TO ('{end_date}');
            """
            cur.execute(create_partition_query)
            conn.commit()

        print("Range partitioning for sales table created successfully.")

    except Exception as e:
        print(f"Error: Unable to create range partitions - {e}")

def insert_range_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
    try:
        cur = conn.cursor()

        # Generate and execute INSERT statements for 50 rows with random data
        for i in range(1, 51):
            product_name = choice(PRODUCT_NAMES)
            amount = randint(1, 100)
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2022, 12, 31)
            random_date = start_date + timedelta(days=randint(0, (end_date - start_date).days))
            sale_date = random_date.strftime('%Y-%m-%d')

            insert_query = f"""
            INSERT INTO {SALES_TABLE} (id, product_name, amount, sale_date)
            VALUES ({i}, '{product_name}', {amount}, '{sale_date}');
            """
            cur.execute(insert_query)

        conn.commit()
        print("Inserted 50 rows of data into the sales table.")
    except Exception as e:
        print(f"Error: Unable to insert data - {e}")


def select_range_data(conn):
    """Select data from {SALES_TABLE}, {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE} seperately.
           Print each tables' data.
           Commit the changes to the database
        """
    try:
        cur = conn.cursor()

        # Select data from the sales table
        select_sales_query = f"SELECT * FROM {SALES_TABLE};"
        cur.execute(select_sales_query)
        sales_data = cur.fetchall()

        # Select data from the sales_2020 table
        select_sales_2020_query = f"SELECT * FROM {SALES_2020_TABLE};"
        cur.execute(select_sales_2020_query)
        sales_2020_data = cur.fetchall()

        # Select data from the sales_2021 table
        select_sales_2021_query = f"SELECT * FROM {SALES_2021_TABLE};"
        cur.execute(select_sales_2021_query)
        sales_2021_data = cur.fetchall()

        # Select data from the sales_2022 table
        select_sales_2022_query = f"SELECT * FROM {SALES_2022_TABLE};"
        cur.execute(select_sales_2022_query)
        sales_2022_data = cur.fetchall()

        # Print the data from each table
        print("Sales Data:")
        for row in sales_data:
            print(row)

        print("\nSales 2020 Data:")
        for row in sales_2020_data:
            print(row)

        print("\nSales 2021 Data:")
        for row in sales_2021_data:
            print(row)

        print("\nSales 2022 Data:")
        for row in sales_2022_data:
            print(row)

    except Exception as e:
        print(f"Error: Unable to select data - {e}")

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