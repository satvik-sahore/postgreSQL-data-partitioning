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
    try:
        conn = connect_postgres('postgres')
        if conn:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE {dbname};")
            print(f"Database {dbname} created successfully")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        print(f"Error: Database creation failed - {e}")

def connect_postgres(dbname):
    try:
        conn = psycopg2.connect(user='postgres', dbname=dbname, host='localhost', password='postgres')
        print(f"Connection to {dbname} successful.")
        return conn
    except Exception as e:
        print(f"Error: Connection to PostgreSQL failed - {e}")
        return None

def list_partitioning(conn):
    try:
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
        print("List partitioning successful.")
    
    except Exception as e:
        print(f"Error: List partition failed - {e}")

def insert_list_data(conn):
    try:
        cur = conn.cursor()

        for i in range(1, 51):
            amt = random.randint(100, 1000)
            region = random.choice(REGIONS)

            insert_query = f"""
            INSERT INTO {SALES_REGION_TABLE} (id, amount, region)
            VALUES ({i}, {amt}, '{region}');
            """
            cur.execute(insert_query)

        conn.commit()
        print("Data insertion to sales_region table successful.")
    except Exception as e:
        print(f"Error: Data insertion to sales_region table failed - {e}")


def select_list_data(conn):
    try:
        cur = conn.cursor()

        select_sales_region = f"SELECT * FROM {SALES_REGION_TABLE};"
        cur.execute(select_sales_region)
        sales_region_data = cur.fetchall()
        
        select_boston = f"SELECT * FROM {BOSTON_TABLE};"
        cur.execute(select_boston)
        boston_data = cur.fetchall()
        
        select_london = f"SELECT * FROM {LONDON_TABLE};"
        cur.execute(select_london)
        london_data = cur.fetchall()
        
        select_sydney = f"SELECT * FROM {SYDNEY_TABLE};"
        cur.execute(select_sydney)
        sydney_data = cur.fetchall()
        
        print("\nSales Region Data:")
        for row in sales_region_data:
            print(row)
        
        print("\nBoston Data")
        for row in boston_data:
            print(row)
        
        print("\nLondon Data:")
        for row in london_data:
            print(row)
        
        print("\nSydney Data:")
        for row in sydney_data:
            print(row)

    except Exception as e:
        print(f"Error: Data fetch failed - {e}")

def range_partitioning(conn):
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {SALES_TABLE} (id serial not null, product_name text, amount int, sale_date date) "
                    "PARTITION BY RANGE (sale_date);")
        conn.commit()

        for year in range(2020, 2023):
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            partition_name = f"{SALES_TABLE}_{year}"

            create_partition = f"""
            CREATE TABLE {partition_name} PARTITION OF {SALES_TABLE}
            FOR VALUES FROM ('{start_date}') TO ('{end_date}');
            """
            cur.execute(create_partition)
            conn.commit()

        print("Range partitioning successful.")

    except Exception as e:
        print(f"Error: Range partitioning failed - {e}")

def insert_range_data(conn):
    try:
        cur = conn.cursor()

        for i in range(1, 51):
            product_name = choice(PRODUCT_NAMES)
            amt = randint(1, 100)
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2022, 12, 31)
            random_date = start_date + timedelta(days=randint(0, (end_date - start_date).days))
            sale_date = random_date.strftime('%Y-%m-%d')

            insert_query = f"""
            INSERT INTO {SALES_TABLE} (id, product_name, amount, sale_date)
            VALUES ({i}, '{product_name}', {amt}, '{sale_date}');
            """
            cur.execute(insert_query)

        conn.commit()
        print("Data insertion to sales table successful.")
    except Exception as e:
        print(f"Error: Data insertion to sales table failed - {e}")

def select_range_data(conn):
    try:
        cur = conn.cursor()

        select_sales = f"SELECT * FROM {SALES_TABLE};"
        cur.execute(select_sales)
        sales_rows = cur.fetchall()

        select_sales_2020 = f"SELECT * FROM {SALES_2020_TABLE};"
        cur.execute(select_sales_2020)
        sales_rows_2020 = cur.fetchall()

        select_sales_2021 = f"SELECT * FROM {SALES_2021_TABLE};"
        cur.execute(select_sales_2021)
        sales_rows_2021 = cur.fetchall()

        select_sales_2022 = f"SELECT * FROM {SALES_2022_TABLE};"
        cur.execute(select_sales_2022)
        sales_rows2022 = cur.fetchall()

        print("\nSales Data:")
        for row in sales_rows:
            print(row)

        print("\nSales 2020 Data:")
        for row in sales_rows_2020:
            print(row)

        print("\nSales 2021 Data:")
        for row in sales_rows_2021:
            print(row)

        print("\nSales 2022 Data:")
        for row in sales_rows2022:
            print(row)

    except Exception as e:
        print(f"Error: Data fetch failed - {e}")

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
        print('\nDone')