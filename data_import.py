import os
import psycopg2
import subprocess


tables = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]

data_path = "/home/ubuntu/tpcds-data"  # directory of data files
target_path = "/home/ubuntu/format_data"  # directory of data files

if not os.path.exists(target_path):
    os.mkdir(target_path)

# read data and strip the last '|'
for table in tables:
    print(table)
    file_path = os.path.join(data_path, table + ".dat")
    target_file_path = os.path.join(target_path, table + ".dat")

    with open(file_path, "r", encoding='iso-8859-1') as fin:
        lines = fin.readlines()
        with open(target_file_path, "w") as fout:
            for line in lines:
                fout.write(line[:-2] + "\n")


# Connct to the database
class Database:
    def __init__(self, database_name):
        self.database_name = database_name
        self.conn = psycopg2.connect(
            host="localhost",
            database=database_name,
            user="postgres",
            password="123456",
        )
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


db = Database("tpcds")
# copy data to database from target_path
print("copy data to database from target_path")
for table in tables:
    print(table)
    file_path = os.path.join(target_path, table + ".dat")
    
    command = [
        "psql",
        "-U", 'postgres',
        "-d", 'tpcds',
        "-h", 'localhost',
        "-c", f"\\COPY {table} FROM '{file_path}' WITH DELIMITER '|' NULL '';"
    ]
    
    # 执行命令
    try:
        subprocess.run(command, check=True)
        print(f"Successfully imported {table}.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to import {table}. Error: {e}")
    
    