# yugabyte.py - Define functions to interact with YugabyteDB
import psycopg2
from io import StringIO
from .config import get_config

config = get_config()

# Establish a connection to YugabyteDB
def get_warehouse_connection():
    conn = psycopg2.connect(
        host=config['YUGABYTE_HOST'],
        port=config['YUGABYTE_PORT'],
        user=config['YUGABYTE_USER'],
        password=config['YUGABYTE_PASSWORD'],
        database="yugabyte"
    )
    return conn

def create_table():
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS flight_data (
        flight_number TEXT,
        year INT,
        month INT,
        day INT,
        dep_time TEXT,
        arr_time TEXT,
        origin TEXT,
        destination TEXT,
        air_time FLOAT,
        distance FLOAT,
        airline_name TEXT
    )''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'flight_data' created successfully.")


def insert_data(flight_number, year, month, day, dep_time, arr_time, origin, destination, air_time, distance, airline_name):
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO flight_data (flight_number, year, month, day, dep_time, arr_time, origin, destination, air_time, distance, airline_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (flight_number, year, month, day, dep_time, arr_time, origin, destination, air_time, distance, airline_name)
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data for flight '{flight_number}' inserted successfully.")
    
def insert_batch_data(file):
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.copy_expert("COPY flight_data FROM STDIN DELIMITER ',' CSV HEADER", file)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'flight_data' created successfully.")
    
def df2filestream(df):
    list_row = df.collect()
    # Convert Row objects to CSV format
    output = StringIO()
    header = list_row[0].asDict().keys()  # Get the column names from the first row (if the rows are consistent)
    # Write header
    output.write(','.join(header) + '\n')
    # Write each row as a CSV line
    for row in list_row:
        row_dict = {k: ("" if v is None else v) for k, v in row.asDict().items()}
        output.write(','.join(map(str, row_dict.values())) + '\n')
    # Seek to the beginning of the file-like object to use it
    output.seek(0)
    return output
