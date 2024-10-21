# yugabyte.py - Define functions to interact with YugabyteDB
import psycopg2
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