# yugabyte.py - Define functions to interact with YugabyteDB
import psycopg2
from config import get_config

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
        flight_number TEXT PRIMARY KEY,
        departure TEXT,
        arrival TEXT,
        status TEXT
    )''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'flight_data' created successfully.")


def insert_data(flight_number, departure, arrival, status):
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO flight_data (flight_number, departure, arrival, status) VALUES (%s, %s, %s, %s)",
        (flight_number, departure, arrival, status)
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data for flight '{flight_number}' inserted successfully.")