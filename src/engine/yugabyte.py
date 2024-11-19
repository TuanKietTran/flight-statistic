# yugabyte.py - Define functions to interact with YugabyteDB
import psycopg2
from io import StringIO
from .config import get_config
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

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
    
def insert_batch_data(file, tablename='flight_data'):
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    columns = file.readline()[:-1]
    if tablename == "carrier_staging":
        columns = columns.replace("carrier", "carrier_code", 1)
    elif tablename == "airport_staging":
        columns = columns.replace("airport", "airport_code", 1)
    elif tablename == "ot_delay_staging":
        columns = columns.replace("carrier", "carrier_code", 1)
        columns = columns.replace("airport", "airport_code", 1)
        
        
    cursor.copy_expert(f"COPY {tablename} ({columns}) FROM STDIN DELIMITER ',' CSV QUOTE '\"'", file)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Table '{tablename}' created successfully.")
    
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
        output.write('"' + '","'.join(map(str, row_dict.values())) + '"\n')
    # Seek to the beginning of the file-like object to use it
    output.seek(0)
    return output

def create_star_schema():
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS carrier_dim (
        carrier_code VARCHAR PRIMARY KEY,
        carrier_name VARCHAR UNIQUE
    );

    CREATE TABLE IF NOT EXISTS airport_dim (
        airport_code CHAR(3) PRIMARY KEY,
        airport_name VARCHAR UNIQUE
    );

    CREATE TABLE IF NOT EXISTS time_dim (
        id INTEGER PRIMARY KEY,
        year INTEGER CHECK (year >= 1000 AND year <= 9999),
        month INTEGER CHECK (month >= 1 AND month <= 12)
    );

    CREATE TABLE IF NOT EXISTS ot_delay_fact (
        id SERIAL PRIMARY KEY,
        time_id INTEGER,
        carrier_code VARCHAR,
        airport_code CHAR(3),
        arr_flights REAL,
        arr_del15 REAL,
        carrier_ct REAL,
        weather_ct REAL,
        nas_ct REAL,
        security_ct REAL,
        late_aircraft_ct REAL,
        arr_cancelled REAL,
        arr_diverted REAL,
        arr_delay REAL,
        carrier_delay REAL,
        weather_delay REAL,
        nas_delay REAL,
        security_delay REAL,
        late_aircraft_delay REAL,
        
        CONSTRAINT fk_time_id FOREIGN KEY (time_id) REFERENCES time_dim (id),
        CONSTRAINT fk_carrier_code FOREIGN KEY (carrier_code) REFERENCES carrier_dim (carrier_code),
        CONSTRAINT fk_airport_code FOREIGN KEY (airport_code) REFERENCES airport_dim (airport_code)
    );
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Star schema created successfully.")

def create_staging_table():
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS carrier_staging AS TABLE carrier_dim WITH NO DATA;
    CREATE TABLE IF NOT EXISTS airport_staging AS TABLE airport_dim WITH NO DATA;
    CREATE TABLE IF NOT EXISTS time_staging AS TABLE time_dim WITH NO DATA;
    CREATE TABLE IF NOT EXISTS ot_delay_staging AS TABLE ot_delay_fact WITH NO DATA;
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Temp staging table created successfully.")

def drop_staging_table():
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute('''
    DROP TABLE carrier_staging;
    DROP TABLE airport_staging;
    DROP TABLE time_staging;
    DROP TABLE ot_delay_staging;
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Temp staging table created successfully.")
    
def extract_tables(raw_data_df: DataFrame):
    """Extract table raw_data to 3 dim tables and 1 fact table

    Args:
        raw_data_df (pyspark.sql.DataFrame): table raw_data

    Returns:
        Tuple: Tables after extract
    """
    # Extract data for 3 dim tables
    carrier_dim_df = raw_data_df.select("carrier", "carrier_name").distinct()
    airport_dim_df = raw_data_df.select("airport", "airport_name").distinct()
    time_dim_df = raw_data_df.select("year", "month").distinct()

    # Prepare data for loading to yugabyte
    carrier_dim_data = carrier_dim_df
    airport_dim_data = airport_dim_df
    time_dim_data = time_dim_df.withColumn("id", col("year") * 100 + col("month")).select("id", "year", "month")
    ot_delay_fact_data = raw_data_df.withColumn("time_id", col("year") * 100 + col("month")) \
                            .select("time_id","carrier","airport","arr_flights",
                                    "arr_del15","carrier_ct","weather_ct","nas_ct",
                                    "security_ct","late_aircraft_ct","arr_cancelled",
                                    "arr_diverted","arr_delay","carrier_delay","weather_delay",
                                    "nas_delay","security_delay","late_aircraft_delay")
    
    return carrier_dim_data, airport_dim_data, time_dim_data, ot_delay_fact_data

def upsert():
    conn = get_warehouse_connection()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO carrier_dim (carrier_code, carrier_name)
    SELECT carrier_code, carrier_name
    FROM carrier_staging
    ON CONFLICT (carrier_code) DO UPDATE 
    SET carrier_name = EXCLUDED.carrier_name;
    
    INSERT INTO airport_dim (airport_code, airport_name)
    SELECT airport_code, airport_name
    FROM airport_staging
    ON CONFLICT (airport_code) DO UPDATE 
    SET airport_name = EXCLUDED.airport_name;
    
    INSERT INTO time_dim (id, year, month)
    SELECT id, year, month
    FROM time_staging
    ON CONFLICT (id) DO UPDATE
    SET year = EXCLUDED.year,
        month = EXCLUDED.month;
    
    INSERT INTO ot_delay_fact (
        time_id, carrier_code, airport_code, arr_flights, arr_del15, carrier_ct, 
        weather_ct, nas_ct, security_ct, late_aircraft_ct, arr_cancelled, arr_diverted, 
        arr_delay, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
    )
    SELECT 
        time_id, carrier_code, airport_code, arr_flights, arr_del15, carrier_ct, 
        weather_ct, nas_ct, security_ct, late_aircraft_ct, arr_cancelled, arr_diverted, 
        arr_delay, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
    FROM ot_delay_staging
    ON CONFLICT (id) DO UPDATE
    SET 
        time_id = EXCLUDED.time_id,
        carrier_code = EXCLUDED.carrier_code,
        airport_code = EXCLUDED.airport_code,
        arr_flights = EXCLUDED.arr_flights,
        arr_del15 = EXCLUDED.arr_del15,
        carrier_ct = EXCLUDED.carrier_ct,
        weather_ct = EXCLUDED.weather_ct,
        nas_ct = EXCLUDED.nas_ct,
        security_ct = EXCLUDED.security_ct,
        late_aircraft_ct = EXCLUDED.late_aircraft_ct,
        arr_cancelled = EXCLUDED.arr_cancelled,
        arr_diverted = EXCLUDED.arr_diverted,
        arr_delay = EXCLUDED.arr_delay,
        carrier_delay = EXCLUDED.carrier_delay,
        weather_delay = EXCLUDED.weather_delay,
        nas_delay = EXCLUDED.nas_delay,
        security_delay = EXCLUDED.security_delay,
        late_aircraft_delay = EXCLUDED.late_aircraft_delay;
    ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Update and Insert successfully.")

