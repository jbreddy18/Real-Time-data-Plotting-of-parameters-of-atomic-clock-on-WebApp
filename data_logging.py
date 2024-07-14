import serial
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from astropy.time import Time
import csv
import os

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "Fluke_1620a"
DB_USER = "postgres"
DB_PASS = "12345678"
DB_PORT = 5432

# Serial port configuration
SERIAL_PORT = 'COM3'
BAUD_RATE = 9600
TIMEOUT = 60

# Function to establish connection to PostgreSQL database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connected to database!")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        return None

# Function to create table if not exists in PostgreSQL
def create_table_if_not_exists(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                mjd FLOAT,
                temperature_s1 NUMERIC,
                humidity_s1 NUMERIC,
                temperature_s2 NUMERIC,
                humidity_s2 NUMERIC
            );
        """)
        conn.commit()
        print("Table 'sensor_data' created or already exists.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        conn.rollback()

# Function to send command and read response from the Fluke 1620A
def send_command(cmd, ser):
    ser.write((cmd + '\r\n').encode())
    time.sleep(1)  # Allow some time for response
    response = ser.read_all().decode().strip()
    print(f"Command: {cmd}, Response: {response}")  # Debug statement
    return response

# Function to read data from the Fluke 1620A
def read_data(ser):
    read_command = "DATa:RECord:READ?"
    response = send_command(read_command, ser)
    return response

# Function to process and parse the data response
def process_data(data_response, conn, csv_writer, today_date):
    data_lines = data_response.splitlines()
    for line in data_lines:
        print(f"Processing line: {line}")  # Debug statement
        parts = line.split(",")

        if len(parts) >= 8:
            try:
                timestamp_str = parts[0].strip()
                try:
                    timestamp = datetime.strptime(timestamp_str, "%d/%m/%Y %H:%M:%S")
                except ValueError as e:
                    print(f"Error parsing timestamp {timestamp_str}: {e}")
                    continue

                date_str = timestamp.date()
                mjd = datetime_to_mjd(timestamp)

                temperature_s1 = float(parts[1].strip().rstrip('C'))
                humidity_s1 = float(parts[3].strip().rstrip('%'))
                temperature_s2 = float(parts[5].strip().rstrip('C'))
                humidity_s2 = float(parts[7].strip().rstrip('%'))
                
                # Write data to the PostgreSQL database
                write_to_database(conn, timestamp, mjd, temperature_s1, humidity_s1, temperature_s2, humidity_s2)
                
                # Append data to CSV file
                csv_writer.writerow([date_str, timestamp, mjd, temperature_s1, humidity_s1, temperature_s2, humidity_s2])
                print("Data appended to CSV file.")
            except ValueError as e:
                print(f"Error converting data to float: {e}")
                continue
        else:
            print(f"Unexpected format in line: {line}")

# Function to convert datetime to MJD
def datetime_to_mjd(dt):
    t = Time(dt, format='datetime')
    return t.mjd

# Function to write data to the PostgreSQL database
def write_to_database(conn, timestamp, mjd, temperature_s1, humidity_s1, temperature_s2, humidity_s2):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sensor_data (timestamp, mjd, temperature_s1, humidity_s1, temperature_s2, humidity_s2)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (timestamp, mjd, temperature_s1, humidity_s1, temperature_s2, humidity_s2))
        conn.commit()
        print("Data inserted into database.")
    except psycopg2.Error as e:
        print(f"Error inserting data into PostgreSQL: {e}")
        conn.rollback()

# Main function to execute the data retrieval, storage, and plotting process
def main():
    conn = connect_to_db()
    if not conn:
        return
    
    create_table_if_not_exists(conn)
    
    # Determine today's date for CSV file
    today_date = datetime.now().date()
    csv_file_path = f'sensor_dataset_{today_date}.csv'
    
    # Check if CSV file exists and create if necessary
    if not os.path.exists(csv_file_path):
        with open(csv_file_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['date', 'timestamp', 'mjd', 'temperature_s1', 'humidity_s1', 'temperature_s2', 'humidity_s2'])
    
    # Open CSV file in append mode
    with open(csv_file_path, 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        try:
            with serial.Serial(port=SERIAL_PORT, baudrate=BAUD_RATE, timeout=TIMEOUT) as ser:
                print("Serial port open.")
                
                while True:
                    # Check if the date has changed
                    current_date = datetime.now().date()
                    if current_date != today_date:
                        csvfile.close()  # Close the current file
                        today_date = current_date
                        csv_file_path = f'sensor_dataset_{today_date}.csv'
                        if not os.path.exists(csv_file_path):
                            with open(csv_file_path, 'w', newline='') as new_csvfile:
                                csv_writer = csv.writer(new_csvfile)
                                csv_writer.writerow(['date', 'timestamp', 'mjd', 'temperature_s1', 'humidity_s1', 'temperature_s2', 'humidity_s2'])
                        csvfile = open(csv_file_path, 'a', newline='')
                        csv_writer = csv.writer(csvfile)

                    # Read data from the Fluke 1620A
                    data_response = read_data(ser)
                    print(f"Data response: {data_response}")

                    # Process and parse the data response
                    process_data(data_response, conn, csv_writer, today_date)

                    time.sleep(10)  # Wait for 10 seconds before the next data collection

        except serial.SerialException as e:
            print(f"Serial communication error: {e}")
        except PermissionError as e:
            print(f"Permission error: {e}")
        finally:
            if csvfile:
                csvfile.close()  # Close the file if it's still open
            if conn:
                conn.close()
                print("Database connection closed.")

# Execute the main function
if __name__ == "__main__":
    main()
