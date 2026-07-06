"""
query_drivers.py
----------------
Week 2 Assignment — Python + PostgreSQL

Your task: complete each function marked with TODO.
Run the script when done:  python query_drivers.py

Expected output:
    Driver                    Completed Rides
    ------------------------------------------
    Alice Johnson                          12
    Bob Smith                               9
    ...
    ------------------------------------------
    Total drivers:                         15
"""

import os
import psycopg2
from dotenv import load_dotenv


# The SQL query is provided — do not change it.
SQL = """
    SELECT
        d.name              AS driver_name,
        COUNT(t.trip_id)    AS completed_rides
    FROM drivers d
    LEFT JOIN trips t
        ON t.driver_id = d.driver_id
        AND t.status = 'completed'
    GROUP BY d.driver_id, d.name
    ORDER BY completed_rides DESC;
"""


# ─── TASK 1 ───────────────────────────────────────────────────────────────────
def load_config() -> dict:
    """
    Load database credentials from a .env file.

    Steps:
      1. Call load_dotenv() to read the .env file.
      2. Use os.getenv() to read each variable and store it in a dict.

    The .env file should contain:
        DB_HOST=...
        DB_PORT=...
        DB_NAME=...
        DB_USER=...
        DB_PASSWORD=...

    Returns:
        dict with keys: host, port, dbname, user, password
    """
    # TODO: call load_dotenv() here
    load_dotenv()

    # TODO:
    #  return a dict using os.getenv() for each key
    # Hint: { "host": os.getenv("DB_HOST"), ... }
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")     
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD") 
    return {"host": host, "port": port, "dbname": dbname, "user": user, "password": password}


# ─── TASK 2 ───────────────────────────────────────────────────────────────────
def get_connection(config: dict):
    return psycopg2.connect(**config)
    """
    # Open and return a psycopg2 database connection.

    Args:
        config: dict returned by load_config()

    Returns:
        psycopg2 connection object

    Hint: psycopg2.connect(host=..., port=..., dbname=..., user=..., password=...)
          Use ** to unpack the config dict directly.
    """
    # TODO: return psycopg2.connect(**config


# ─── TASK 3 ───────────────────────────────────────────────────────────────────
def fetch_drivers(conn) -> list:
    """
    Execute the SQL query and return all rows.

    Args:
        conn: open psycopg2 connection

    Returns:
        list of tuples — each tuple is (driver_name, completed_rides)

    Steps:
      1. Open a cursor with conn.cursor()
      2. Call cur.execute(SQL)
      3. Fetch all rows with cur.fetchall()
      4. Close the cursor and return the rows
    """
    cur=conn.cursor()
    cur.execute(SQL)
    rows=cur.fetchall()
    cur.close()
    return rows
    # TODO: implement this function
    pass


# ─── TASK 4 ───────────────────────────────────────────────────────────────────
def print_results(rows: list) -> None:
    """
    Print the query results as a formatted table.

    Args:
        rows: list of (driver_name, completed_rides) tuples

    Expected format (column widths: name = 25 left, rides = 15 right):

        Driver                    Completed Rides
        ------------------------------------------
        Alice Johnson                          12
        ------------------------------------------
        Total drivers:                         15

    Hints:
        - f"{value:<25}"  left-aligns in 25 chars
        - f"{value:>15}"  right-aligns in 15 chars
    """
    # TODO: print the header

    # header
    print(f"{'Driver':<25}{'Completed Rides':>15}")
    print("-" * 44)
 
    # rows
    for driver_name, completed_rides in rows:
        print(f"{driver_name:<25}{completed_rides:>15}")
 
    # footer
    print("-" * 44)
    print(f"{'Total drivers:':<25}{len(rows):>15}")
 


    # TODO: loop over rows and print each driver_name and completed_rides


    # TODO: print a footer with the total number of drivers


# ─────────────────────────────────────────────────────────────────────────────
def main():
    config = load_config()

    try:
        conn = get_connection(config)
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        return

    rows = fetch_drivers(conn)
    print_results(rows)

    
if __name__ == "__main__":
    main()
