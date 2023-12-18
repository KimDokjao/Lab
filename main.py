import time
from config import host, user, password, dbname,file,command
import psycopg2
import sqlite3
from sqlalchemy import create_engine
import duckdb
import pandas as pd
import openpyxl
from openpyxl.chart import LineChart, Reference
import os
import statistics

def import_file_to_postgres():
    engine=create_engine("postgresql://postgres:"+password+"@localhost:5432/postgres")
    #tiny= 'nyc_yellow_tiny.csv'
    df=pd.read_csv(file)
    df.to_sql('trips', engine, if_exists='replace', index=False)
    print(df)
def median(a):
    n = len(a)
    index = n // 2
    if n % 2:
        return sorted(a)[index]
    return sum(sorted(a)[index - 1:index + 1]) / 2

def postgress():
    post1 = []
    post2 = []
    post3 = []
    post4 = []
    try:
        conn= psycopg2.connect(
            user=user,
            password=password,
            host=host,
            dbname= dbname,
            port="5432"
        )
        cur = conn.cursor()

        for i in range(10):
            start_time = time.time()
            cur.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;')
            end_time = time.time()
            execution_time = end_time - start_time
            post1.append(execution_time)
            start_time = time.time()
            cur.execute("SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;")
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post2.append(execution_time)
            start_time = time.time()
            cur.execute("SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;")
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post3.append(execution_time)
            start_time = time.time()
            cur.execute( "SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;")
            cur.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            post4.append(execution_time)

        cur.close()
        conn.close()

        psycopg2_time = []
        psycopg2_time.append(median(post1))
        psycopg2_time.append(median(post2))
        psycopg2_time.append(median(post3))
        psycopg2_time.append(median(post4))


        print("Execution time for SQL 1 in PostgreSQL: ", psycopg2_time[0], " seconds ")
        print("Execution time for SQL 2 in PostgreSQL: ", psycopg2_time[1], " seconds ")
        print("Execution time for SQL 3 in PostgreSQL: ", psycopg2_time[2], " seconds ")
        print("Execution time for SQL 4 in PostgreSQL: ", psycopg2_time[3], " seconds ")

    except psycopg2.Error as error:
        print("Error: ", error)

lite1 = []
lite2 = []
lite3 = []
lite4 = []

def sqlite():
    engine = create_engine("sqlite:///database.db")
        # tiny = 'nyc_yellow_tiny.csv'
    df = pd.read_csv(file)
    df.to_sql('trips', engine, if_exists='replace', index=False)
    print(df)
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()

    for i in range(10):
        start_time = time.time()
        cur.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;')
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite1.append(execution_time)
        start_time = time.time()
        cur.execute("SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;")
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite2.append(execution_time)
        start_time = time.time()
        cur.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;")
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite3.append(execution_time)
        start_time = time.time()
        cur.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;")
        cur.fetchall()
        end_time = time.time()
        execution_time = end_time - start_time
        lite4.append(execution_time)

    cur.close()
    conn.close()

    sqllite_time = []
    sqllite_time.append(median(lite1))
    sqllite_time.append(median(lite2))
    sqllite_time.append(median(lite3))
    sqllite_time.append(median(lite4))

    print("Execution time for SQL 1 in SQLite: ", sqllite_time[0] ," seconds ")
    print("Execution time for SQL 2 in SQLite: ", sqllite_time[1], " seconds ")
    print("Execution time for SQL 3 in SQLite: ", sqllite_time[2], " seconds ")
    print("Execution time for SQL 4 in SQLite: ", sqllite_time[3], " seconds ")


pan1 = []
pan2 = []
pan3 = []
pan4 = []

def median(a):
    n = len(a)
    index = n // 2
    if n % 2:
        return sorted(a)[index]
    return sum(sorted(a)[index - 1:index + 1]) / 2



def pandas():
    trips = pd.read_csv("nyc_yellow_tiny.csv")
    trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])


    for i in range(10):
        start_time = time.time()
        result1 = trips.groupby("VendorID").size()
        end_time = time.time()
        execution_time = end_time - start_time
        pan1.append(execution_time)
        start_time = time.time()
        result2 = trips.groupby('passenger_count')['total_amount'].mean()
        end_time = time.time()
        execution_time = end_time - start_time
        pan2.append(execution_time)
        start_time = time.time()
        result3 = trips.groupby(['passenger_count', trips['tpep_pickup_datetime'].dt.year]).size()
        end_time = time.time()
        execution_time = end_time - start_time
        pan3.append(execution_time)
        result4 = trips.groupby(['passenger_count', trips['tpep_pickup_datetime'].dt.year, trips['trip_distance'].round()]).size().reset_index(name='count').sort_values(['tpep_pickup_datetime', 'count'], ascending=[True, False])
        end_time = time.time()
        execution_time = end_time - start_time
        pan4.append(execution_time)
        start_time = time.time()

    pandas_time = []
    pandas_time.append(median(pan1))
    pandas_time.append(median(pan2))
    pandas_time.append(median(pan3))
    pandas_time.append(median(pan4))

    print("Execution time for SQL 1 in Pandas:",     pandas_time[0], " seconds ")
    print("Execution time for SQL 2 in Pandas: ",    pandas_time[1], " seconds ")
    print("Execution time for SQL 3 in Pandas: ",     pandas_time[2], " seconds ")
    print("Execution time for SQL 4 in Pandas: ",     pandas_time[3], " seconds ")



def duck_db():

    conn = duckdb.connect(database=':memory:', read_only=False)
    df = pd.read_csv(file)
    conn.register('trips', df)

    queries = [
        "SELECT VendorID, count(*) FROM trips GROUP BY 1;",
        "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;",
        "SELECT passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4), count(*) FROM trips GROUP BY passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4);",
        "SELECT passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4), ROUND(trip_distance), count(*) FROM trips GROUP BY passenger_count, SUBSTRING(tpep_pickup_datetime, 1, 4), ROUND(trip_distance) ORDER BY SUBSTRING(tpep_pickup_datetime, 1, 4), count(*) DESC;"
    ]

    def measure_query_time(query, conn):
        times = []
        for _ in range(10):
            start_time = time.time()
            result = conn.execute(query)
            end_time = time.time()
            times.append(end_time - start_time)
        return median(times)

    i=1
    duckdb_time=[]
    for query in queries:
        query_time = measure_query_time(query, conn)
        duckdb_time.append(query_time)
        print(f"Execution time for SQL {i} in DuckDB: {query_time} seconds ")
        i=i+1

    conn.close()

if command=="import":
    import_file_to_postgres()
elif command=="psycopg2":
    postgress()
elif command=="sqlite":
    sqlite()
elif command=="pandas":
    pandas()
elif command=="duckdb":
    duck_db()
else:
    print("проверьте правильность введенной команды")