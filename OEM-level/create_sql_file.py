import sqlite3
import csv


# Function to create table
def create_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS fact_oem_data_by_state_and_category (
                        year integer,
                        month integer,
                        day integer,
                        date varchar,
                        state varchar,
                        vehicle_class varchar,
                        vehicle_type varchar,
                        vehicle_category varchar,
                        maker varchar,
                        cng_only integer,
                        diesel integer,
                        diesel_hybrid integer,
                        di_methyl_ether integer,
                        dual_diesel_bio_cng integer,
                        dual_diesel_cng integer,
                        dual_diesel_lng integer,
                        electric_bov integer,
                        ethanol integer,
                        fuel_cell_hydrogen integer,
                        lng integer,
                        lpg_only integer,
                        methanol integer,
                        not_applicable integer,
                        petrol integer,
                        petrol_cng integer,
                        petrol_ethanol integer,
                        petrol_hybrid integer,
                        petrol_lpg integer,
                        petrol_methanol integer,
                        plug_in_hybrid_ev integer,
                        pure_ev integer,
                        solar integer,
                        strong_hybrid_ev integer,
                        total integer
                    )''')


# Function to insert data from CSV
def insert_data(cursor, csv_file):
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            cursor.execute('''INSERT INTO fact_oem_data_by_state_and_category (
                                year,
                                month,
                                day,
                                date,
                                state,
                                vehicle_class,
                                vehicle_type,
                                vehicle_category,
                                maker,
                                cng_only,
                                diesel,
                                diesel_hybrid,
                                di_methyl_ether,
                                dual_diesel_bio_cng,
                                dual_diesel_cng,
                                dual_diesel_lng,
                                electric_bov,
                                ethanol,
                                fuel_cell_hydrogen,
                                lng,
                                lpg_only,
                                methanol,
                                not_applicable,
                                petrol,
                                petrol_cng,
                                petrol_ethanol,
                                petrol_hybrid,
                                petrol_lpg,
                                petrol_methanol,
                                plug_in_hybrid_ev,
                                pure_ev,
                                solar,
                                strong_hybrid_ev,
                                total)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           row)


# Function to query and print sample rows
def query_sample_rows(cursor, limit=10):
    cursor.execute(f'SELECT * FROM fact_oem_data_by_state_and_category LIMIT {limit}')
    rows = cursor.fetchall()
    for row in rows:
        print(row)


# Connect to SQLite in-memory database
conn = sqlite3.connect('climate_dot_oem_data.db')
cursor = conn.cursor()

# Create table
create_table(cursor)

# Insert data from CSV
insert_data(cursor, 'oem_data_by_state_and_category_2024_to_2013.csv')

# Query and print sample rows
query_sample_rows(cursor, limit=10)

# Commit changes and close connection
conn.commit()
conn.close()
