from datetime import datetime, timedelta
from sqlalchemy import create_engine

import csv
import logging
import pandas as pd
import pyodbc
import urllib.parse
import yaml

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OEMDataIngest:
    def __init__(self):
        with open("config.yaml", "r") as file:
            config = yaml.safe_load(file)
        self.database_host = config["database"]["server"]
        self.database_name = config["database"]["database"]
        self.database_username = config["database"]["username"]
        self.database_password = config["database"]["password"]
        self.encoded_db_password = urllib.parse.quote_plus(self.database_password)

        self.staging_table_name = "staging_fact_oem_data_by_state_and_category"
        self.final_table_name = "fact_oem_data_by_state_and_category"
        self.driver = "{ODBC Driver 18 for SQL Server}"

    @staticmethod
    def get_year_month_label():
        """
        Get month and year label for OEM scraper.
        :return:
        """
        # Get the current date
        current_date = datetime.now()
        # Calculate the first day of the current month
        first_day_of_current_month = current_date.replace(day=1)
        # Calculate the last day of the previous month
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        # Get the month abbreviation and year for the last day of the previous month
        month_abbreviation = last_day_of_previous_month.strftime("%b")
        year = last_day_of_previous_month.strftime("%Y")
        return month_abbreviation.upper(), year

    def data_ingest(self):
        month, year = self.get_year_month_label()
        file_path = f"oem_data_by_state_and_category_{month}_{year}.csv"

        # Read CSV file
        with open(file_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)  # Read the header row

            # Connect to the database
            logging.info("Connecting to the database...")
            try:
                conn = pyodbc.connect(
                    driver=self.driver,
                    server=self.database_host,
                    database=self.database_name,
                    uid=self.database_username,
                    pwd=self.database_password,
                )
                cursor = conn.cursor()
                logging.info("Successfully connected to the database.")
            except Exception as e:
                logging.error(f"Error connecting to the database: {str(e)}")
                raise

            # Truncate staging table
            logging.info(f"Truncating staging table: {self.staging_table_name}")
            truncate_query = f"TRUNCATE TABLE {self.staging_table_name}"
            try:
                cursor.execute(truncate_query)
                logging.info(
                    f"Successfully truncated staging table: {self.staging_table_name}"
                )
            except Exception as e:
                logging.error(f"Error truncating staging table: {str(e)}")
                raise

            # Insert data into staging table
            columns = ", ".join(headers)
            placeholders = ", ".join(["?" for _ in headers])
            insert_query = f"INSERT INTO {self.staging_table_name} ({columns}, insert_date) VALUES ({placeholders}, GETDATE())"

            logging.info(
                f"Inserting data into staging table: {self.staging_table_name}"
            )
            try:
                # Use cursor.executemany to insert data from csv_reader
                cursor.executemany(insert_query, csv_reader)
                conn.commit()
                logging.info(
                    f"Successfully inserted data into staging table: {self.staging_table_name}"
                )
            except Exception as e:
                logging.error(f"Error inserting data into staging table: {str(e)}")
                raise

            # Transfer data from staging to final table
            transfer_query = f"""
            INSERT INTO {self.final_table_name}
            SELECT * FROM {self.staging_table_name}
            """
            logging.info(
                f"Transferring data from staging table to final table: {self.final_table_name}"
            )
            try:
                cursor.execute(transfer_query)
                logging.info(
                    f"Successfully transferred data to final table: {self.final_table_name}"
                )
            except Exception as e:
                logging.error(f"Error transferring data to final table: {str(e)}")
                raise

            # Commit the transaction and close the connection
            conn.commit()
            cursor.close()
            conn.close()


def main():
    oem_data_ingest = OEMDataIngest()
    oem_data_ingest.data_ingest()


if __name__ == "__main__":
    main()
