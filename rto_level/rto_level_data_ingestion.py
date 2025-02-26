from datetime import datetime, timedelta

import csv
import logging
import os
import pyodbc
import sys
import time
import urllib.parse
import yaml

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RtoDataIngest:
    def __init__(self):
        with open("config.yaml", "r") as file:
            config = yaml.safe_load(file)
        self.database_host = config["database"]["server"]
        self.database_name = config["database"]["database"]
        self.database_username = config["database"]["username"]
        self.database_password = config["database"]["password"]
        self.encoded_db_password = urllib.parse.quote_plus(self.database_password)
        self.staging_table_name = "staging_fact_ev_data_by_rto"
        self.final_table_name = "fact_ev_data_by_rto"
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

    def data_ingest(self, month, year):
        file_path = f"rto_level_ev_data_{month}_{year}.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"the file {file_path} does not exist. did you run data_pre_processing yet?"
            )

        # Read CSV file
        data = []
        with open(file_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)  # Read the header row

            # Dynamically create tuples for each row
            for row in csv_reader:
                # Convert to tuple and add to data list
                data.append(tuple(row))

            SQL_ATTR_CONNECTION_TIMEOUT = 113
            login_timeout = 30
            connection_timeout = 30

            # Connect to the database
            logging.info("Connecting to the database...")
            for attempt in range(5):
                try:
                    conn = pyodbc.connect(
                        driver=self.driver,
                        server=self.database_host,
                        database=self.database_name,
                        uid=self.database_username,
                        pwd=self.database_password,
                        timeout=login_timeout,
                        attrs_before={SQL_ATTR_CONNECTION_TIMEOUT: connection_timeout},
                    )
                    cursor = conn.cursor()
                    logging.info("Successfully connected to the database.")

                except pyodbc.Error as e:
                    logging.info(f"Attempt {attempt + 1} failed: {e}")
                    wait_time = 20 * attempt  # backoff
                    logging.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

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
            cursor.fast_executemany = True
            try:
                # Use cursor.executemany to insert data from csv_reader
                cursor.executemany(insert_query, data)
                conn.commit()
                logging.info(
                    f"Successfully inserted data into staging table: {self.staging_table_name}"
                )
            except Exception as e:
                logging.error(f"Error inserting data into staging table: {str(e)}")
                raise

            # Delete existing records from final table
            delete_query = f"""
            DELETE FROM {self.final_table_name}
            WHERE EXISTS (
                SELECT 1 FROM {self.staging_table_name} s
                WHERE s.date = {self.final_table_name}.date
                AND s.state = {self.final_table_name}.state
                AND s.rto_code = {self.final_table_name}.rto_code
                AND s.rto_name = {self.final_table_name}.rto_name
                AND s.vehicle_class = {self.final_table_name}.vehicle_class
            )
            """
            logging.info(
                f"Deleting existing records from final table: {self.final_table_name}"
            )
            try:
                cursor.execute(delete_query)
                logging.info(
                    f"Successfully deleted existing records from final table: {self.final_table_name}"
                )
            except Exception as e:
                logging.error(
                    f"Error deleting existing records from final table: {str(e)}"
                )
                raise

            # # Transfer data from staging to final table
            # transfer_query = f"""
            # INSERT INTO {self.final_table_name}
            # SELECT * FROM {self.staging_table_name}
            # """
            # logging.info(
            #     f"Transferring data from staging table to final table: {self.final_table_name}"
            # )
            # try:
            #     cursor.execute(transfer_query)
            #     logging.info(
            #         f"Successfully transferred data to final table: {self.final_table_name}"
            #     )
            # except Exception as e:
            #     logging.error(f"Error transferring data to final table: {str(e)}")
            #     raise

            # Commit the transaction and close the connection
            conn.commit()
            cursor.close()
            conn.close()


def main():
    rto_ev_data_ingest = RtoDataIngest()
    if len(sys.argv) > 2:
        # If month and year are passed as command-line arguments
        month = sys.argv[1]
        year = sys.argv[2]
    else:
        # Use the default function to get the month and year
        month, year = rto_ev_data_ingest.get_year_month_label()
    rto_ev_data_ingest.data_ingest(month, year)


if __name__ == "__main__":
    main()
