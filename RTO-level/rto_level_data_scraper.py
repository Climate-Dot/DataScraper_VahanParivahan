from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException
import random
import time
from datetime import datetime
import pandas as pd
import pyodbc


# Intialize  database Connection
def model(sql_statement, status, data):
    server = ""
    database = ""
    username = ""
    password = ""
    response = ""
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        if status == "insert":
            # Execute the INSERT single value query
            cursor.execute(sql_statement, data)
            conn.commit()
            response = "SQL Executed Successfully"
        elif status == "insertmany":
            # Execute the INSERT Multiple values query
            cursor.executemany(sql_statement, data)
            conn.commit()
            response = "Record Inserted Successfully"
        elif status == "truncate":
            # Execute the truncate query
            cursor.execute(sql_statement)
            # Commit the changes
            conn.commit()
            response = "Table Truncated Successfully"
        elif status == "delete":
            # Execute the delete record query
            cursor.execute(sql_statement)
            # Commit the changes
            conn.commit()
            response = "Record Deleted Successfully"
        elif status == "update":
            # Execute the delete record query
            cursor.execute(sql_statement)
            # Commit the changes
            conn.commit()
            response = "Record Updated Successfully"
        else:
            # Execute the select record query
            cursor.execute(sql_statement)
            response = cursor.fetchall()
    except pyodbc.Error as e:
        response = f"Connection Error: {str(e)}"
    finally:
        conn.close()
        return response


# Initialize the Chrome WebDriver
driver_path = (
    r"C:\Users\mayur\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
)
web_url = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
service = ChromeService(driver_path)
options = Options()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(service=service, options=options)
driver.get(web_url)


# Function to click an element by ID and wait for it to become clickable
def click_element_by_id_and_wait(driver, element_id):
    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    element.click()
    time.sleep(1)


# Function to click an element by ID and wait for it to become clickable
def click_element_by_class_and_wait(driver, class_name, wait_time):
    element = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.CLASS_NAME, class_name))
    )
    element.click()
    time.sleep(wait_time)


# Intialize all drop-down Labels

ALL_LABEL = model(
    "select element_id,selector_id,element_name from elements", "select", ""
)

for LABELS in ALL_LABEL:
    if LABELS[2] == "DOWNLOAD_BUTTON":
        DOWNLOAD_BTN_LABEL = LABELS[0]
    if LABELS[2] == "REFRESH_BUTTON":
        REFRESH_BTN_LABEL = LABELS[0]
    if LABELS[2] == "STATE_LABEL":
        STATE_LABEL = LABELS[0]
        STATE_INPUT = LABELS[1]
    if LABELS[2] == "RTO_LABEL":
        RTO_LABEL = LABELS[0]
        RTO_INPUT = LABELS[1]
    if LABELS[2] == "Y_AXIS_LABEL":
        Y_AXIS_LABEL = LABELS[0]
        Y_AXIS_INPUT = LABELS[1]
    if LABELS[2] == "X_AXIS_LABEL":
        X_AXIS_LABEL = LABELS[0]
        X_AXIS_INPUT = LABELS[1]
    if LABELS[2] == "YEAR_TYPE_LABEL":
        YEAR_TYPE_LABEL = LABELS[0]
        YEAR_TYPE_INPUT = LABELS[1]
    if LABELS[2] == "YEAR_LABEL":
        YEAR_LABEL = LABELS[0]
        YEAR_INPUT = LABELS[1]
    if LABELS[2] == "MONTH_LABEL":
        MONTH_LABEL = LABELS[0]
        MONTH_INPUT = LABELS[1]

# Initialize all Drop-Down IDs & Values
YEAR_COUNT = 10
MONTH_COUNT = 12
STATE_VALUE = ""

# X-Axis Values
X_AXIS_DATA = model(
    "select axis_id,axis_name from axis_values where axis_type = 'x_axis'", "select", ""
)
for X_AXIS in X_AXIS_DATA:
    if X_AXIS[1] == "Fuel":
        X_AXIS_VALUE = X_AXIS[0]

# Y-Axis Values
Y_AXIS_DATA = model(
    "select axis_id,axis_name from axis_values where axis_type = 'y_axis'", "select", ""
)
for Y_AXIS in Y_AXIS_DATA:
    if Y_AXIS[1] == "Vehicle Class":
        Y_AXIS_VALUE = Y_AXIS[0]


# State Values
state_statement = "select state_id, state_name,state_value from states"
if STATE_VALUE:
    state_statement = "where state_value = '" + STATE_VALUE + "';"

STATE_DATA = model(state_statement, "select", "")


# Function to set filters
def set_filters(driver):
    click_element_by_id_and_wait(driver, Y_AXIS_LABEL)
    click_element_by_id_and_wait(driver, Y_AXIS_VALUE)
    click_element_by_id_and_wait(driver, X_AXIS_LABEL)
    click_element_by_id_and_wait(driver, X_AXIS_VALUE)


def set_elements(driver, data, page_no, pages):
    for dt in data:
        click_element_by_id_and_wait(driver, dt)
        time.sleep(1)
    time.sleep(5)
    if page_no > 0 & page_no != pages:
        for j in range(page_no):
            click_element_by_class_and_wait(driver, "ui-paginator-next", 5)
            time.sleep(2)


# Primary Loop
def run_primary_loop(driver):
    for STATE in STATE_DATA:
        click_element_by_id_and_wait(driver, STATE_LABEL)
        click_element_by_id_and_wait(driver, STATE[0])
        time.sleep(1)
        click_element_by_id_and_wait(driver, REFRESH_BTN_LABEL)
        time.sleep(1)

        GET_OLD_RTOS = model(
            "select rto_name from rto_staging_table where state_name = '"
            + STATE[2]
            + "' Group by rto_name having count(distinct year_id) = "
            + str(YEAR_COUNT)
            + " and count(distinct month_name) = "
            + str(MONTH_COUNT)
            + ";",
            "select",
            "",
        )

        RTO_STATEMENT = (
            "SELECT rto_id,rto_name,rto_value from rtos where state_id = '"
            + STATE[0]
            + "' and rto_value NOT LIKE '%All Vahan4 Running%'"
        )
        if GET_OLD_RTOS:
            RTO_STATEMENT += (
                " and rto_value NOT IN (select rto_name from rto_staging_table where state_name = '"
                + STATE[2]
                + "' Group by rto_name having count(distinct year_id) = "
                + str(YEAR_COUNT)
                + " and count(distinct month_name) = "
                + str(MONTH_COUNT)
                + ") "
            )

        RTO_DATA = model(RTO_STATEMENT, "select", "")

        for RTO in RTO_DATA:
            click_element_by_id_and_wait(driver, RTO_LABEL)
            click_element_by_id_and_wait(driver, RTO[0])
            # Year values
            CHECK_YEARS = model(
                "select distinct year_id from rto_staging_table where state_name = '"
                + STATE[2]
                + "' and rto_name = '"
                + str(RTO[2])
                + "' group by year_id having count(distinct month_name) = 12;",
                "select",
                "",
            )
            if len(CHECK_YEARS) != YEAR_COUNT:
                YEAR_DATA = model(
                    "select id, year_id from years where id in ('2023') and id not in (select distinct year_id from rto_staging_table where state_name = '"
                    + STATE[2]
                    + "' and rto_name = '"
                    + str(RTO[2])
                    + "' group by year_id having count(distinct month_name) = 12 ) ;",
                    "select",
                    "",
                )

                for YEAR in YEAR_DATA:
                    click_element_by_id_and_wait(driver, YEAR_LABEL)
                    click_element_by_id_and_wait(driver, YEAR[1])
                    click_element_by_id_and_wait(driver, REFRESH_BTN_LABEL)
                    time.sleep(1)
                    YEAR_VALUE = str(YEAR[0])
                    GET_OLD_MONTHS = model(
                        "select month_name from rto_staging_table where state_name = '"
                        + STATE_VALUE
                        + "' and rto_name = '"
                        + str(RTO[2])
                        + "' and year_id = '"
                        + YEAR_VALUE
                        + "' Group by month_name having count(*) > 0",
                        "select",
                        "",
                    )

                    MONTH_STATEMENT = (
                        "select month_id, month_name from months where year_id = '"
                        + YEAR_VALUE
                        + "'"
                    )
                    if GET_OLD_MONTHS:
                        MONTH_STATEMENT += (
                            " and month_name NOT IN (select month_name from rto_staging_table where state_name = '"
                            + STATE_VALUE
                            + "' and rto_name = '"
                            + str(RTO[2])
                            + "' and year_id = '"
                            + YEAR_VALUE
                            + "' Group by month_name having count(*) > 0) ;"
                        )

                    # MONTH VALUES
                    MONTH_DATA = model(MONTH_STATEMENT, "select", "")

                    for MONTH in MONTH_DATA:
                        click_element_by_id_and_wait(driver, MONTH_LABEL)
                        click_element_by_id_and_wait(driver, MONTH[0])
                        time.sleep(1)

                        paginated_pages = driver.find_elements(
                            By.CLASS_NAME, "ui-paginator-page"
                        )
                        PAGES_COUNT = len(paginated_pages)
                        if PAGES_COUNT > 0:
                            for j in range(PAGES_COUNT):
                                set_id_values = (
                                    STATE_LABEL,
                                    STATE[0],
                                    REFRESH_BTN_LABEL,
                                    RTO_LABEL,
                                    RTO[0],
                                    YEAR_LABEL,
                                    YEAR[1],
                                    REFRESH_BTN_LABEL,
                                    MONTH_LABEL,
                                    MONTH[0],
                                )

                                response = get_page_data(
                                    driver,
                                    set_id_values,
                                    j,
                                    PAGES_COUNT,
                                    STATE,
                                    RTO,
                                    YEAR,
                                    MONTH,
                                )

                                if response == "error":
                                    get_page_data(
                                        driver,
                                        set_id_values,
                                        j,
                                        PAGES_COUNT,
                                        STATE,
                                        YEAR,
                                        MONTH,
                                    )


def get_page_data(driver, data, page_no, PAGES_COUNT, STATE, RTO, YEAR, MONTH):
    row_data = []
    HEADER = [
        "sr._no.",
        "vehicle_class",
        "cng_only",
        "diesel",
        "diesel/hybrid",
        "di-methyl_ether",
        "dual_diesel/bio_cng",
        "dual_diesel/cng",
        "dual_diesel/lng",
        "electric(bov)",
        "ethanol",
        "fuel_cell_hydrogen",
        "lng",
        "lpg_only",
        "methanol",
        "not_applicable",
        "petrol",
        "petrol/cng",
        "petrol/ethanol",
        "petrol/hybrid",
        "petrol/lpg",
        "petrol/methanol",
        "solar",
        "total",
    ]

    insert_statement = "Insert Into rto_staging_table (year_id,month_name,day,date,state_name,rto_name,vehicle_class,vehicle_category,vehicle_type,cng_only,diesel,diesel_hybrid,di_methyl_ether,dual_diesel_bio_cng,dual_diesel_cng,dual_diesel_lng,electric_bov,ethanol,fuel_cell_hydrogen,lng,lpg_only,methanol,not_applicable,petrol,petrol_cng,petrol_ethanol,petrol_hybrid,petrol_lpg,petrol_methanol,solar,total,insert_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    driver.refresh()
    time.sleep(2)
    set_filters(driver)
    set_elements(driver, data, page_no, PAGES_COUNT)
    time.sleep(2)
    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "groupingTable_data"))
    )
    time.sleep(1)
    OUTPUT_ELEMENT = element.find_elements(By.TAG_NAME, "label")
    time.sleep(1)
    element_data = []
    i = 0
    a = 0

    if OUTPUT_ELEMENT:
        for OUTPUT in OUTPUT_ELEMENT:
            element_html = OUTPUT.text.replace(",", "")

            if HEADER[i] == "total":
                current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                element_data.append(element_html)
                element_data.append(current_date)

                row_data.append(element_data)

                a = a + 1
                i = 0
                element_data = []
            else:
                if HEADER[i] == "sr._no.":
                    month_date = f"{MONTH[1]} 01 {YEAR[0]} 00:00:00"
                    new_date = datetime.strptime(month_date, "%b %d %Y %H:%M:%S")

                    element_data = [
                        YEAR[0],
                        MONTH[1],
                        "1",
                        str(new_date),
                        STATE[2],
                        RTO[2],
                    ]
                else:
                    if HEADER[i] == "vehicle_class":
                        select_statement = (
                            "select b.type_name,c.category_name from vehicle_classes a inner join vehicle_types b on a.type_id = b.type_id inner join vehicle_categories c on b.category_id = c.category_id where a.class_name = '"
                            + element_html
                            + "' ;"
                        )
                        GET_CATEGORY = model(select_statement, "select", "")

                        element_data.append(element_html)
                        element_data.append(GET_CATEGORY[0][1])
                        element_data.append(GET_CATEGORY[0][0])
                    else:
                        element_data.append(element_html)

                i += 1
        print_row = model(insert_statement, "insertmany", row_data)
        if print_row == "Record Inserted Successfully":
            return "success"
        else:
            return "error"


# Execute the code
set_filters(driver)
run_primary_loop(driver)

# Close the WebDriver when done
driver.quit()
