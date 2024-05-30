from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('sqlite://', echo=False)

df = pd.read_csv("oem_data_by_state_and_category_2024_to_2013.csv")

with engine.begin() as connection:
    df.to_sql(name='fact_oem_data_by_category', con=connection)

with engine.connect() as conn:
   conn.execute(text("SELECT * FROM fact_oem_data_by_category limit 100")).fetchall()