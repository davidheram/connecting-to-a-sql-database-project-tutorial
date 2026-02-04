import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# Load environment variables
load_dotenv()
# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = (
            f"postgresql://{os.getenv('DB_USER')}:"
            f"{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:"
            f"{os.getenv('DB_PORT')}/"
            f"{os.getenv('DB_NAME')}"
        )

        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
# 2) Create the tables
engine = connect()

if engine is not None: 
    with open("src/sql/drop.sql", "r", encoding="utf-8") as file:
        drop_tables_sql = file.read()

    with engine.connect() as connection:
        connection.execute(text(drop_tables_sql))
        connection.commit()

    print("Tables dropped succesfully")

    with open("src/sql/create.sql", "r", encoding="utf-8") as file:
        create_tables_sql = file.read()

    with engine.connect() as connection:
        connection.execute(text(create_tables_sql))
        connection.commit()

    print("Tables created successfully!")

# 3) Insert data

    with open("src/sql/insert.sql", "r", encoding="utf-8") as file:
        insert_data_sql = file.read()

    with engine.connect() as connection:
        connection.execute(text(insert_data_sql))
        connection.commit()

    print("Data inserted successfully!")
# 4) Use Pandas to read and display a table
query = "SELECT * FROM books;"

df = pd.read_sql(query, engine)
print(df)
