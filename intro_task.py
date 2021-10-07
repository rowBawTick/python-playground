import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine
from sqlalchemy_utils import create_database, database_exists


def main():
    india_df = get_india_data()
    import_data(india_df)


def get_india_data():
    india_df = pd.read_csv('./data/indian_data.csv')

    # Use .head() to see the first 5 rows (useful for checking columns)

    # rename column headings to lower (.lower()) case and replace spaces (' ') with underscores ('_')
    india_df.rename(columns=lambda x: x.replace(' ', '_').lower(), inplace=True)

    # Use .describe() to see some common stats about the data

    # print out all the columns
    print('\n *** Columns ***')
    for column in india_df.columns:
        print(column)

    return india_df


# Import the data into the database
def import_data(india_df):
    print('\n *** importing data *** \n')
    # You should use environment variables to hide passwords
    database_user = 'root'
    database_ip = 'localhost'
    database_port = 3306
    database_name = 'db'
    try:
        sql_engine = create_engine(
            f'mysql+mysqlconnector://{database_user}@{database_ip}:{database_port}/{database_name}',
            connect_args={'connect_timeout': 180}
        )

        # Create database if it doesn't exist already
        create_mysql_database(sql_engine)

        if sql_engine:
            print(sql_engine, '\n')

            table_name = 'indian_population'
            # Create the table if it doesn't exist
            create_table(sql_engine, table_name, india_df)

    except SQLAlchemyError as e:
        print("Error while connecting to MySQL \n", e)
    finally:
        if sql_engine:
            sql_engine.dispose()
        print("MySQL connection is closed \n")


def create_mysql_database(sql_engine):
    # Create Database if it doesn't exist
    if not database_exists(sql_engine.url):
        create_database(sql_engine.url)
    print(database_exists(sql_engine.url))


def create_table(sql_engine, table_name, india_df):
    meta = MetaData()
    if not sql_engine.dialect.has_table(sql_engine, table_name):
        indian_population_table = Table(
            table_name, meta,
            Column('id', Integer, primary_key=True),
            Column('registrar', String(255)),
            Column('enrolment_agency', String(255)),
            Column('state', String(255)),
            Column('district', String(255)),
            Column('sub_district', String(255)),
            Column('pin_code', String(255)),
            Column('gender', String(255)),
            Column('age', Integer),
            Column('aadhaar_generated', Integer),
            Column('enrolment_rejected', Integer),
            Column('residents_providing_email', Integer),
            Column('residents_providing_mobile_number', Integer),
        )
        meta.create_all(sql_engine)

        # import data
        india_df.to_sql(table_name, con=sql_engine, if_exists='replace', index=False, chunksize=1000)


if __name__ == "__main__":
    main()

