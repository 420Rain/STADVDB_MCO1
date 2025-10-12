import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO
import sys, os
from dotenv import load_dotenv

load_dotenv()

SOURCE_DB_CONFIG = {
    "host": os.getenv("SOURCE_HOST"),
    "port": os.getenv("SOURCE_PORT"),
    "database": os.getenv("SOURCE_DB"),
    "user": os.getenv("SOURCE_USER"),
    "password": os.getenv("SOURCE_PASS"),
    "schema": os.getenv("SOURCE_SCHEMA"),
}

DWH_DB_CONFIG = {
    "host": os.getenv("DW_HOST"),
    "port": os.getenv("DW_PORT"),
    "database": os.getenv("DW_DB"),
    "user": os.getenv("DW_USER"),
    "password": os.getenv("DW_PASS"),
    "schema": os.getenv("DW_SCHEMA"),
}

def create_dwh_tables(dwh_conn):
    ddl_script = """
    DROP TABLE IF EXISTS fact_title_principals, fact_title_ratings, dim_date, dim_title, dim_person, dim_role CASCADE;

    CREATE TABLE dim_date (
        date_key INT PRIMARY KEY,
        year INT NOT NULL,
        decade INT NOT NULL,
        century INT NOT NULL
    );

    CREATE TABLE dim_title (
        title_key SERIAL PRIMARY KEY,
        tconstid VARCHAR(15) UNIQUE NOT NULL,
        title_type VARCHAR(50),
        parent_tconst VARCHAR(15),
        primary_title TEXT,
        original_title TEXT,
        title_language VARCHAR(50),
        is_adult BOOLEAN,
        start_year INT,
        end_year INT,
        episode_number INT,
        season_number INT,
        genre_1 VARCHAR(50),
        genre_2 VARCHAR(50),
        genre_3 VARCHAR(50)
    );

    CREATE TABLE dim_person (
        person_key SERIAL PRIMARY KEY,
        nconstid VARCHAR(15) UNIQUE NOT NULL,
        primary_name VARCHAR(255),
        birth_year INT,
        death_year INT,
        profession_1 VARCHAR(100),
        profession_2 VARCHAR(100),
        profession_3 VARCHAR(100)
    );

    CREATE TABLE dim_role (
        role_key SERIAL PRIMARY KEY,
        category VARCHAR(100),
        job VARCHAR(255),
        character_name VARCHAR(512),
        UNIQUE(category, job, character_name)
    );

    CREATE TABLE fact_title_ratings (
        title_key INT REFERENCES dim_title(title_key),
        date_key INT REFERENCES dim_date(date_key),
        average_rating DECIMAL(3,1),
        num_votes INT
    );

    CREATE TABLE fact_title_principals (
        title_key INT REFERENCES dim_title(title_key),
        person_key INT REFERENCES dim_person(person_key),
        role_key INT REFERENCES dim_role(role_key),
        principal_ordering INT
    );
    """
    with dwh_conn.cursor() as cur:
        cur.execute(ddl_script)
    dwh_conn.commit()
    print("Data warehouse tables created successfully.")


def load_df_to_postgres(df, table_name, conn):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)

    with conn.cursor() as cur:
        try:
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV DELIMITER E'\\t'", buffer)
            conn.commit()
            print(f"Successfully loaded {len(df)} rows into {table_name}.")
        except Exception as e:
            conn.rollback()
            print(f"Error loading data into {table_name}: {e}")
            sys.exit(1)


def etl_dim_date(source_conn, dwh_conn):
    print("Starting ETL for DimDate...")
    df_years = pd.read_sql('SELECT DISTINCT "startYear" FROM title_basics WHERE "startYear" IS NOT NULL;', source_conn)

    min_year = int(df_years['startYear'].min())
    max_year = int(df_years['startYear'].max())

    years = range(min_year, max_year + 1)
    dim_date_df = pd.DataFrame(years, columns=['year'])
    dim_date_df['date_key'] = dim_date_df['year'] # Simple key: YYYY
    dim_date_df['decade'] = (dim_date_df['year'] // 10) * 10
    dim_date_df['century'] = (dim_date_df['year'] // 100) * 100

    dim_date_df = dim_date_df[['date_key', 'year', 'decade', 'century']]

    load_df_to_postgres(dim_date_df, 'dim_date', dwh_conn)


def etl_dim_person(source_conn, dwh_conn):
    print("Starting ETL for DimPerson...")
    df = pd.read_sql('SELECT "nconst", "primaryName", "birthYear", "deathYear", "primaryProfession" FROM name_basics_import', source_conn)

    df.columns = df.columns.str.lower()
    df.rename(columns={'nconst': 'nconstid', 'primaryname': 'primary_name', 'birthyear': 'birth_year', 'deathyear': 'death_year'}, inplace=True)
    df['birth_year'] = pd.to_numeric(df['birth_year'], errors='coerce')
    df['death_year'] = pd.to_numeric(df['death_year'], errors='coerce')

    if 'primaryprofession' in df.columns:
        professions = df['primaryprofession'].str.split(',', expand=True)
        df['profession_1'] = professions[0]
        df['profession_2'] = professions[1] if 1 in professions.columns else None
        df['profession_3'] = professions[2] if 2 in professions.columns else None
    else:
        df['profession_1'] = df['profession_2'] = df['profession_3'] = None

    df_to_load = df[['nconstid', 'primary_name', 'birth_year', 'death_year', 'profession_1', 'profession_2', 'profession_3']]

    with dwh_conn.cursor() as cur:
        execute_values(cur, "INSERT INTO dim_person (nconstid, primary_name, birth_year, death_year, profession_1, profession_2, profession_3) VALUES %s", df_to_load.to_records(index=False).tolist())
    dwh_conn.commit()
    print(f"Successfully loaded {len(df_to_load)} rows into dim_person.")


def etl_dim_role(source_conn, dwh_conn):
    print("Starting ETL for DimRole...")
    query = "SELECT DISTINCT category, job, characters FROM principals;"
    df = pd.read_sql(query, source_conn)
    df.rename(columns={'characters': 'character_name'}, inplace=True)

    df.replace('\\N', None, inplace=True)

    with dwh_conn.cursor() as cur:
        execute_values(cur, "INSERT INTO dim_role (category, job, character_name) VALUES %s ON CONFLICT (category, job, character_name) DO NOTHING", df.to_records(index=False).tolist())
    dwh_conn.commit()
    print(f"Successfully loaded {len(df)} unique roles into dim_role.")


def etl_dim_title(source_conn, dwh_conn):
    print("Starting ETL for DimTitle...")
    query = """
    SELECT
        b.tconst,
        b.titletype,
        e.parentTconst,
        b.primarytitle,
        b.originaltitle,
        b.isadult,
        b.startyear,
        b.endyear,
        b.genres,
        a.language,
        e.episodeNumber,
        e.seasonNumber
    FROM title_basics b
    LEFT JOIN episode e ON b.tconst = e.tconst
    LEFT JOIN (
        SELECT titleId, language
        FROM akas_import
        WHERE isOriginalTitle = '1'
    ) a ON b.tconst = a.titleId;
    """
    df = pd.read_sql(query, source_conn)

    df.rename(columns={
        'tconst': 'tconstid', 'primarytitle': 'primary_title', 'originaltitle': 'original_title',
        'isadult': 'is_adult', 'startyear': 'start_year', 'endyear': 'end_year', 'language': 'title_language', 'titletype': 'title_type', 'episodeNumber': 'episode_number', 'seasonNumber': 'season_number'
    }, inplace=True)

    df['genres'] = df['genres'].str.split(',')
    df['genre_1'] = df['genres'].str[0].replace({'\\N': None})
    df['genre_2'] = df['genres'].str[1].replace({'\\N': None})
    df['genre_3'] = df['genres'].str[2].replace({'\\N': None})

    df['is_adult'] = df['is_adult'].apply(lambda x: True if x == '1' else False)
    for col in ['start_year', 'end_year', 'episode_number', 'season_number']:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    df_to_load = df[[
        'tconstid', 'title_type', 'parent_tconst', 'primary_title', 'original_title',
        'title_language', 'is_adult', 'start_year', 'end_year', 'episode_number',
        'season_number', 'genre_1', 'genre_2', 'genre_3'
    ]]

    with dwh_conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_title (tconstid, title_type, parent_tconst, primary_title, original_title,
                                  title_language, is_adult, start_year, end_year, episode_number,
                                  season_number, genre_1, genre_2, genre_3)
            VALUES %s
        """, df_to_load.to_records(index=False).tolist())
    dwh_conn.commit()
    print(f"Successfully loaded {len(df_to_load)} rows into dim_title.")


def etl_fact_title_ratings(source_conn, dwh_conn):
    print("Starting ETL for FactTitleRatings...")
    title_map = pd.read_sql("SELECT title_key, tconstid FROM dim_title", dwh_conn).set_index('tconstid')
    date_map = pd.read_sql("SELECT date_key, year FROM dim_date", dwh_conn).set_index('year')

    query = """
    SELECT r.tconst, r.averagerating, r.numvotes, b.startyear
    FROM ratings r
    JOIN title_basics b ON r.tconst = b.tconst
    """
    df = pd.read_sql(query, source_conn)

    df['title_key'] = df['tconst'].map(title_map['title_key'])
    df['startyear'] = pd.to_numeric(df['startyear'], errors='coerce')
    df['date_key'] = df['startyear'].map(date_map['date_key'])

    df.rename(columns={'averagerating': 'average_rating', 'numvotes': 'num_votes'}, inplace=True)

    df_to_load = df[['title_key', 'date_key', 'average_rating', 'num_votes']].dropna()
    for col in ['title_key', 'date_key', 'num_votes']:
        df_to_load[col] = df_to_load[col].astype(int)

    load_df_to_postgres(df_to_load, 'fact_title_ratings', dwh_conn)


def etl_fact_title_principals(source_conn, dwh_conn):
    print("Starting ETL for FactTitlePrincipals...")
    title_map = pd.read_sql("SELECT title_key, tconstid FROM dim_title", dwh_conn).set_index('tconstid')
    person_map = pd.read_sql("SELECT person_key, nconstid FROM dim_person", dwh_conn).set_index('nconstid')
    role_map_df = pd.read_sql("SELECT role_key, category, job, character_name FROM dim_role", dwh_conn)
    role_map_df.replace({None: 'NULL_VAL'}, inplace=True)
    role_map = role_map_df.set_index(['category', 'job', 'character_name'])

    df = pd.read_sql("SELECT tconst, nconst, ordering, category, job, characters FROM principals", source_conn)
    df.replace('\\N', None, inplace=True)

    df['title_key'] = df['tconst'].map(title_map['title_key'])
    df['person_key'] = df['nconst'].map(person_map['person_key'])

    df_role_lookup = df[['category', 'job', 'characters']].replace({None: 'NULL_VAL'})
    df_role_lookup.columns = ['category', 'job', 'character_name'] # Match index names
    role_keys = role_map.loc[pd.MultiIndex.from_frame(df_role_lookup)].reset_index()['role_key']
    df['role_key'] = role_keys

    df_to_load = df[['title_key', 'person_key', 'role_key', 'ordering']].dropna()

    for col in ['title_key', 'person_key', 'role_key']:
        df_to_load[col] = df_to_load[col].astype(int)

    load_df_to_postgres(df_to_load, 'fact_title_principals', dwh_conn)

def main():
    try:
        source_conn = psycopg2.connect(
            host=SOURCE_DB_CONFIG["host"],
            port=SOURCE_DB_CONFIG["port"],
            database=SOURCE_DB_CONFIG["database"],
            user=SOURCE_DB_CONFIG["user"],
            password=SOURCE_DB_CONFIG["password"],
        )
        with source_conn.cursor() as cur:
            cur.execute(f"SET search_path TO {SOURCE_DB_CONFIG['schema']};")

        dwh_conn = psycopg2.connect(
            host=DWH_DB_CONFIG["host"],
            port=DWH_DB_CONFIG["port"],
            database=DWH_DB_CONFIG["database"],
            user=DWH_DB_CONFIG["user"],
            password=DWH_DB_CONFIG["password"],
        )
        with dwh_conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DWH_DB_CONFIG['schema']};")
        print("Successfully connected to source and DWH databases.")

        # create_dwh_tables(dwh_conn)

        # etl_dim_date(source_conn, dwh_conn)
        etl_dim_person(source_conn, dwh_conn)
        # etl_dim_role(source_conn, dwh_conn)
        # etl_dim_title(source_conn, dwh_conn)

        # etl_fact_title_ratings(source_conn, dwh_conn)
        # etl_fact_title_principals(source_conn, dwh_conn)

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if 'source_conn' in locals() and source_conn: # type: ignore
            source_conn.close()
        if 'dwh_conn' in locals() and dwh_conn: # type: ignore
            dwh_conn.close()
        print("ETL process finished. Connections closed.")

if __name__ == '__main__':
    main()