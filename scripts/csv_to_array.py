import psycopg2
from psycopg2.extras import execute_values
from utils.conn import get_connection
import pandas as pd

def load_akas(conn):
  df = pd.read_sql(
    'SELECT "titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle" FROM akas_import',
    conn
  )

  df["types"] = df["types"].str.split(",")
  df["attributes"] = df["attributes"].str.split(",")

  tuples = [(
    row.titleId,
    row.ordering,
    row.title,
    row.region,
    row.language,
    row.types,
    row.attributes,
    row.isOriginalTitle,
  ) for row in df.itertuples(index=False)]

  insert_query = """
    INSERT INTO stadvdb.akas (title_id, ordering, title, region, language, types, attributes, is_original_title)
    VALUES %s
  """

  with conn.cursor() as cur:
    execute_values(cur, insert_query, tuples)
    conn.commit()

def load_crew(conn):
  df = pd.read_sql(
    'SELECT "tconst", "directors", "writers" FROM crew_import',
    conn
  )

  df["directors"] = df["directors"].str.split(",")
  df["writers"] = df["writers"].str.split(",")

  tuples = [(
    row.tconst,
    row.directors,
    row.writers,
  ) for row in df.itertuples(index=False)]

  insert_query = """
    INSERT INTO stadvdb.crew (tconst, directors, writers)
    VALUES %s
  """

  with conn.cursor() as cur:
    execute_values(cur, insert_query, tuples)
    conn.commit()

def load_name_basics(conn):
  df = pd.read_sql(
    'SELECT "nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles" FROM name_basics_import',
    conn
  )

  df["primaryProfession"] = df["primaryProfession"].str.split(",")
  df["knownForTitles"] = df["knownForTitles"].str.split(",")

  tuples = [(
    row.nconst,
    row.primaryName,
    row.birthYear,
    row.deathYear,
    row.primaryProfession,
    row.knownForTitles
  ) for row in df.itertuples(index=False)]

  insert_query = """
    INSERT INTO stadvdb.name_basics (nconst, primary_name, birth_year, death_year, primary_profession, known_for_titles)
    VALUES %s
  """

  with conn.cursor() as cur:
    execute_values(cur, insert_query, tuples)
    conn.commit()

def main():
  with get_connection() as conn:
    load_crew(conn)
    load_akas(conn)
    load_name_basics(conn)

if __name__ == "__main__":
  main()