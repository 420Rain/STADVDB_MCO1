-- TRUNCATE TABLE stadvdb.crew, stadvdb.akas, stadvdb.name_basics RESTART IDENTITY CASCADE;

INSERT INTO stadvdb.crew (tconst, directors, writers)
SELECT
  tconst,
  STRING_TO_ARRAY(directors, ',') AS directors,
  STRING_TO_ARRAY(writers, ',') AS writers
FROM stadvdb.crew_import;

INSERT INTO stadvdb.akas (title_id, ordering, title, region, language, types, attributes, is_original_title)
SELECT
  "titleId" AS title_id,
  ordering,
  title,
  region,
  language,
  STRING_TO_ARRAY(types, ',') AS types,
  STRING_TO_ARRAY(attributes, ',') AS attributes,
  "isOriginalTitle" AS is_original_title
FROM stadvdb.akas_import;

INSERT INTO stadvdb.name_basics (nconst, primary_name, birth_year, death_year, primary_profession, known_for_titles)
SELECT
  nconst,
  "primaryName" AS primary_name,
  "birthYear" AS birth_year,
  "deathYear" AS death_year,
  STRING_TO_ARRAY("primaryProfession", ',') AS primary_profession,
  STRING_TO_ARRAY("knownForTitles", ',') AS known_for_titles
FROM stadvdb.name_basics_import;