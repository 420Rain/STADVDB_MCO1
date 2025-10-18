-- Table: stadvdb.akas
CREATE TABLE IF NOT EXISTS stadvdb.akas (
  title_id text NOT NULL,
  ordering integer NOT NULL,
  title text,
  region text,
  language text,
  types text,
  attributes text,
  is_original_title boolean
);

-- Table: stadvdb.crew
CREATE TABLE IF NOT EXISTS stadvdb.crew
(
  tconst text NOT NULL,
  directors text[],
  writers text[]
);

-- Table: stadvdb.crew_import
CREATE TABLE IF NOT EXISTS stadvdb.crew_import
(
  tconst text NOT NULL,
  directors text,
  writers text
);

-- Table: stadvdb.episode
CREATE TABLE IF NOT EXISTS stadvdb.episode
(
  tconst text NOT NULL,
  parent_tconst text NOT NULL,
  season_number integer,
  episode_number integer
);

-- Table: stadvdb.name_basics
CREATE TABLE IF NOT EXISTS stadvdb.name_basics
(
  nconst text NOT NULL,
  primary_name text,
  birth_year integer,
  death_year integer,
  known_for_titles text[],
  primary_profession text[]
);

-- Table: stadvdb.name_basics_import
CREATE TABLE IF NOT EXISTS stadvdb.name_basics_import
(
  nconst text NOT NULL,
  "primaryName" text,
  "birthYear" integer,
  "deathYear" integer,
  "primaryProfession" text,
  "knownForTitles" text
);

-- Table: stadvdb.principals
CREATE TABLE IF NOT EXISTS stadvdb.principals
(
  tconst text NOT NULL,
  ordering integer NOT NULL,
  nconst text,
  category text,
  job text,
  characters text
);

-- Table: stadvdb.ratings
CREATE TABLE IF NOT EXISTS stadvdb.ratings
(
  tconst text NOT NULL,
  "averageRating" numeric(3,1),
  "numVotes" integer
);

-- Table: stadvdb.title_basics
CREATE TABLE IF NOT EXISTS stadvdb.title_basics
(
  tconst text NOT NULL,
  title_type text,
  primary_title text,
  original_title text,
  is_adult boolean,
  start_year integer,
  end_year integer,
  runtime_minutes bigint,
  genres text[]
);

-- Table: stadvdb.title_basics_import
CREATE TABLE IF NOT EXISTS stadvdb.title_basics_import
(
  tconst text NOT NULL,
  "titleType" text,
  "primaryTitle" text,
  "originalTitle" text,
  "isAdult" boolean,
  "startYear" integer,
  "endYear" integer,
  "runtimeMinutes" integer,
  genres text
);