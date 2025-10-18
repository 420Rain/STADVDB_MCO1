DROP TABLE IF EXISTS 
    dw_schema.fact_title_principals,
    dw_schema.fact_title_ratings,
    dw_schema.dim_date,
    dw_schema.dim_title,
    dw_schema.dim_person,
    dw_schema.dim_role 
CASCADE;

CREATE TABLE dw_schema.dim_date (
    date_key INT PRIMARY KEY,
    year INT NOT NULL,
    decade INT NOT NULL,
    century INT NOT NULL
);

CREATE TABLE dw_schema.dim_person (
    person_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nconstid TEXT UNIQUE NOT NULL,
    primary_name VARCHAR(255),
    birth_year INT,
    death_year INT,
    profession_1 VARCHAR(100),
    profession_2 VARCHAR(100),
    profession_3 VARCHAR(100)
);

CREATE TABLE dw_schema.dim_role (
    role_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    category TEXT,
    job VARCHAR(300),
    character_name VARCHAR(500),
    UNIQUE(category, job, character_name)
);

CREATE TABLE dw_schema.dim_title (
    title_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tconstid TEXT NOT NULL,
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

CREATE TABLE dw_schema.fact_title_ratings (
    title_key BIGINT REFERENCES dw_schema.dim_title(title_key),
    date_key INT REFERENCES dw_schema.dim_date(date_key),
    average_rating DECIMAL(3,1),
    num_votes INT
);

CREATE TABLE dw_schema.fact_title_principals (
    title_key BIGINT REFERENCES dw_schema.dim_title(title_key),
    person_key BIGINT REFERENCES dw_schema.dim_person(person_key),
    role_key BIGINT REFERENCES dw_schema.dim_role(role_key),
    principal_ordering INT
);

-- dim_date 159 rows [~17s]
WITH bounds AS (
  SELECT 
    MIN(start_year)::INT AS min_year,
    MAX(start_year)::INT AS max_year
  FROM stadvdb.title_basics
  WHERE start_year IS NOT NULL
)
INSERT INTO dw_schema.dim_date (date_key, year, decade, century)
SELECT 
    gs.year AS date_key,
    gs.year,
    (gs.year / 10) * 10 AS decade,
    (gs.year / 100) * 100 AS century
FROM bounds b, generate_series(b.min_year, b.max_year) AS gs(year)
ON CONFLICT (date_key) DO NOTHING;

-- dim_person [~3m]
INSERT INTO dw_schema.dim_person (nconstid, primary_name, birth_year, death_year, profession_1, profession_2, profession_3)
SELECT
    nconst AS nconstid,
    primary_name,
    birth_year,
    death_year,
    primary_profession[1] AS profession_1,
    primary_profession[2] AS profession_2,
    primary_profession[3] AS profession_3
FROM stadvdb.name_basics;

-- dim_role 4599505 rows [~1m 8s]
INSERT INTO dw_schema.dim_role(category, job, character_name)
SELECT DISTINCT
    category,
    job,
    characters AS character_name
FROM stadvdb.principals
ON CONFLICT (category, job, character_name) DO NOTHING;

-- dim_title
-- [~1m 40s]
CREATE TEMP TABLE akas_filter AS
SELECT 
	title_id, 
	language
FROM stadvdb.akas;
CREATE INDEX akas_filter_title_id_idx ON akas_filter(title_id);

-- [~5m]
INSERT INTO dw_schema.dim_title(
    tconstid, title_type, parent_tconst, primary_title, original_title,
    title_language, is_adult, start_year, end_year, episode_number,
    season_number, genre_1, genre_2, genre_3
)
SELECT
    b.tconst AS tconstid,
    b.title_type,
    e.parent_tconst,
    b.primary_title,
    b.original_title,
    a.language AS title_language,
    b.is_adult,
    b.start_year,
    b.end_year,
    e.episode_number,
    e.season_number,
	b.genres[1] AS genre_1,
    b.genres[2] AS genre_2,
    b.genres[3] AS genre_3
FROM stadvdb.title_basics b
LEFT JOIN stadvdb.episode e ON b.tconst = e.tconst
LEFT JOIN akas_filter a ON b.tconst = a.title_id;

CREATE INDEX dim_date_year_idx ON dw_schema.dim_date (year);
CREATE INDEX dim_title_tconstid_idx ON dw_schema.dim_title (tconstid);
CREATE INDEX dim_person_nconstid_idx ON dw_schema.dim_person (nconstid);
CREATE INDEX dim_role_category_idx ON dw_schema.dim_role (category);

-- fact_title_ratings
INSERT INTO dw_schema.fact_title_ratings(title_key, date_key, average_rating, num_votes)
SELECT
    t.title_key,
    d.date_key,
    r.average_rating,
    r.num_votes
FROM stadvdb.ratings r
JOIN stadvdb.title_basics b ON r.tconst = b.tconst
JOIN dw_schema.dim_title t ON t.tconstid = b.tconst
JOIN dw_schema.dim_date d ON d.year = b.start_year;

-- fact_title_principals (Best to do this in batches)
--tt0000001
--tt9916880
INSERT INTO dw_schema.fact_title_principals(title_key, person_key, role_key, principal_ordering)
SELECT
    t.title_key,
    p.person_key,
    r.role_key,
    pr.ordering AS principal_ordering
FROM stadvdb.principals pr
JOIN dw_schema.dim_title t ON t.tconstid = pr.tconst
JOIN dw_schema.dim_person p ON p.nconstid = pr.nconst
JOIN dw_schema.dim_role r ON r.category IS NOT DISTINCT FROM pr.category
                         AND r.job IS NOT DISTINCT FROM pr.job
                         AND r.character_name IS NOT DISTINCT FROM pr.characters
WHERE pr.tconst = 'tt0000001'; -- AND pr.tconst < 'tt0010000';