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
    person_key SERIAL PRIMARY KEY,
    nconstid VARCHAR(15) UNIQUE NOT NULL,
    primary_name VARCHAR(255),
    birth_year INT,
    death_year INT,
    profession_1 VARCHAR(100),
    profession_2 VARCHAR(100),
    profession_3 VARCHAR(100)
);

CREATE TABLE dw_schema.dim_role (
    role_key SERIAL PRIMARY KEY,
    category VARCHAR(100),
    job VARCHAR(255),
    character_name VARCHAR(512),
    UNIQUE(category, job, character_name)
);

CREATE TABLE dw_schema.dim_title (
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

CREATE TABLE dw_schema.fact_title_ratings (
    title_key INT REFERENCES dw_schema.dim_title(title_key),
    date_key INT REFERENCES dw_schema.dim_date(date_key),
    average_rating DECIMAL(3,1),
    num_votes INT
);

CREATE TABLE dw_schema.fact_title_principals (
    title_key INT REFERENCES dw_schema.dim_title(title_key),
    person_key INT REFERENCES dw_schema.dim_person(person_key),
    role_key INT REFERENCES dw_schema.dim_role(role_key),
    principal_ordering INT
);

-- dim_date
INSERT INTO dw_schema.dim_date (date_key, year, decade, century)
SELECT 
    year AS date_key,
    year,
    (year / 10) * 10 AS decade,
    (year / 100) * 100 AS century
FROM generate_series(
    (SELECT MIN("startYear"::INT) FROM stadvdb.title_basics WHERE "startYear" IS NOT NULL),
    (SELECT MAX("startYear"::INT) FROM stadvdb.title_basics WHERE "startYear" IS NOT NULL)
) AS t(year);

-- dim_person
INSERT INTO dw_schema.dim_person (nconstid, primary_name, birth_year, death_year, profession_1, profession_2, profession_3)
SELECT
    "nconst" AS nconstid,
    "primaryName" AS primary_name,
    NULLIF("birthYear", '\N')::INT AS birth_year,
    NULLIF("deathYear", '\N')::INT AS death_year,
    split_part("primaryProfession", ',', 1) AS profession_1,
    NULLIF(split_part("primaryProfession", ',', 2), '') AS profession_2,
    NULLIF(split_part("primaryProfession", ',', 3), '') AS profession_3
FROM stadvdb.name_basics;

-- dim_role
INSERT INTO dw_schema.dim_role(category, job, character_name)
SELECT DISTINCT
    category,
    NULLIF(job, '\N') AS job,
    NULLIF(characters, '\N') AS character_name
FROM stadvdb.principals
ON CONFLICT (category, job, character_name) DO NOTHING;

-- dim_title
INSERT INTO dw_schema.dim_title(
    tconstid, title_type, parent_tconst, primary_title, original_title,
    title_language, is_adult, start_year, end_year, episode_number,
    season_number, genre_1, genre_2, genre_3
)
SELECT
    b.tconst AS tconstid,
    b.titletype AS title_type,
    e.parentTconst AS parent_tconst,
    b.primarytitle AS primary_title,
    b.originaltitle AS original_title,
    a.language AS title_language,
    b.isadult::BOOLEAN AS is_adult,
    NULLIF(b.startyear, '\N')::INT AS start_year,
    NULLIF(b.endyear, '\N')::INT AS end_year,
    NULLIF(e.episodeNumber, '\N')::INT AS episode_number,
    NULLIF(e.seasonNumber, '\N')::INT AS season_number,
    split_part(b.genres, ',', 1) AS genre_1,
    NULLIF(split_part(b.genres, ',', 2), '') AS genre_2,
    NULLIF(split_part(b.genres, ',', 3), '') AS genre_3
FROM stadvdb.title_basics b
LEFT JOIN stadvdb.episode e ON b.tconst = e.tconst
LEFT JOIN (
    SELECT titleId, language
    FROM stadvdb.akas
    WHERE isOriginalTitle = '1'
) a ON b.tconst = a.titleId;

-- fact_title_ratings
INSERT INTO dw_schema.fact_title_ratings(title_key, date_key, average_rating, num_votes)
SELECT
    t.title_key,
    d.date_key,
    r.averagerating AS average_rating,
    r.numvotes AS num_votes
FROM stadvdb.ratings r
JOIN stadvdb.title_basics b ON r.tconst = b.tconst
JOIN dw_schema.dim_title t ON t.tconstid = b.tconst
JOIN dw_schema.dim_date d ON d.year = NULLIF(b.startyear,'\N')::INT;

-- fact_title_principals
INSERT INTO dw_schema.fact_title_principals(title_key, person_key, role_key, principal_ordering)
SELECT
    t.title_key,
    p.person_key,
    r.role_key,
    pr.ordering AS principal_ordering
FROM stadvdb.principals pr
JOIN dw_schema.dim_title t ON t.tconstid = pr.tconst
JOIN dw_schema.dim_person p ON p.nconstid = pr.nconst
JOIN dw_schema.dim_role r ON r.category = COALESCE(pr.category, 'NULL') 
                          AND r.job = COALESCE(pr.job, 'NULL')
                          AND r.character_name = COALESCE(pr.characters, 'NULL');
