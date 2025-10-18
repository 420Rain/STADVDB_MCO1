-- [~17s]
INSERT INTO stadvdb.crew (tconst, directors, writers)
SELECT
  tconst,
  STRING_TO_ARRAY(directors, ',') AS directors,
  STRING_TO_ARRAY(writers, ',') AS writers
FROM stadvdb.crew_import;

-- [~27s]
INSERT INTO stadvdb.name_basics (nconst, primary_name, birth_year, death_year, primary_profession, known_for_titles)
SELECT
  nconst,
  "primaryName" AS primary_name,
  "birthYear" AS birth_year,
  "deathYear" AS death_year,
  STRING_TO_ARRAY("primaryProfession", ',') AS primary_profession,
  STRING_TO_ARRAY("knownForTitles", ',') AS known_for_titles
FROM stadvdb.name_basics_import;

-- [~21s]
INSERT INTO stadvdb.title_basics (tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres)
	SELECT
	tconst, 
	"titleType" AS title_type, 
	"primaryTitle" AS primary_title, 
	"originalTitle" AS original_title, 
	"isAdult" AS is_adult, 
	"startYear" AS start_year, 
	"endYear" AS end_year, 
	"runtimeMinutes" AS runtime_minutes,
	STRING_TO_ARRAY("genres", ',') AS genres
FROM stadvdb.title_basics_import;

ALTER TABLE stadvdb.ratings
	RENAME COLUMN "averageRating" TO average_rating;

ALTER TABLE stadvdb.ratings
	RENAME COLUMN "numVotes" TO num_votes;
	
CREATE INDEX IF NOT EXISTS akas_titleid_isoriginal_key ON stadvdb.akas(title_id, is_original_title);
CREATE INDEX akas_title_id_idx ON stadvdb.akas (title_id);
CREATE INDEX principals_tconst_idx ON stadvdb.principals (tconst);

ALTER TABLE IF EXISTS stadvdb.akas
	ADD CONSTRAINT akas_pkey PRIMARY KEY (ordering, title_id);

ALTER TABLE IF EXISTS stadvdb.crew
	ADD CONSTRAINT crew_pkey PRIMARY KEY (tconst);

ALTER TABLE IF EXISTS stadvdb.episode
	ADD CONSTRAINT episode_pkey PRIMARY KEY (tconst);

ALTER TABLE IF EXISTS stadvdb.name_basics
	ADD CONSTRAINT name_basics_pkey PRIMARY KEY (nconst);

ALTER TABLE IF EXISTS stadvdb.principals
	ADD CONSTRAINT principals_pkey PRIMARY KEY (tconst, ordering);

ALTER TABLE IF EXISTS stadvdb.ratings
	ADD CONSTRAINT ratings_pkey PRIMARY KEY (tconst);

ALTER TABLE IF EXISTS stadvdb.title_basics
	ADD CONSTRAINT title_basics_pkey PRIMARY KEY (tconst);
	
ALTER TABLE IF EXISTS stadvdb.akas
	ADD CONSTRAINT akas_title_id_fkey FOREIGN KEY (title_id)
        REFERENCES stadvdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
		
ALTER TABLE IF EXISTS stadvdb.episode
	ADD CONSTRAINT episode_parent_tconst_fkey FOREIGN KEY (parent_tconst)
		REFERENCES stadvdb.title_basics (tconst) MATCH SIMPLE
	    ON UPDATE NO ACTION
		ON DELETE NO ACTION
		NOT VALID;
		
ALTER TABLE IF EXISTS stadvdb.episode
	ADD CONSTRAINT episode_tconst_fkey FOREIGN KEY (tconst)
		REFERENCES stadvdb.title_basics (tconst) MATCH SIMPLE
		ON UPDATE NO ACTION
		ON DELETE NO ACTION
		NOT VALID;

ALTER TABLE IF EXISTS stadvdb.principals
    ADD CONSTRAINT principals_tconst_fkey FOREIGN KEY (tconst)
        REFERENCES stadvdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

ALTER TABLE IF EXISTS stadvdb.ratings
    ADD CONSTRAINT ratings_tconst_fkey FOREIGN KEY (tconst)
        REFERENCES stadvdb.title_basics (tconst) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;