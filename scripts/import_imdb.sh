#!/bin/bash
# Import IMDb TSV files into PostgreSQL
set -e

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

echo "Starting data import..."

import_tsv() {
  local table=$1
  local file=$2
  echo "Importing $file into $table..."
  psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_DATABASE" -c "\copy $DB_SCHEMA.$table FROM '$IMPORT_DIR/$file' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', HEADER)"
}

import_tsv "akas" "title.akas.tsv"                  # 53395136 rows
import_tsv "title_basics_import" "title.basics.tsv" # 11962454 rows
import_tsv "crew_import" "title.crew.tsv"           # 11962454 rows
import_tsv "episode" "title.episode.tsv"            # 9211339 rows
import_tsv "ratings" "title.ratings.tsv"            # 1623931 rows
import_tsv "name_basics_import" "name.basics.tsv"   # 14765578 rows
import_tsv "principals" "title.principals.tsv"      # 95161168 rows

echo "Imports completed successfully ✓"