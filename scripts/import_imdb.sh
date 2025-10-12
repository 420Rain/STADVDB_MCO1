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
  psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "\copy $DB_SCHEMA.$table FROM '$IMPORT_DIR/$file' WITH (FORMAT text, DELIMITER E'\t', NULL '\N', HEADER)"
}

import_tsv "akas_import" "title.akas.tsv"
import_tsv "title_basics" "title.basics.tsv"
import_tsv "crew_import" "title.crew.tsv"
import_tsv "episode" "title.episode.tsv"
import_tsv "ratings" "title.ratings.tsv"
import_tsv "name_basics_import" "name.basics.tsv"
import_tsv "principals" "title.principals.tsv"

echo "Imports completed successfully ✓"