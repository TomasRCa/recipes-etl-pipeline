import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

# *********************************************************************************************************************************************
# 1 - Config
# *********************************************************************************************************************************************
load_dotenv()

DB_HOST     = os.getenv("POSTGRES_HOST")
DB_PORT     = os.getenv("POSTGRES_PORT")
DB_NAME     = os.getenv("POSTGRES_DB")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "recipes"

SILVER_SCHEMA = "silver"
SILVER_TABLE = "recipes"
SILVER_CUISINES_TABLE = "cuisines"
SILVER_DIETS_TABLE = "diets"
SYNC_STATE_TABLE = "sync_state"
PIPELINE_NAME = "bronze_to_silver_recipes"

LOG_SCHEMA = "logging"
LOG_TABLE = "transform_log"

BATCH_SIZE = 500  # rows pulled from bronze per cycle

_missing = [k for k, v in {
    "POSTGRES_HOST": DB_HOST,
    "POSTGRES_PORT": DB_PORT,
    "POSTGRES_DB": DB_NAME,
    "POSTGRES_USER": DB_USER,
    "POSTGRES_PASSWORD": DB_PASSWORD,
}.items() if not v]

if _missing:
    raise EnvironmentError(f"Missing environment variables: {', '.join(_missing)}")


# *********************************************************************************************************************************************
# 2 - Engine to connect to the PostgreSQL Database
# *********************************************************************************************************************************************
def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True)


ENGINE = get_engine()


# *********************************************************************************************************************************************
# 3 - Create Logging Schema and table. Displayed on the console and saved on PostgreSQL Database
# *********************************************************************************************************************************************
def create_log_table(engine) -> None:
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

        CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
            id          SERIAL PRIMARY KEY,
            log_time    TIMESTAMPTZ,
            level       TEXT,
            logger_name TEXT,
            message     TEXT
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


# Writes each log record as a row in logging.extraction_log. Failures here are swallowed (via handleError) so a DB hiccup never crashes the run itself
class PostgresLogHandler(logging.Handler):

    def __init__(self, engine):
        super().__init__()
        self.engine = engine

    def emit(self, record):
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(f"""
                        INSERT INTO {LOG_SCHEMA}.{LOG_TABLE}
                            (log_time, level, logger_name, message)
                        VALUES
                            (:log_time, :level, :logger_name, :message)
                    """),
                    {
                        "log_time":    datetime.now(timezone.utc),
                        "level":       record.levelname,
                        "logger_name": record.name,
                        "message":     self.format(record),
                    },
                )
        except Exception:
            self.handleError(record)


create_log_table(ENGINE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

_pg_handler = PostgresLogHandler(ENGINE)
_pg_handler.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_pg_handler)  # attach to root -> captures all module loggers

logger = logging.getLogger(__name__)


# *********************************************************************************************************************************************
# 4 - Create silver schema and tables if they don't exist
# *********************************************************************************************************************************************
def create_silver_tables(engine) -> None:
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SILVER_TABLE} (
            recipe_id      INTEGER PRIMARY KEY,
            title          TEXT NOT NULL,
            calories_kcal  NUMERIC,
            protein_g      NUMERIC,
            fat_g          NUMERIC,
            carbs_g        NUMERIC,
            fiber_g        NUMERIC,
            sugar_g        NUMERIC,
            sodium_mg      NUMERIC,
            iron_mg        NUMERIC,
            updated_at     TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SILVER_CUISINES_TABLE} (
            recipe_id  INTEGER REFERENCES {SILVER_SCHEMA}.{SILVER_TABLE}(recipe_id) ON DELETE CASCADE,
            cuisine    TEXT,
            PRIMARY KEY (recipe_id, cuisine)
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SILVER_DIETS_TABLE} (
            recipe_id  INTEGER REFERENCES {SILVER_SCHEMA}.{SILVER_TABLE}(recipe_id) ON DELETE CASCADE,
            diet       TEXT,
            PRIMARY KEY (recipe_id, diet)
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SYNC_STATE_TABLE} (
            pipeline       TEXT PRIMARY KEY,
            last_loaded_at TIMESTAMPTZ
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info(f"Silver tables ready -> {SILVER_SCHEMA}.{SILVER_TABLE} (+cuisines/diets/sync_state)")


# *********************************************************************************************************************************************
# 5 - Watermark helpers: track how far into bronze.recipes (by loaded_at) silver has already processed
# *********************************************************************************************************************************************
def get_watermark(engine):
    with engine.connect() as conn:
        result = conn.execute(
            text(f"""
                SELECT last_loaded_at FROM {SILVER_SCHEMA}.{SYNC_STATE_TABLE}
                WHERE pipeline = :pipeline
            """),
            {"pipeline": PIPELINE_NAME},
        )
        row = result.fetchone()
        return row[0] if row else None


def set_watermark(engine, last_loaded_at) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SILVER_SCHEMA}.{SYNC_STATE_TABLE} (pipeline, last_loaded_at)
                VALUES (:pipeline, :last_loaded_at)
                ON CONFLICT (pipeline) DO UPDATE
                    SET last_loaded_at = EXCLUDED.last_loaded_at
            """),
            {"pipeline": PIPELINE_NAME, "last_loaded_at": last_loaded_at},
        )


# *********************************************************************************************************************************************
# 6 - Cleaning/validation helpers. Turns bronze's comma-separated text into a clean, de-duplicated, title-cased list.
# *********************************************************************************************************************************************
def normalize_tags(raw_text: str) -> list:
    if not raw_text:
        return []

    seen = []
    for tag in raw_text.split(","):
        cleaned = tag.strip().title()
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
    return seen


def round_numeric(value, ndigits=1):
    return round(value, ndigits) if value is not None else None


# Rows that fail these are skipped and logged, not written to silver.
def is_valid_row(row: dict) -> bool:
    if not row.get("title"):
        return False

    for field in ("calories_kcal", "protein_g", "fat_g", "carbs_g", "fiber_g", "sugar_g", "sodium_mg", "iron_mg"):
        value = row.get(field)
        if value is not None and value < 0:
            return False

    return True


# *********************************************************************************************************************************************
# 7 - Transform a bronze row into a silver row + its cuisine/diet tag lists
# *********************************************************************************************************************************************
def transform_row(bronze_row: dict) -> dict:
    return {
        "recipe_id":     bronze_row["recipe_id"],
        "title":         (bronze_row.get("title") or "").strip(),
        "calories_kcal": round_numeric(bronze_row.get("calories_kcal")),
        "protein_g":     round_numeric(bronze_row.get("protein_g")),
        "fat_g":         round_numeric(bronze_row.get("fat_g")),
        "carbs_g":       round_numeric(bronze_row.get("carbs_g")),
        "fiber_g":       round_numeric(bronze_row.get("fiber_g")),
        "sugar_g":       round_numeric(bronze_row.get("sugar_g")),
        "sodium_mg":     round_numeric(bronze_row.get("sodium_mg")),
        "iron_mg":       round_numeric(bronze_row.get("iron_mg")),
        "cuisines":      normalize_tags(bronze_row.get("cuisines")),
        "diets":         normalize_tags(bronze_row.get("diets")),
    }


# *********************************************************************************************************************************************
# 8 - Write a batch of transformed rows into silver.recipes + the cuisine/diet junction tables
# *********************************************************************************************************************************************
def write_silver(rows: list, engine) -> int:
    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    recipe_ids = [r["recipe_id"] for r in rows]

    recipe_params = [
        {
            "recipe_id":     r["recipe_id"],
            "title":         r["title"],
            "calories_kcal": r["calories_kcal"],
            "protein_g":     r["protein_g"],
            "fat_g":         r["fat_g"],
            "carbs_g":       r["carbs_g"],
            "fiber_g":       r["fiber_g"],
            "sugar_g":       r["sugar_g"],
            "sodium_mg":     r["sodium_mg"],
            "iron_mg":       r["iron_mg"],
            "updated_at":    now,
        }
        for r in rows
    ]

    cuisine_params = [
        {"recipe_id": r["recipe_id"], "cuisine": cuisine}
        for r in rows for cuisine in r["cuisines"]
    ]

    diet_params = [
        {"recipe_id": r["recipe_id"], "diet": diet}
        for r in rows for diet in r["diets"]
    ]

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SILVER_SCHEMA}.{SILVER_TABLE}
                    (recipe_id, title, calories_kcal, protein_g, fat_g, carbs_g,
                     fiber_g, sugar_g, sodium_mg, iron_mg, updated_at)
                VALUES
                    (:recipe_id, :title, :calories_kcal, :protein_g, :fat_g, :carbs_g,
                     :fiber_g, :sugar_g, :sodium_mg, :iron_mg, :updated_at)
                ON CONFLICT (recipe_id) DO UPDATE SET
                    title         = EXCLUDED.title,
                    calories_kcal = EXCLUDED.calories_kcal,
                    protein_g     = EXCLUDED.protein_g,
                    fat_g         = EXCLUDED.fat_g,
                    carbs_g       = EXCLUDED.carbs_g,
                    fiber_g       = EXCLUDED.fiber_g,
                    sugar_g       = EXCLUDED.sugar_g,
                    sodium_mg     = EXCLUDED.sodium_mg,
                    iron_mg       = EXCLUDED.iron_mg,
                    updated_at    = EXCLUDED.updated_at
            """),
            recipe_params,
        )

        # Tags are fully replaced per recipe rather than merged, so a recipe that lost a cuisine/diet tag in a later bronze pull doesn't end up with stale entries here.
        conn.execute(
            text(f"DELETE FROM {SILVER_SCHEMA}.{SILVER_CUISINES_TABLE} WHERE recipe_id = ANY(:ids)"),
            {"ids": recipe_ids},
        )
        conn.execute(
            text(f"DELETE FROM {SILVER_SCHEMA}.{SILVER_DIETS_TABLE} WHERE recipe_id = ANY(:ids)"),
            {"ids": recipe_ids},
        )

        if cuisine_params:
            conn.execute(
                text(f"""
                    INSERT INTO {SILVER_SCHEMA}.{SILVER_CUISINES_TABLE} (recipe_id, cuisine)
                    VALUES (:recipe_id, :cuisine)
                    ON CONFLICT DO NOTHING
                """),
                cuisine_params,
            )

        if diet_params:
            conn.execute(
                text(f"""
                    INSERT INTO {SILVER_SCHEMA}.{SILVER_DIETS_TABLE} (recipe_id, diet)
                    VALUES (:recipe_id, :diet)
                    ON CONFLICT DO NOTHING
                """),
                diet_params,
            )

    logger.info(f"Silver: upserted {len(rows)} recipes")
    return len(rows)


# *********************************************************************************************************************************************
# 9 - Read new/changed rows from bronze.recipes since the last watermark
# *********************************************************************************************************************************************
def read_bronze_batch(engine, watermark, limit=BATCH_SIZE) -> list:
    sql = f"""
        SELECT recipe_id, title, cuisines, diets,
               calories_kcal, protein_g, fat_g, carbs_g,
               fiber_g, sugar_g, sodium_mg, iron_mg, loaded_at
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}
        WHERE (:watermark IS NULL OR loaded_at > :watermark)
        ORDER BY loaded_at
        LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"watermark": watermark, "limit": limit})
        return [dict(row._mapping) for row in result]


# *********************************************************************************************************************************************
# 10 - Main transform. Pulls new bronze rows in batches, cleans them, and upserts into silver until caught up
# *********************************************************************************************************************************************
def transform() -> None:

    create_silver_tables(ENGINE)

    watermark = get_watermark(ENGINE)
    logger.info(f"Resuming from watermark: {watermark or 'beginning of bronze'}")

    total_processed = 0
    total_skipped = 0

    while True:
        bronze_rows = read_bronze_batch(ENGINE, watermark)

        if not bronze_rows:
            break

        valid_rows = []
        for bronze_row in bronze_rows:
            silver_row = transform_row(bronze_row)
            if is_valid_row(silver_row):
                valid_rows.append(silver_row)
            else:
                total_skipped += 1
                logger.warning(f"Skipped invalid recipe_id={bronze_row['recipe_id']} (failed validation)")

        write_silver(valid_rows, ENGINE)

        watermark = bronze_rows[-1]["loaded_at"]
        set_watermark(ENGINE, watermark)

        total_processed += len(bronze_rows)
        logger.info(f"Batch complete | processed={len(bronze_rows)} | watermark={watermark}")

        if len(bronze_rows) < BATCH_SIZE:  # last batch
            break

    logger.info(f"Transform complete: {total_processed} rows processed, {total_skipped} skipped, watermark={watermark}")


# Entry point for running standalone testing
if __name__ == "__main__":
    transform()