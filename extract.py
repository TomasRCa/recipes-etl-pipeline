import requests
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

# *********************************************************************************************************************************************
# 1 - Config
# *********************************************************************************************************************************************
load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.spoonacular.com/recipes/complexSearch"

DB_HOST     = os.getenv("POSTGRES_HOST")
DB_PORT     = os.getenv("POSTGRES_PORT")
DB_NAME     = os.getenv("POSTGRES_DB")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "recipes"

LOG_SCHEMA = "logging"
LOG_TABLE = "extraction_log"

if not API_KEY:
    raise EnvironmentError("API_KEY not found")

_missing = [k for k, v in {
    "POSTGRES_HOST": DB_HOST,
    "POSTGRES_PORT": DB_PORT,
    "POSTGRES_DB": DB_NAME,
    "POSTGRES_USER": DB_USER,
    "POSTGRES_PASSWORD": DB_PASSWORD,
}.items() if not v]

if _missing:
    raise EnvironmentError(f"Missing environment variables: {', '.join(_missing)}")

CUISINES = [
    "African", "Asian", "American", "British", "Cajun",
    "Caribbean", "Chinese", "Eastern European", "European",
    "French", "German", "Greek", "Indian", "Irish",
    "Italian", "Japanese", "Jewish", "Korean", "Latin American",
    "Mediterranean", "Mexican", "Middle Eastern", "Nordic",
    "Southern", "Spanish", "Thai", "Vietnamese"
]

DIETS = [
    "gluten free", "ketogenic", "vegetarian",
    "vegan", "pescetarian", "paleo", "whole30"
]

REQUEST_DELAY = 0.5   # Seconds between requests
MAX_RETRIES = 3        # Retries on failure
SESSION = requests.Session() 


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
# 3 - Create Logging Schema and tables. Displayed on the console and saved on PostgreSQL Database
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


# Writes each log record as a row in logging.extraction_log. Failures here are swallowed (via handleError) so a DB hiccup never crashes the extraction run itself
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
# 4 - Create bronze schema and table if they don't exist 
# *********************************************************************************************************************************************
def create_bronze_table(engine) -> None:
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA};

        CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.{BRONZE_TABLE}(
            recipe_id            INTEGER PRIMARY KEY,
            title                TEXT,
            cuisines             TEXT,
            diets                TEXT,
            calories_kcal        NUMERIC,
            protein_g            NUMERIC,
            fat_g                NUMERIC,
            carbs_g              NUMERIC,
            fiber_g              NUMERIC,
            sugar_g              NUMERIC,
            sodium_mg            NUMERIC,
            iron_mg              NUMERIC,
            cuisine_filter       TEXT,
            extracted_at         TIMESTAMPTZ,
            loaded_at            TIMESTAMPTZ
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info(f"Bronze table ready -> {BRONZE_SCHEMA}.{BRONZE_TABLE}")


# *********************************************************************************************************************************************
# 5 - Gets recipe IDs already stored in the bronze layer, avoiding duplicated ingestions, and batch number logic
# *********************************************************************************************************************************************
def get_existing_bronze_ids(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT recipe_id FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}")
        )
        return set(row[0] for row in result)
    

# *********************************************************************************************************************************************
# 6 - Pull only the target columns out of a raw recipe object returned by the API
# *********************************************************************************************************************************************
def get_nutrient(recipe: dict, name: str):
    nutrients = recipe.get("nutrition", {}).get("nutrients", [])

    for nutrient in nutrients:
        if nutrient.get("name", "").lower() == name.lower():
            return nutrient.get("amount")

    return None


def extract_recipe_row(recipe: dict, cuisine_filter, extracted_at: datetime, batch_id: int) -> dict:
    cuisines = recipe.get("cuisines") or []
    diets = recipe.get("diets") or []

    return {
        "recipe_id":           recipe["id"],
        "title":               recipe.get("title"),
        "cuisines":            ", ".join(cuisines) if cuisines else None,
        "diets":               ", ".join(diets) if diets else None,
        "calories_kcal":       get_nutrient(recipe, "Calories"),
        "protein_g":           get_nutrient(recipe, "Protein"),
        "fat_g":               get_nutrient(recipe, "Fat"),
        "carbs_g":             get_nutrient(recipe, "Carbohydrates"),
        "fiber_g":             get_nutrient(recipe, "Fiber"),
        "sugar_g":             get_nutrient(recipe, "Sugar"),
        "sodium_mg":           get_nutrient(recipe, "Sodium"),
        "iron_mg":             get_nutrient(recipe, "Iron"),
        "cuisine_filter":      cuisine_filter,
        "batch_id":            batch_id,
        "extracted_at":        extracted_at,
        "loaded_at":           datetime.now(timezone.utc)
    }


# *********************************************************************************************************************************************
# 7 - Write a batch of extracted rows into the bronze layer. Returns the number of rows inserted (duplicates skipped)
# *********************************************************************************************************************************************
def write_bronze(rows: list, engine) -> int:
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {BRONZE_SCHEMA}.{BRONZE_TABLE}
            (recipe_id, title, cuisines, diets,
             calories_kcal, protein_g, fat_g, carbs_g, fiber_g, sugar_g, sodium_mg, iron_mg,
             cuisine_filter, extracted_at, loaded_at)
        VALUES(
            :recipe_id, :title, :cuisines, :diets,
            :calories_kcal, :protein_g, :fat_g, :carbs_g,
            :fiber_g, :sugar_g, :sodium_mg, :iron_mg,
            :cuisine_filter,
            :extracted_at, :loaded_at
        )
        ON CONFLICT (recipe_id) DO NOTHING
    """

    with engine.begin() as conn:
        conn.execute(text(sql), rows)

    logger.info(f"Bronze: inserted {len(rows)} new rows")
    return len(rows)


# *********************************************************************************************************************************************
# 8 - API call with safeguards
# *********************************************************************************************************************************************
def fetch_page(cuisine=None, diet=None, offset=0):
    params = {
        "apiKey": API_KEY,
        "number": 100,
        "offset": offset,
        "addRecipeInformation": True,
        "addRecipeNutrition": True
    }
    if cuisine: params["cuisine"] = cuisine
    if diet: params["diet"] = diet

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 402:
                logger.warning("Daily quota exceeded.")
                return None

            if response.status_code == 429:
                logger.warning(f"Rate limit exceeded. Attempt {attempt}/{MAX_RETRIES}")
                time.sleep(2 ** attempt)
                continue

            response.raise_for_status()

            data = response.json()

            if "code" in data and data["code"] != 200:
                logger.warning(f"API error in response: {data.get('message')}")
                return None

            return data

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt}/{MAX_RETRIES}")

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error on attempt {attempt}/{MAX_RETRIES}: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)

    logger.error(f"All {MAX_RETRIES} attempts failed for offset={offset}")
    return None


# *********************************************************************************************************************************************
# 9 - Pull all pages for a given filter, extract the target columns, and write them to the bronze layer as they arrive
# *********************************************************************************************************************************************
def pull_axis(seen_ids, engine, cuisine=None, diet=None) -> int:

    label         = cuisine or diet or "no-filter"
    total_written = 0

    for offset in range(0, 1000, 100):
        data = fetch_page(cuisine=cuisine, diet=diet, offset=offset)

        if data is None:  # Quota hit or unrecoverable error
            logger.warning(f"Stopping pagination for '{label}' at offset {offset}")
            break

        results = data.get("results", [])

        if not results:
            break

        extracted_at = datetime.now(timezone.utc)
        new_rows = []
        for recipe in results:
            if recipe["id"] not in seen_ids:
                seen_ids.add(recipe["id"])
                new_rows.append(
                    extract_recipe_row(recipe, cuisine, extracted_at)
                )

        written = write_bronze(new_rows, engine)
        total_written += written

        logger.info(
            f"[{label}] offset={offset} | "
            f"page_results={len(results)} | "
            f"new_written={written}"
        )

        time.sleep(REQUEST_DELAY)

        if len(results) < 100:  # last page
            break

    return total_written


# *********************************************************************************************************************************************
# 10 - Main extraction. Extracts recipes across cuisine and diet
# *********************************************************************************************************************************************
def extract(cuisines=CUISINES, diets=None) -> None:

    create_bronze_table(ENGINE)

    # Seed seen_ids from existing Bronze records to avoid re-inserting on reruns
    seen_ids = get_existing_bronze_ids(ENGINE)
    logger.info(f"Resuming from {len(seen_ids)} already stored recipes")

    total_written = 0

    # Pull by cuisine
    if cuisines:
        for cuisine in cuisines:
            logger.info(f"Extracting cuisine: {cuisine}")
            written = pull_axis(seen_ids, ENGINE, cuisine=cuisine)
            total_written += written
            logger.info(f"Total written so far: {total_written}")

    # Pull by diet
    if diets:
        for diet in diets:
            logger.info(f"Extracting diet: {diet}")
            written = pull_axis(seen_ids, ENGINE, diet=diet)
            total_written += written
            logger.info(f"Total written so far: {total_written}")

    logger.info(f"Extraction complete: {total_written} new recipes written to Bronze")


# Standalone testing
if __name__ == "__main__":
    try:
        extract(
            cuisines=CUISINES,
            diets=DIETS
        )
    finally:
        SESSION.close()