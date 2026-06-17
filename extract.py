import requests
import pandas as pd
import time
import logging
from datetime import datetime
import json
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
BRONZE_TABLE = "recipes_raw"

if not API_KEY:
    raise EnvironmentError("API_KEY not found")

_missing = [k for k, v in {
    "POSTGRES_DB":       DB_NAME,
    "POSTGRES_USER":     DB_USER,
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

MEAL_TYPES = [
    "main course", "side dish", "dessert", "appetizer",
    "salad", "bread", "breakfast", "soup", "snack"
]

DIETS = [
    "gluten free", "ketogenic", "vegetarian",
    "vegan", "pescetarian", "paleo", "whole30"
]

REQUEST_DELAY = 0.5   # Seconds between requests
MAX_RETRIES = 3        # Retries on failure


# *********************************************************************************************************************************************
# 2 - Logging
# *********************************************************************************************************************************************
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# *********************************************************************************************************************************************
# 3 - Engine to connect to the PostgreSQL Database
# *********************************************************************************************************************************************
def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(url, pool_pre_ping=True)


# *********************************************************************************************************************************************
# 4 - Create Bronze schema and table if they don't exist
# *********************************************************************************************************************************************
def create_bronze_table(engine) -> None:
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA};

        CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.{BRONZE_TABLE} (
            recipe_id       INTEGER,
            raw_json        TEXT,
            cuisine_filter  TEXT,
            type_filter     TEXT,
            diet_filter     TEXT,
            extracted_at    TIMESTAMPTZ,
            loaded_at       TIMESTAMPTZ
        );
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    logger.info(f"Bronze table ready → {BRONZE_SCHEMA}.{BRONZE_TABLE}")



# *********************************************************************************************************************************************
# 5 - Gets recipe IDs already stored in the bronze layer, avoiding duplicated ingestions
# *********************************************************************************************************************************************
def get_existing_bronze_ids(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT recipe_id FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}")
        )
        return set(row[0] for row in result)
    


# *********************************************************************************************************************************************
# 6 - Write a batch of recipes directly into the bronze layer. 
#     Serializes each recipe as a JSON string and inserts only new IDs. Returns the number of rows inserted
# *********************************************************************************************************************************************
def write_bronze(recipes: list, engine) -> int:

    if not recipes:
        return 0

    # No duplicate check needed here — pull_axis() already filters seen_ids
    rows = []
    for recipe in recipes:
        rows.append({
            "recipe_id":      recipe["id"],
            "raw_json":       json.dumps(recipe, default=str),
            "cuisine_filter": recipe.get("_cuisine_filter"),
            "type_filter":    recipe.get("_type_filter"),
            "diet_filter":    recipe.get("_diet_filter"),
            "extracted_at":   recipe.get("_extracted_at"),
            "loaded_at":      datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(rows)
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce", utc=True)
    df["loaded_at"]    = pd.to_datetime(df["loaded_at"],    errors="coerce", utc=True)

    df.to_sql(
        name=BRONZE_TABLE,
        con=engine,
        schema=BRONZE_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500
    )

    logger.info(f"Bronze: inserted {len(rows)} new rows")
    return len(rows)



# *********************************************************************************************************************************************
# 7 - API call with safeguards
# *********************************************************************************************************************************************
def fetch_page(cuisine=None, meal_type=None, diet=None, offset=0):
    params = {
        "apiKey": API_KEY,
        "number": 100,
        "offset": offset,
        "addRecipeInformation": True,
        "addRecipeNutrition": True,
        "addRecipeInstructions": True,
    }
    if cuisine: params["cuisine"] = cuisine
    if meal_type: params["type"] = meal_type
    if diet: params["diet"] = diet

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 402:
                logger.warning("Daily quota exceeded.")
                return None

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
            time.sleep(2 ** attempt)  # exponential cooldown

    logger.error(f"All {MAX_RETRIES} attempts failed for offset={offset}")
    return None



# *********************************************************************************************************************************************
# 8 - Pull all pages for a given filter and write them to the bronze layer as they arrive. Returns the number of new rows written
# *********************************************************************************************************************************************
def pull_axis(seen_ids, engine, cuisine=None, meal_type=None, diet=None) -> int:

    label        = cuisine or meal_type or diet or "no-filter"
    total_written = 0

    for offset in range(0, 1000, 100):
        data = fetch_page(cuisine=cuisine, meal_type=meal_type, diet=diet, offset=offset)

        if data is None:  # Quota hit or unrecoverable error
            logger.warning(f"Stopping pagination for '{label}' at offset {offset}")
            break

        results = data.get("results", [])

        if not results:
            break

        new_recipes = []
        for recipe in results:
            if recipe["id"] not in seen_ids:
                seen_ids.add(recipe["id"])
                recipe["_cuisine_filter"] = cuisine
                recipe["_type_filter"]    = meal_type
                recipe["_diet_filter"]    = diet
                recipe["_extracted_at"]   = datetime.utcnow().isoformat()
                new_recipes.append(recipe)

        # Write this page onto the bronze layer
        written = write_bronze(new_recipes, engine)
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
# 9 - Main extraction. Extracts recipes across cuisine, meal type and diet
# *********************************************************************************************************************************************
def extract(cuisines=CUISINES, meal_types=None, diets=None) -> None:

    engine   = get_engine()

    create_bronze_table(engine)

    # Seed seen_ids from existing Bronze records to avoid re-inserting on reruns
    seen_ids = get_existing_bronze_ids(engine)
    logger.info(f"Resuming from {len(seen_ids)} already stored recipes")

    total_written = 0

    # Pull by cuisine
    if cuisines:
        for cuisine in cuisines:
            logger.info(f"Extracting cuisine: {cuisine}")
            written = pull_axis(seen_ids, engine, cuisine=cuisine)
            total_written += written
            logger.info(f"Total written so far: {total_written}")

    # Pull by meal type
    if meal_types:
        for meal_type in meal_types:
            logger.info(f"Extracting meal type: {meal_type}")
            written = pull_axis(seen_ids, engine, meal_type=meal_type)
            total_written += written
            logger.info(f"Total written so far: {total_written}")

    # Pull by diet
    if diets:
        for diet in diets:
            logger.info(f"Extracting diet: {diet}")
            written = pull_axis(seen_ids, engine, diet=diet)
            total_written += written
            logger.info(f"Total written so far: {total_written}")

    logger.info(f"Extraction complete: {total_written} new recipes written to Bronze")



# Entry point for running standalone testing
if __name__ == "__main__":
    extract(
        cuisines=["Italian", "Mexican", "Asian"],
        meal_types=None,
        diets=None
    )
