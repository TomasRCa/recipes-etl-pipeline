import requests
import pandas as pd
import time
import logging
from datetime import datetime
import fastparquet
from dotenv import load_dotenv
import os

# *********************************************************************************************************************************************
# 1 - Config
# *********************************************************************************************************************************************
load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.spoonacular.com/recipes/complexSearch"

if not API_KEY:
    raise EnvironmentError("API_KEY not found")

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
# 3 - API call with safeguards
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
# 4 - Pull all pages
# *********************************************************************************************************************************************
def pull_axis(seen_ids, cuisine=None, meal_type=None, diet=None) -> list:
    new_recipes = []
    label = cuisine or meal_type or diet or "no-filter"

    for offset in range(0, 1000, 100):
        data = fetch_page(cuisine=cuisine, meal_type=meal_type, diet=diet, offset=offset)

        if data is None:  # Quota hit or unrecoverable error
            logger.warning(f"Stopping pagination for '{label}' at offset {offset}")
            break

        results = data.get("results", [])

        if not results:
            break

        for recipe in results:
            if recipe["id"] not in seen_ids:
                seen_ids.add(recipe["id"]) # Tag each recipe with extraction metadata
                recipe["_cuisine_filter"] = cuisine
                recipe["_type_filter"] = meal_type
                recipe["_diet_filter"] = diet
                recipe["_extracted_at"] = datetime.utcnow().isoformat()
                new_recipes.append(recipe)

        logger.info(
            f"[{label}] offset={offset} | "
            f"page_results={len(results)} | "
            f"new_unique={len(new_recipes)}"
        )

        time.sleep(REQUEST_DELAY)

        if len(results) < 100:  # last page
            break

    return new_recipes



# *********************************************************************************************************************************************
# 5 - Main extraction. Extracts recipes across cuisine, meal type and diet, returning a Dataframe and exporting a the data in JSON format
# *********************************************************************************************************************************************
def extract(cuisines=CUISINES, meal_types=None, diets=None) -> pd.DataFrame:

    all_recipes = []
    seen_ids = set()

    # Pull by cuisine
    if cuisines:
        for cuisine in cuisines:
            logger.info(f"Extracting cuisine: {cuisine}")
            batch = pull_axis(seen_ids, cuisine=cuisine)
            all_recipes.extend(batch)
            logger.info(f"Total unique recipes so far: {len(all_recipes)}")

    # Pull by meal type
    if meal_types:
        for meal_type in meal_types:
            logger.info(f"Extracting meal type: {meal_type}")
            batch = pull_axis(seen_ids, meal_type=meal_type)
            all_recipes.extend(batch)
            logger.info(f"Total unique recipes so far: {len(all_recipes)}")

    # Pull by diet
    if diets:
        for diet in diets:
            logger.info(f"Extracting diet: {diet}")
            batch = pull_axis(seen_ids, diet=diet)
            all_recipes.extend(batch)
            logger.info(f"Total unique recipes so far: {len(all_recipes)}")

    if not all_recipes:
        logger.warning("No recipes extracted.")
        return pd.DataFrame()

    df = pd.json_normalize(all_recipes, sep="_")

    logger.info(f"Extraction complete: {len(df)} recipes, {len(df.columns)} columns")
    return df


# Entry point for running standalone testing
if __name__ == "__main__":
    df = extract(
        cuisines=["Italian", "Mexican", "Asian"],
        meal_types=None,
        diets=None
    )

    if not df.empty:
        df.to_parquet("recipes_raw.parquet", index=False)
        print(f"\nSaved {len(df)} recipes")
        print(f"Columns ({len(df.columns)}): {df.columns.tolist()}")