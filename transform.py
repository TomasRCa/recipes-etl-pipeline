import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# Paths
RAW_INPUT_PATH         = "/tmp/recipes_raw.parquet"
SILVER_OUTPUT_PATH     = "/tmp/recipes_silver.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION")


# *********************************************************************************************************************************************
# 1 - Renames all columns, standardizing to snake_case
# *********************************************************************************************************************************************

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        # Core recipe fields
        "id":                                        "recipe_id",
        "readyInMinutes":                            "ready_in_minutes",
        "preparationMinutes":                        "preparation_minutes",
        "cookingMinutes":                            "cooking_minutes",
        "pricePerServing":                           "price_per_serving",
        "healthScore":                               "health_score",
        "spoonacularScore":                          "spoonacular_score",
        "weightWatcherSmartPoints":                  "weight_watcher_points",
        "aggregateLikes":                            "aggregate_likes",
        "sourceName":                                "source_name",
        "sourceUrl":                                 "source_url",
        "spoonacularSourceUrl":                      "spoonacular_source_url",
        "creditsText":                               "credits_text",

        # Boolean flags
        "cheap":                                     "is_cheap",
        "sustainable":                               "is_sustainable",
        "vegan":                                     "is_vegan",
        "vegetarian":                                "is_vegetarian",
        "glutenFree":                                "is_gluten_free",
        "dairyFree":                                 "is_dairy_free",
        "veryHealthy":                               "is_very_healthy",
        "veryPopular":                               "is_very_popular",
        "ketogenic":                                 "is_ketogenic",
        "whole30":                                   "is_whole30",
        "lowFodmap":                                 "is_low_fodmap",

        # List columns 
        "dishTypes":                                 "dish_types",
        "extendedIngredients":                       "extended_ingredients",
        "analyzedInstructions":                      "analyzed_instructions",

        # Nested dict columns (flattened by json_normalize)
        "nutrition_nutrients":                       "nutrients",
        "nutrition_ingredients":                     "ingredients",
        "nutrition_caloricBreakdown_percentProtein": "pct_protein",
        "nutrition_caloricBreakdown_percentFat":     "pct_fat",
        "nutrition_caloricBreakdown_percentCarbs":   "pct_carbs",
        "nutrition_weightPerServing_amount":         "weight_per_serving_g",
        "nutrition_weightPerServing_unit":           "weight_per_serving_unit"
    }

    # Only rename columns that actually exist in the DataFrame
    existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing_renames)

    logger.info(f"Renamed {len(existing_renames)} columns to snake_case")
    return df


# *********************************************************************************************************************************************
# 2 - Drop redundant columns or columns that won't be used for analytics
# *********************************************************************************************************************************************

def drop_useless_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [
        "image",
        "imageType",
        "spoonacularSourceUrl",         # redundant with source_url
        "creditsText",                  # usually same as source_name
        "gaps",                         # Spoonacular internal field, not useful
        "license",                      # rarely populated
    ]

    existing_drops = [c for c in cols_to_drop if c in df.columns]
    df = df.drop(columns=existing_drops)

    logger.info(f"Dropped {len(existing_drops)} useless columns")
    return df


# *********************************************************************************************************************************************
# 3 - Enforcing data types. List/Dict columns are untouched and will be dealt with on the gold layer
# *********************************************************************************************************************************************

def fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    # Integer columns
    int_cols = ["recipe_id", "servings", "ready_in_minutes",
                "preparation_minutes", "cooking_minutes",
                "weight_watcher_points", "aggregate_likes"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Float columns
    float_cols = ["price_per_serving", "health_score", "spoonacular_score",
                  "pct_protein", "pct_fat", "pct_carbs", "weight_per_serving_g"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Boolean columns — Spoonacular returns True/False but
    # json_normalize can sometimes convert them to strings or objects
    bool_cols = ["is_cheap", "is_sustainable", "is_vegan", "is_vegetarian",
                 "is_gluten_free", "is_dairy_free", "is_very_healthy",
                 "is_very_popular", "is_ketogenic", "is_whole30", "is_low_fodmap"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)

    # Datetime columns
    if "extracted_at" in df.columns:
        df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce", utc=True)

    logger.info("Data types enforced")
    return df


# *********************************************************************************************************************************************
# 4 - Handle nulls. Replaces empty strings with NONE, and empty lists with NONE
# *********************************************************************************************************************************************

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:

    string_cols = ["title", "summary", "instructions_raw", "source_name",
                   "source_url", "credits_text", "cuisine_filter",
                   "type_filter", "diet_filter", "weight_per_serving_unit"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].replace("", None)
            df[col] = df[col].where(df[col].notna(), None)

    list_cols = ["cuisines", "dish_types", "diets", "occasions",
                 "extended_ingredients", "analyzed_instructions",
                 "nutrition_nutrients", "nutrition_ingredients"]
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x if isinstance(x, list) else []
            )

    logger.info("Null handling complete")
    return df


# *********************************************************************************************************************************************
# 5 - Standardize string values. Strip empty spaces, standardize value names to lower case
# *********************************************************************************************************************************************

def standardize_strings(df: pd.DataFrame) -> pd.DataFrame:

    if "title" in df.columns:
        df["title"] = df["title"].str.strip()


    for col in ["cuisine_filter", "type_filter", "diet_filter"]:
        if col in df.columns:
            df[col] = df[col].str.lower().str.strip()


    list_cols = ["cuisines", "dish_types", "diets", "occasions"]
    for col in list_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda lst: [s.lower().strip() for s in lst if isinstance(s, str)]
                if isinstance(lst, list) else []
            )

    logger.info("String standardization complete")
    return df


# *********************************************************************************************************************************************
# 6 - Deduplicate. Removing duplicates by recipe_id, keeping the most recently extracted row
# *********************************************************************************************************************************************

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:

    before = len(df)

    if "extracted_at" in df.columns:
        df = df.sort_values("extracted_at", ascending=False)

    df = df.drop_duplicates(subset=["recipe_id"], keep="first")

    after = len(df)
    logger.info(f"Deduplication: {before} → {after} rows ({before - after} removed)")
    return df


# *********************************************************************************************************************************************
# 7 - Add metadata audit column
# *********************************************************************************************************************************************

def add_audit_columns(df: pd.DataFrame) -> pd.DataFrame:

    df["transformed_at"] = pd.to_datetime(df["transformed_at"])
    df["pipeline_version"] = "1.0.0"   # Change this value only when the code is altered

    logger.info("Audit column added")
    return df


# *********************************************************************************************************************************************
# Main transformation function. Applies all the previously built transformations
# *********************************************************************************************************************************************

def transform(df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Silver layer transform. Reads Bronze parquet, applies all
    cleaning steps, returns a clean Silver DataFrame.
    """
    if df is None:
        logger.info(f"Reading Bronze data from {RAW_INPUT_PATH}")
        df = pd.read_parquet(RAW_INPUT_PATH)

    logger.info(f"Silver transform started — {len(df)} raw rows, {len(df.columns)} columns")

    df = rename_columns(df)
    df = drop_useless_columns(df)
    df = fix_dtypes(df)
    df = handle_nulls(df)
    df = standardize_strings(df)
    df = deduplicate(df)
    df = add_audit_columns(df)

    logger.info(f"Silver transform complete — {len(df)} rows, {len(df.columns)} columns")
    return df


# Entry point for running standalone testing

if __name__ == "__main__":
    df_silver = transform()

    if not df_silver.empty:
        df_silver.to_parquet(SILVER_OUTPUT_PATH, index=False)
        print(f"\nSaved {len(df_silver)} rows to {SILVER_OUTPUT_PATH}")
        print(f"Columns ({len(df_silver.columns)}):")
        for col in df_silver.columns:
            print(f"  {col:45} {str(df_silver[col].dtype)}")