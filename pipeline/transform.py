import json
import logging
import re
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
SYNC_STATE_TABLE = "sync_state"
PIPELINE_NAME = "bronze_to_silver_recipes"

LOG_SCHEMA = "logging"
LOG_TABLE = "transform_log"

BATCH_SIZE = 500  # rows pulled from bronze per cycle

# Maps every cuisine tag in CUISINES to a region. None means no clean mapping exists.
CUISINE_REGION_MAP = {
    "African":          "Africa",
    "Asian":            "Asia",
    "American":         "North America",
    "British":          "Europe",
    "Cajun":            "North America",
    "Caribbean":        "Caribbean",
    "Chinese":          "Asia",
    "Eastern European": "Europe",
    "European":         "Europe",
    "French":           "Mediterranean",
    "German":           "Europe",
    "Greek":            "Mediterranean",
    "Indian":           "Asia",
    "Irish":            "Europe",
    "Italian":          "Mediterranean",
    "Japanese":         "Asia",
    "Jewish":           None,
    "Korean":           "Asia",
    "Latin American":   "Latin America",
    "Mediterranean":    "Mediterranean",
    "Mexican":          "Latin America",
    "Middle Eastern":   "Middle East",
    "Nordic":           "Europe",
    "Southern":         "North America",
    "Spanish":          "Mediterranean",
    "Thai":             "Asia",
    "Vietnamese":       "Asia",
}

# Ingredient names longer than this are almost certainly descriptions, not ingredient names
INGREDIENT_MAX_NAME_LENGTH = 60

# SUFFIX noise: when the pattern is found, everything from that point to the end of the string is noise. The part before the pattern is kept as the ingredient name
# e.g. "black bean garlic sauce from a jar" -> "black bean garlic sauce"
STRIP_FROM_MATCH = [
    " from a jar",
    " if desired",
    " if needed",
    " can be subbed",
    " can be used",
    " depending on",
    " soaked in",
    " to cook in",
    " make these",
    " dissolved into",
    " shopping list",
    " is ready",
    " -or",
    "-4 to 5",
    "-tear into pieces ",
    " plus extra 2-3 tablespoons",
    "-taste as you go to adjust flavors",
    "-minced",
    "& cut to bite size wedges",
    " plus 1 tablespoon",
    " plus 1 rounded tablespoon",
    " -shredded",
    " pieces",
    " thinly 2 • hamburger buns",
    "-very few",
    " to garnish",
    ") leaves",
    " torn into bite-sized pieces",
    " to your taste",
    " chunks",
    " above",
    " nos",
    " if you prefer a spicier one",
    " : or",
    " or",
    " to serve",
    "to serve",
    "block extra",
    "then add",
    ".5 ",
    "% ",
    "-6 to",
    " : 1/ lemon",
    "—the best you can afford",
    " into ¼",
    " as required",
    ". thinly",
    " of frying",
    " long 2 inch",
    " rinsed/drained",
    " 2 inch long",
    " : 1/ soaked",
    " tsp",
    " 2 honey",
    " -tear into",
    " from the freezer"
    "-drained",
    "-cut into florets",
    "-5 to"
]

# PREFIX noise: when the name starts with the pattern, the pattern itself is noise and the actual ingredient is what follows. Keeps everything after these pattern.
# e.g. "ea. eggs" -> "eggs"
STRIP_PREFIXES = [
    "pre-washed bagged ",
    "pre-washed ",
    "approximately of turbonado ",
    "cooking spoon of ",
    "centimeters piece of ",
    "cm piece of ",
    "mediums sized ",
    "you favorite ",
    "favorite ",
    "real salt ",
    "amounts of ",
    "pack of ",
    "teaspoons ",
    "spices: ",
    "plus 1 ",
    "each: ",
    "ea. ",
    "ea ",
    "drop ",
    "additional ",
    "-to cover",
    "pd of ",
    "so of ",
    "firmly-packed ",
    ".2 lb ",
    "in piece of ",
    "an ",
    "ez peel ",
    "cans of ",
    "approx. ",
    "if non-vegetarian: add ",
    "drops of ",
    "pounded ",
    "clean ",
    "thinly",
    "thumb-size piece ",
    "some "
]

# Substrings where the entire entry is dropped — used when the prefix before the pattern is also likely to be invalid (not just a noise suffix)
REJECT_SUBSTRINGS = [
    "and cook", 
    "recipe below",
    "recipe follows",
    "heat a",
    "crossing over quintessential american desserts",
    "if you don't eat bread",
    "ghee required",
    "loaves ready-made naan",
    "to assemble the burgers",
    "remove all extra fat",
    "sauce a",
    "seasonings",
    "do you love everything bagels as much as i do",
    "prepare a",
    "add meat and fry",
    "torn into",
    "fry onion till golden brown",
    "freshly short-grain",
    "drain off the bacon fat from the skillet",
    "juice of",
    "assemble the burgers on the bagels"
]

# Prefixes that identify instructions, articles, prepositions, or editorial text.
REJECT_START_PATTERNS = [
    "fyi", "note:", "tip:", "p.s",
    "you can", "you may", "you'll", "into ", "now ", "serve",
    "the ", "of ", "several ",
    "oz. ",
    "delicious ", "vegetarian option"
]

# Unit abbreviations that, when followed by a space at the START of a name, indicate a measurement was mistakenly included ("tsp pepper", "g sugar")
# e.g. "g sugar"
UNIT_PREFIXES = [
    "tsp ", "tbsp ", "cup ", "cups ",
    "oz ", "g ", "ml ", "lb ", "lbs ",
    "teaspoon ", "tablespoon ", "gram ",
]

# Maps raw API unit strings to canonical abbreviations
UNIT_NORMALIZATION = {
    "T": "tbsp", "Tbsp": "tbsp", "Tbsps": "tbsp",
    "tablespoon": "tbsp", "tablespoons": "tbsp", "tb": "tbsp",
    "t": "tsp", "teaspoon": "tsp", "teaspoons": "tsp",

    "c": "cups", "C": "cups", "cup": "cups",

    "gram": "g", "grams": "g",
    "kilogram": "kg", "kilograms": "kg",
    "ounce": "oz", "ounces": "oz",
    "pound": "lb", "pounds": "lb", "lbs": "lb",

    "milliliter": "ml", "milliliters": "ml", "millilitre": "ml", "millilitres": "ml", "mL": "ml",
    "liter": "l", "liters": "l", "litre": "l", "litres": "l", "L": "l",
    "fluid ounce": "fl oz", "fluid ounces": "fl oz",
    "pint": "pt", "pints": "pt",
    "quart": "qt", "quarts": "qt",
    "gallon": "gal", "gallons": "gal",
}

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


# Writes each log record as a row in logging.transform_log. Failures here are swallowed (via handleError) so a DB hiccup never crashes the run itself
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
# 4 - Create silver schema and tables if they don't exist, then seed dim_cuisines from CUISINE_REGION_MAP
# *********************************************************************************************************************************************
def create_silver_tables(engine) -> None:
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};

        -- Core recipe table. instructions_text holds all steps concatenated in order.
        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SILVER_TABLE} (
            recipe_id         INTEGER PRIMARY KEY,
            title             TEXT NOT NULL,
            instructions_text TEXT,
            calories_kcal     NUMERIC,
            protein_g         NUMERIC,
            fat_g             NUMERIC,
            carbs_g           NUMERIC,
            fiber_g           NUMERIC,
            sugar_g           NUMERIC,
            sodium_mg         NUMERIC,
            iron_mg           NUMERIC,
            updated_at        TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.dim_cuisines (
            cuisine_id   SERIAL PRIMARY KEY,
            cuisine_name TEXT NOT NULL UNIQUE,
            region       TEXT
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.recipe_cuisines (
            recipe_id  INTEGER REFERENCES {SILVER_SCHEMA}.{SILVER_TABLE}(recipe_id) ON DELETE CASCADE,
            cuisine_id INTEGER REFERENCES {SILVER_SCHEMA}.dim_cuisines(cuisine_id),
            PRIMARY KEY (recipe_id, cuisine_id)
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.dim_diets (
            diet_id   SERIAL PRIMARY KEY,
            diet_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.recipe_diets (
            recipe_id INTEGER REFERENCES {SILVER_SCHEMA}.{SILVER_TABLE}(recipe_id) ON DELETE CASCADE,
            diet_id   INTEGER REFERENCES {SILVER_SCHEMA}.dim_diets(diet_id),
            PRIMARY KEY (recipe_id, diet_id)
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.dim_ingredients (
            ingredient_id   SERIAL PRIMARY KEY,
            ingredient_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.recipe_ingredients (
            recipe_id     INTEGER REFERENCES {SILVER_SCHEMA}.{SILVER_TABLE}(recipe_id) ON DELETE CASCADE,
            ingredient_id INTEGER REFERENCES {SILVER_SCHEMA}.dim_ingredients(ingredient_id),
            amount        NUMERIC,
            unit          TEXT,
            PRIMARY KEY (recipe_id, ingredient_id)
        );

        -- Watermark table: tracks how far silver has processed bronze (by loaded_at)
        CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SYNC_STATE_TABLE} (
            pipeline       TEXT PRIMARY KEY,
            last_loaded_at TIMESTAMPTZ
        );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    _seed_dim_cuisines(engine)
    logger.info(f"Silver tables ready -> {SILVER_SCHEMA}.{SILVER_TABLE} (+dim/junction tables)")


# Pre-populates dim_cuisines from CUISINE_REGION_MAP. Safe to re-run: (ON CONFLICT DO NOTHING)
def _seed_dim_cuisines(engine) -> None:
    rows = [
        {"cuisine_name": name, "region": region}
        for name, region in CUISINE_REGION_MAP.items()
    ]
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SILVER_SCHEMA}.dim_cuisines (cuisine_name, region)
                VALUES (:cuisine_name, :region)
                ON CONFLICT (cuisine_name) DO NOTHING
            """),
            rows,
        )


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
# 6 - Cleaning and validation helpers
# *********************************************************************************************************************************************

#  JSONB columns come back from psycopg2 as Python objects already. This guard handles any edge case where they arrive as a raw string instead
def parse_json_field(value):
    if value is None:
        return []
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return []
    return value


# Turns bronze's comma-separated text into a de-duplicated list. Uses title case by default (for cuisines); pass lowercase=True for diets
def normalize_tags(raw_text: str, lowercase: bool = False) -> list:
    if not raw_text:
        return []
    seen = []
    for tag in raw_text.split(","):
        cleaned = tag.strip().lower() if lowercase else tag.strip().title()
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
    return seen

# Normalizes a raw API unit string to a canonical abbreviation. Applied on the raw (pre-lowercase) string so that cooking notation is preserved:
# lowercase 't' = teaspoon, uppercase 'T' = tablespoon.
def normalize_unit(raw_unit: str) -> str | None:
    if not raw_unit:
        return None
    stripped = raw_unit.strip()
    if not stripped:
        return None
    if stripped in UNIT_NORMALIZATION:
        return UNIT_NORMALIZATION[stripped]
    # Case-insensitive fallback
    lower = stripped.lower()
    return UNIT_NORMALIZATION.get(lower, lower)


# Cleans a nameClean value from extendedIngredients.
# Applies reject checks first (cheap string ops before regex), strip operations next to salvage partial names, then substring reject checks on the cleaned
#    string. Two layers work together: the id != 0 gate in extract_ingredients removes unrecognized tokens; this function handles noise that slips through
#    even for recognized ingredients.

#    Known limitations -> no clean structural signal to filter without false positives:
#      - Descriptive combos like "salt and a nice grind of pepper" pass through
#      - Valid compound ingredients like "peas and carrots" correctly pass through
def clean_ingredient_name(raw_name: str) -> str | None:
    if not raw_name:
        return None

    name = raw_name.strip().lower()

    # Strip embedded double quotes: '"chicken" broth' → 'chicken broth'
    name = name.replace('"', '').strip()

    for prefix in STRIP_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
            break

    for suffix in STRIP_FROM_MATCH:
        if suffix in name:
            name = name[:name.index(suffix)].strip()


    # Reject if starts with a digit: "6 garlic", "4 garlic"
    if re.match(r'^\d', name):
        return None

    # Reject if contains "=": editorial/combination notes
    if '=' in name:
        return None

    # Reject alternative expression marker: "sweet potatoes yams -or- 5 and sweet potatoes"
    if '-or-' in name:
        return None

    # Reject if contains " / " (space-slash-space): alternative quantity notation
    # "5 oz / 150g" — note: "boneless/skinless" (no spaces) correctly passes
    if re.search(r'\s/\s', name):
        return None

    # Reject if contains an embedded digit+unit in the middle of the name:
    # "sherry 1 tbsp. honey" — two ingredients combined with a measurement
    if re.search(r'\s\d+\s*(tbsp|tsp|cup|oz|ml|g|lb)\.?', name):
        return None

    # Reject if starts with known noise/instruction/article prefix
    for prefix in REJECT_START_PATTERNS:
        if name.startswith(prefix):
            return None

    # Reject if starts with a unit abbreviation
    for unit_prefix in UNIT_PREFIXES:
        if name.startswith(unit_prefix):
            return None

    # Reject range fragments like "to 8 salmon"
    if re.match(r'^to\s+\d', name):
        return None

    # Reject corrupted single-letter prefix: "f water"
    if re.match(r'^[a-z]\s', name):
        return None

    # --- Strip operations: run before substring checks so that noise appended
    #     after a dash/bracket is removed before REJECT_SUBSTRINGS fires. ---

    # Strip from the first "[" onwards
    name = re.sub(r'\[.*', '', name).strip()

    # Strip trailing unmatched closing parentheses: "coriander leaves)" → "coriander leaves"
    name = re.sub(r'\)+$', '', name).strip()

    # Strip from dash-space (no space before dash): "tofu- a block" → "tofu"
    name = re.sub(r'-\s.*$', '', name).strip()

    # Strip preparation notes after ' - ' (space-dash-space): "garlic - &" → "garlic"
    if ' - ' in name:
        name = name.split(' - ')[0].strip()

    # Strip trailing quantity+unit suffix: "basmati rice-500g" → "basmati rice"
    name = re.sub(r'-\d+[a-z]*$', '', name).strip()

    # Substring reject checks (run after strips so suffixes have been removed)
    for pattern in REJECT_SUBSTRINGS:
        if pattern in name:
            return None

    # Final length check. This is meant to catch rows that end up just like "to"
    if len(name) < 3:
        return None
    if len(name) > INGREDIENT_MAX_NAME_LENGTH:
        return None

    return name or None


def round_numeric(value, ndigits=1):
    return round(value, ndigits) if value is not None else None


# Rows that fail these checks are skipped and logged, not written to silver
def is_valid_row(row: dict) -> bool:
    if not row.get("title"):
        return False

    for field in ("calories_kcal", "protein_g", "fat_g", "carbs_g", "fiber_g", "sugar_g", "sodium_mg", "iron_mg"):
        value = row.get(field)
        if value is not None and value < 0:
            return False

    return True


# *********************************************************************************************************************************************
# 7 - Transform a bronze row into a silver row ready for writing
# *********************************************************************************************************************************************

# Concatenates all step texts across instruction groups into a single ordered string
def build_instructions_text(analyzed_instructions: list) -> str | None:
    steps = []
    for group in analyzed_instructions:
        for step in group.get("steps", []):
            step_text = (step.get("step") or "").strip()
            if step_text:
                steps.append(step_text)
    return " ".join(steps) if steps else None


# Extracts a de-duplicated list of {name, amount, unit} dicts from extendedIngredients. Uses nameClean as the name source (spoonacular's own cleaner field).
# Amount and unit are kept here because they are recipe-specific — they live on recipe_ingredients, not on dim_ingredients
def extract_ingredients(extended_ingredients: list) -> list:
    seen_names = set()
    result = []

    for ingredient in extended_ingredients:
        # Spoonacular assigns a non-zero id to every ingredient it recognizes.
        # id=0 means its NLP couldn't match the token to a known ingredient.
        if not ingredient.get("id"):
            continue

        raw_name = ingredient.get("nameClean") or ""
        name = clean_ingredient_name(raw_name)

        if not name or name in seen_names:
            continue

        seen_names.add(name)
        result.append({
            "name":   name,
            "amount": ingredient.get("amount"),
            "unit":   normalize_unit(ingredient.get("unit")),
        })

    return result


def transform_row(bronze_row: dict) -> dict:
    analyzed_instructions = parse_json_field(bronze_row.get("instructions_raw"))
    extended_ingredients  = parse_json_field(bronze_row.get("extended_ingredients"))

    return {
        "recipe_id":         bronze_row["recipe_id"],
        "title":             (bronze_row.get("title") or "").strip(),
        "instructions_text": build_instructions_text(analyzed_instructions),
        "calories_kcal":     round_numeric(bronze_row.get("calories_kcal")),
        "protein_g":         round_numeric(bronze_row.get("protein_g")),
        "fat_g":             round_numeric(bronze_row.get("fat_g")),
        "carbs_g":           round_numeric(bronze_row.get("carbs_g")),
        "fiber_g":           round_numeric(bronze_row.get("fiber_g")),
        "sugar_g":           round_numeric(bronze_row.get("sugar_g")),
        "sodium_mg":         round_numeric(bronze_row.get("sodium_mg")),
        "iron_mg":           round_numeric(bronze_row.get("iron_mg")),
        "cuisines":          normalize_tags(bronze_row.get("cuisines"), lowercase=False),
        "diets":             normalize_tags(bronze_row.get("diets"), lowercase=True),
        "ingredients":       extract_ingredients(extended_ingredients),
    }


# *********************************************************************************************************************************************
# 8 - Dim table helpers: insert new names if they don't exist, then return the full {name: id} map for a given set of names
# *********************************************************************************************************************************************

# Generic helper for diet and ingredient dim tables (dynamic population). Inserts any unseen names, then returns a {name: id} dict for the full set
def _upsert_dim_and_get_ids(conn, table: str, name_col: str, id_col: str, names: set) -> dict:
    if not names:
        return {}

    conn.execute(
        text(f"""
            INSERT INTO {SILVER_SCHEMA}.{table} ({name_col})
            VALUES (:name)
            ON CONFLICT ({name_col}) DO NOTHING
        """),
        [{"name": n} for n in names],
    )

    result = conn.execute(
        text(f"""
            SELECT {id_col}, {name_col}
            FROM {SILVER_SCHEMA}.{table}
            WHERE {name_col} = ANY(:names)
        """),
        {"names": list(names)},
    )
    return {row[1]: row[0] for row in result}


# Looks up cuisine IDs from dim_cuisines (pre-seeded, read-only at runtime). Cuisines not found in dim_cuisines are skipped
def _get_cuisine_ids(conn, cuisine_names: set) -> dict:
    if not cuisine_names:
        return {}

    result = conn.execute(
        text(f"""
            SELECT cuisine_id, cuisine_name
            FROM {SILVER_SCHEMA}.dim_cuisines
            WHERE cuisine_name = ANY(:names)
        """),
        {"names": list(cuisine_names)},
    )
    return {row[1]: row[0] for row in result}


# *********************************************************************************************************************************************
# 9 - Write a batch of transformed rows into silver.recipes + all dim and junction tables
# *********************************************************************************************************************************************
def write_silver(rows: list, engine) -> int:
    if not rows:
        return 0

    now = datetime.now(timezone.utc)
    recipe_ids = [r["recipe_id"] for r in rows]

    # Collect all unique dim values across this batch upfront for efficient bulk inserts
    all_cuisines    = set(c for r in rows for c in r["cuisines"])
    all_diets       = set(d for r in rows for d in r["diets"])
    all_ingredients = set(i["name"] for r in rows for i in r["ingredients"])

    recipe_params = [
        {
            "recipe_id":         r["recipe_id"],
            "title":             r["title"],
            "instructions_text": r["instructions_text"],
            "calories_kcal":     r["calories_kcal"],
            "protein_g":         r["protein_g"],
            "fat_g":             r["fat_g"],
            "carbs_g":           r["carbs_g"],
            "fiber_g":           r["fiber_g"],
            "sugar_g":           r["sugar_g"],
            "sodium_mg":         r["sodium_mg"],
            "iron_mg":           r["iron_mg"],
            "updated_at":        now,
        }
        for r in rows
    ]

    with engine.begin() as conn:

        # Upsert core recipe rows
        conn.execute(
            text(f"""
                INSERT INTO {SILVER_SCHEMA}.{SILVER_TABLE}
                    (recipe_id, title, instructions_text,
                     calories_kcal, protein_g, fat_g, carbs_g,
                     fiber_g, sugar_g, sodium_mg, iron_mg, updated_at)
                VALUES
                    (:recipe_id, :title, :instructions_text,
                     :calories_kcal, :protein_g, :fat_g, :carbs_g,
                     :fiber_g, :sugar_g, :sodium_mg, :iron_mg, :updated_at)
                ON CONFLICT (recipe_id) DO UPDATE SET
                    title             = EXCLUDED.title,
                    instructions_text = EXCLUDED.instructions_text,
                    calories_kcal     = EXCLUDED.calories_kcal,
                    protein_g         = EXCLUDED.protein_g,
                    fat_g             = EXCLUDED.fat_g,
                    carbs_g           = EXCLUDED.carbs_g,
                    fiber_g           = EXCLUDED.fiber_g,
                    sugar_g           = EXCLUDED.sugar_g,
                    sodium_mg         = EXCLUDED.sodium_mg,
                    iron_mg           = EXCLUDED.iron_mg,
                    updated_at        = EXCLUDED.updated_at
            """),
            recipe_params,
        )

        # Resolve dim IDs — cuisine is lookup-only; diets and ingredients also insert-if-new
        cuisine_id_map    = _get_cuisine_ids(conn, all_cuisines)
        diet_id_map       = _upsert_dim_and_get_ids(conn, "dim_diets",       "diet_name",       "diet_id",       all_diets)
        ingredient_id_map = _upsert_dim_and_get_ids(conn, "dim_ingredients", "ingredient_name", "ingredient_id", all_ingredients)

        # Junction tables: delete then reinsert so stale tags from prior runs don't accumulate
        for junction_table in ("recipe_cuisines", "recipe_diets", "recipe_ingredients"):
            conn.execute(
                text(f"DELETE FROM {SILVER_SCHEMA}.{junction_table} WHERE recipe_id = ANY(:ids)"),
                {"ids": recipe_ids},
            )

        cuisine_junction = [
            {"recipe_id": r["recipe_id"], "cuisine_id": cuisine_id_map[c]}
            for r in rows for c in r["cuisines"]
            if c in cuisine_id_map
        ]

        diet_junction = [
            {"recipe_id": r["recipe_id"], "diet_id": diet_id_map[d]}
            for r in rows for d in r["diets"]
            if d in diet_id_map
        ]

        # Ingredients carry amount and unit — recipe-specific, stored on the junction table
        ingredient_junction = [
            {
                "recipe_id":     r["recipe_id"],
                "ingredient_id": ingredient_id_map[i["name"]],
                "amount":        i["amount"],
                "unit":          i["unit"],
            }
            for r in rows for i in r["ingredients"]
            if i["name"] in ingredient_id_map
        ]

        if cuisine_junction:
            conn.execute(
                text(f"""
                    INSERT INTO {SILVER_SCHEMA}.recipe_cuisines (recipe_id, cuisine_id)
                    VALUES (:recipe_id, :cuisine_id)
                    ON CONFLICT DO NOTHING
                """),
                cuisine_junction,
            )

        if diet_junction:
            conn.execute(
                text(f"""
                    INSERT INTO {SILVER_SCHEMA}.recipe_diets (recipe_id, diet_id)
                    VALUES (:recipe_id, :diet_id)
                    ON CONFLICT DO NOTHING
                """),
                diet_junction,
            )

        if ingredient_junction:
            conn.execute(
                text(f"""
                    INSERT INTO {SILVER_SCHEMA}.recipe_ingredients (recipe_id, ingredient_id, amount, unit)
                    VALUES (:recipe_id, :ingredient_id, :amount, :unit)
                    ON CONFLICT DO NOTHING
                """),
                ingredient_junction,
            )

    logger.info(f"Silver: upserted {len(rows)} recipes")
    return len(rows)


# *********************************************************************************************************************************************
# 10 - Read new/changed rows from bronze.recipes since the last watermark
# *********************************************************************************************************************************************
def read_bronze_batch(engine, watermark, limit=BATCH_SIZE) -> list:
    sql = f"""
        SELECT recipe_id, title, cuisines, diets,
               calories_kcal, protein_g, fat_g, carbs_g,
               fiber_g, sugar_g, sodium_mg, iron_mg,
               instructions_raw, extended_ingredients,
               loaded_at
        FROM {BRONZE_SCHEMA}.{BRONZE_TABLE}
        WHERE (:watermark IS NULL OR loaded_at > :watermark)
        ORDER BY loaded_at
        LIMIT :limit
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"watermark": watermark, "limit": limit})
        return [dict(row._mapping) for row in result]


# *********************************************************************************************************************************************
# 11 - Main transform. Pulls new bronze rows in batches, cleans and normalizes them, and upserts into silver until caught up
# *********************************************************************************************************************************************
def transform() -> None:

    create_silver_tables(ENGINE)

    watermark = get_watermark(ENGINE)
    logger.info(f"Resuming from watermark: {watermark or 'beginning of bronze'}")

    total_processed = 0
    total_skipped   = 0

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


# Standalone
if __name__ == "__main__":
    transform()