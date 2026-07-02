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

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

LOG_SCHEMA = "logging"
LOG_TABLE = "gold_log"

TOP_INGREDIENTS_PER_REGION = 5
TOP_RECIPES_PER_NUTRIENT = 5

# Ingredient names to exclude from the top-ingredients ranking — ubiquitous staples that appear in nearly every recipe and so aren't informative about a region
EXCLUDED_INGREDIENTS = ["salt"]

# The eight nutrient columns carried through from silver.recipes, and the label each one is exposed under in the per-nutrient ranking view.
NUTRIENT_COLUMNS = {
    "calories_kcal": "calories",
    "protein_g":     "protein",
    "fat_g":         "fat",
    "carbs_g":       "carbs",
    "fiber_g":       "fiber",
    "sugar_g":       "sugar",
    "sodium_mg":     "sodium",
    "iron_mg":       "iron",
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


# Writes each log record as a row in logging.gold_log. Failures here are swallowed (via handleError) so a DB hiccup never crashes the run itself
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
# 4 - Shared foundation: a DISTINCT recipe <-> region bridge. A recipe can have several cuisines that map to the same region (Italian and Greek
#     are both Mediterranean), so I dedupe (recipe_id, region) pairs to avoid double counting a recipe within one region.
#     NULL regions (e.g. Jewish) are excluded here so they never enter any region-based analysis
# *********************************************************************************************************************************************
def create_region_bridge(engine) -> None:
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};

        CREATE OR REPLACE VIEW {GOLD_SCHEMA}.recipe_region AS
        SELECT DISTINCT
            rc.recipe_id,
            dc.region
        FROM {SILVER_SCHEMA}.recipe_cuisines rc
        JOIN {SILVER_SCHEMA}.dim_cuisines dc ON dc.cuisine_id = rc.cuisine_id
        WHERE dc.region IS NOT NULL;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info(f"View ready -> {GOLD_SCHEMA}.recipe_region (recipe↔region bridge)")


# *********************************************************************************************************************************************
# 5 - MV 1: Top N ingredients per region. Frequency = (recipes in the region that use the ingredient) / (total distinct recipes in the region)
#     so it's a proportion, comparable across regions of different sizes.
# *********************************************************************************************************************************************
def create_top_ingredients_by_region(engine) -> None:
    # Build a SQL list literal from EXCLUDED_INGREDIENTS, e.g. ('salt')
    excluded_sql = ", ".join(f"'{i}'" for i in EXCLUDED_INGREDIENTS)

    sql = f"""
        DROP MATERIALIZED VIEW IF EXISTS {GOLD_SCHEMA}.top_ingredients_by_region CASCADE;

        CREATE MATERIALIZED VIEW {GOLD_SCHEMA}.top_ingredients_by_region AS
        WITH region_totals AS (
            -- total distinct recipes per region (the denominator)
            SELECT region, COUNT(DISTINCT recipe_id) AS total_recipes
            FROM {GOLD_SCHEMA}.recipe_region
            GROUP BY region
        ),
        ingredient_counts AS (
            -- recipes per (region, ingredient) — DISTINCT recipe_id so a recipe
            -- listing an ingredient twice still counts once.
            -- Ubiquitous staples (EXCLUDED_INGREDIENTS, e.g. salt) are filtered out.
            SELECT
                rr.region,
                di.ingredient_name,
                COUNT(DISTINCT rr.recipe_id) AS recipe_count
            FROM {GOLD_SCHEMA}.recipe_region rr
            JOIN {SILVER_SCHEMA}.recipe_ingredients ri ON ri.recipe_id = rr.recipe_id
            JOIN {SILVER_SCHEMA}.dim_ingredients   di ON di.ingredient_id = ri.ingredient_id
            WHERE di.ingredient_name NOT IN ({excluded_sql})
            GROUP BY rr.region, di.ingredient_name
        ),
        ranked AS (
            SELECT
                ic.region,
                ic.ingredient_name,
                ic.recipe_count,
                rt.total_recipes,
                -- frequency as a percentage of the region's recipes, e.g. 42.75 (%)
                ROUND(100.0 * ic.recipe_count / rt.total_recipes, 2) AS frequency_pct,
                ROW_NUMBER() OVER (
                    PARTITION BY ic.region
                    ORDER BY ic.recipe_count DESC, ic.ingredient_name ASC
                ) AS rank
            FROM ingredient_counts ic
            JOIN region_totals rt ON rt.region = ic.region
        )
        SELECT region, ingredient_name, recipe_count, total_recipes, frequency_pct, rank
        FROM ranked
        WHERE rank <= {TOP_INGREDIENTS_PER_REGION};

        -- Unique index required for REFRESH
        CREATE UNIQUE INDEX IF NOT EXISTS idx_top_ingredients_by_region
            ON {GOLD_SCHEMA}.top_ingredients_by_region (region, rank);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info(f"MV ready -> {GOLD_SCHEMA}.top_ingredients_by_region")


# *********************************************************************************************************************************************
# 6 - MV 2: Top N recipes per nutrient (highest value), with the recipe's full ingredient list (string_agg) and instructions for display.
#     One UNION ALL branch per nutrient, each ranking recipes by that nutrient DESC.
# *********************************************************************************************************************************************
def create_top_recipes_by_nutrient(engine) -> None:
    # Build one ranked branch per nutrient, unioned together.
    branches = []
    for column, label in NUTRIENT_COLUMNS.items():
        branches.append(f"""
        SELECT
            '{label}' AS nutrient,
            r.recipe_id,
            r.title,
            r.{column} AS nutrient_value,
            ing.ingredients,
            r.instructions_text,
            ROW_NUMBER() OVER (ORDER BY r.{column} DESC NULLS LAST, r.recipe_id ASC) AS rank
        FROM {SILVER_SCHEMA}.recipes r
        LEFT JOIN ingredient_agg ing ON ing.recipe_id = r.recipe_id
        WHERE r.{column} IS NOT NULL
        """)

    union_sql = "\n        UNION ALL\n".join(
        f"SELECT * FROM ({b}) AS ranked_{label} WHERE rank <= {TOP_RECIPES_PER_NUTRIENT}"
        for (b, label) in zip(branches, NUTRIENT_COLUMNS.values())
    )

    sql = f"""
        DROP MATERIALIZED VIEW IF EXISTS {GOLD_SCHEMA}.top_recipes_by_nutrient CASCADE;

        CREATE MATERIALIZED VIEW {GOLD_SCHEMA}.top_recipes_by_nutrient AS
        WITH ingredient_agg AS (
            -- One comma-separated ingredient string per recipe, alphabetical for stability
            SELECT
                ri.recipe_id,
                string_agg(di.ingredient_name, ', ' ORDER BY di.ingredient_name) AS ingredients
            FROM {SILVER_SCHEMA}.recipe_ingredients ri
            JOIN {SILVER_SCHEMA}.dim_ingredients di ON di.ingredient_id = ri.ingredient_id
            GROUP BY ri.recipe_id
        )
        {union_sql};

        -- Unique index required for REFRESH
        CREATE UNIQUE INDEX IF NOT EXISTS idx_top_recipes_by_nutrient
            ON {GOLD_SCHEMA}.top_recipes_by_nutrient (nutrient, rank);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info(f"MV ready -> {GOLD_SCHEMA}.top_recipes_by_nutrient")


# *********************************************************************************************************************************************
# 7 - MV 3: Diet options per region. recipe_count = distinct recipes matching that diet in that region
#           rank 1 = the region with the most options for that diet.
# *********************************************************************************************************************************************
def create_diet_options_by_region(engine) -> None:
    sql = f"""
        DROP MATERIALIZED VIEW IF EXISTS {GOLD_SCHEMA}.diet_options_by_region CASCADE;

        CREATE MATERIALIZED VIEW {GOLD_SCHEMA}.diet_options_by_region AS
        WITH diet_region_counts AS (
            SELECT
                dd.diet_name,
                rr.region,
                COUNT(DISTINCT rr.recipe_id) AS recipe_count
            FROM {GOLD_SCHEMA}.recipe_region rr
            JOIN {SILVER_SCHEMA}.recipe_diets rd ON rd.recipe_id = rr.recipe_id
            JOIN {SILVER_SCHEMA}.dim_diets    dd ON dd.diet_id = rd.diet_id
            GROUP BY dd.diet_name, rr.region
        )
        SELECT
            diet_name,
            region,
            recipe_count,
            ROW_NUMBER() OVER (
                PARTITION BY diet_name
                ORDER BY recipe_count DESC, region ASC
            ) AS rank
        FROM diet_region_counts;

        -- Unique index required for REFRESH
        CREATE UNIQUE INDEX IF NOT EXISTS idx_diet_options_by_region
            ON {GOLD_SCHEMA}.diet_options_by_region (diet_name, rank);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info(f"MV ready -> {GOLD_SCHEMA}.diet_options_by_region")


# *********************************************************************************************************************************************
# 8 - MV 4: Average nutrient values per region.
# *********************************************************************************************************************************************
def create_avg_nutrients_by_region(engine) -> None:
    avg_columns = ",\n            ".join(
        f"ROUND(AVG(r.{column}), 2) AS avg_{column}"
        for column in NUTRIENT_COLUMNS
    )

    sql = f"""
        DROP MATERIALIZED VIEW IF EXISTS {GOLD_SCHEMA}.avg_nutrients_by_region CASCADE;

        CREATE MATERIALIZED VIEW {GOLD_SCHEMA}.avg_nutrients_by_region AS
        SELECT
            rr.region,
            COUNT(DISTINCT r.recipe_id) AS recipe_count,
            {avg_columns}
        FROM {GOLD_SCHEMA}.recipe_region rr
        JOIN {SILVER_SCHEMA}.recipes r ON r.recipe_id = rr.recipe_id
        GROUP BY rr.region;

        -- Unique index required for REFRESH
        CREATE UNIQUE INDEX IF NOT EXISTS idx_avg_nutrients_by_region
            ON {GOLD_SCHEMA}.avg_nutrients_by_region (region);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info(f"MV ready -> {GOLD_SCHEMA}.avg_nutrients_by_region")


# *********************************************************************************************************************************************
# 9 - Refresh all materialized views. Falls back to a plain refresh the first time
# *********************************************************************************************************************************************
MATERIALIZED_VIEWS = [
    "top_ingredients_by_region",
    "top_recipes_by_nutrient",
    "diet_options_by_region",
    "avg_nutrients_by_region",
]


# Returns True only if all expected materialized views already exist. Lets the refresh-only task detect a first run and create them instead
def gold_views_exist(engine) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT matviewname
                FROM pg_matviews
                WHERE schemaname = :schema
            """),
            {"schema": GOLD_SCHEMA},
        )
        existing = {row[0] for row in result}
    return all(mv in existing for mv in MATERIALIZED_VIEWS)


# Creates the bridge view and all four materialized views (idempotent -> each MV is DROP ... IF EXISTS then recreated). The CREATE statements populatethe MVs on creation. 
# Run this once at setup, or whenever a view definition changes

def create_gold_views(engine) -> None:
    logger.info("Creating gold views")

    create_region_bridge(engine)
    create_top_ingredients_by_region(engine)
    create_top_recipes_by_nutrient(engine)
    create_diet_options_by_region(engine)
    create_avg_nutrients_by_region(engine)

    logger.info("Gold views created")


# Refreshes every materialized view from the latest silver data. This is the routine Airflow calls on each scheduled run
def refresh_all(engine) -> None:
    for mv in MATERIALIZED_VIEWS:
        fq = f"{GOLD_SCHEMA}.{mv}"
        try:
            with engine.begin() as conn:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {fq}"))
            logger.info(f"Refreshed (concurrently) -> {fq}")
        except Exception as e:
            # First-ever refresh can't be concurrent (MV not yet populated) — fall back.
            logger.warning(f"Concurrent refresh failed for {fq} ({e}); falling back to plain refresh")
            with engine.begin() as conn:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW {fq}"))
            logger.info(f"Refreshed (plain) -> {fq}")


# *********************************************************************************************************************************************
# 10 - Entry points.
#      - create_gold_views(): one-time (or on-definition-change) setup task.
#      - refresh_gold():       the routine Airflow runs after each silver load. It
#                              creates the views first if they don't exist yet, so a
#                              fresh environment self-heals without a separate setup step.
# *********************************************************************************************************************************************
def refresh_gold() -> None:
    logger.info("Refreshing gold layer")

    if not gold_views_exist(ENGINE):
        logger.info("Gold views missing — creating them before refresh")
        create_gold_views(ENGINE)
        # CREATE already populated the MVs, so no separate refresh is needed this run.
        logger.info("Gold layer created and populated")
        return

    refresh_all(ENGINE)
    logger.info("Gold layer refresh complete")


# Standalone
if __name__ == "__main__":
    refresh_gold()