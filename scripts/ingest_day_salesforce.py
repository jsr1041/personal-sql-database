import os
import logging
from datetime import datetime, date
from simple_salesforce import Salesforce
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def get_salesforce():
    return Salesforce(
        username=os.environ['SF_USERNAME'],
        password=os.environ['SF_PASSWORD'],
        security_token=os.environ['SF_TOKEN']
    )


def get_postgres():
    return psycopg2.connect(
        host=os.environ.get('PG_HOST', 'localhost'),
        port=os.environ.get('PG_PORT', 5432),
        dbname=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD')
    )


# ---------------------------------------------------------------------------
# Calendar FK resolution
# ---------------------------------------------------------------------------

def load_calendar_lookups(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:

        cur.execute("""
            SELECT week_id, week_start_date, week_end_date
            FROM week
            ORDER BY week_start_date
        """)
        weeks = cur.fetchall()

        cur.execute("SELECT month_id, calendar_year, month_number FROM month")
        months = {
            (r['calendar_year'], r['month_number']): r['month_id']
            for r in cur.fetchall()
        }

        cur.execute("SELECT year_id, calendar_year FROM year")
        years = {r['calendar_year']: r['year_id'] for r in cur.fetchall()}

    return weeks, months, years


def resolve_week_id(d, weeks):
    for w in weeks:
        if w['week_start_date'] <= d <= w['week_end_date']:
            return w['week_id']
    raise ValueError(f"No week found for date {d}")


def resolve_month_id(d, months):
    key = (d.year, d.month)
    if key not in months:
        raise ValueError(f"No month found for {d.year}-{d.month:02d}")
    return months[key]


def resolve_year_id(d, years):
    if d.year not in years:
        raise ValueError(f"No year found for {d.year}")
    return years[d.year]


# ---------------------------------------------------------------------------
# Salesforce query
# ---------------------------------------------------------------------------

SOQL = """
    SELECT
        Id,
        Date__c,
        Calories__c,
        Protein__c,
        Fat__c,
        Net_Carbs__c,
        Daily_Gallons_of_Water_Drank__c,
        Daily_Liters_of_Water_Drank__c,
        Daily_Alcoholic_Drinks_Drank__c,
        Daily_Steps__c,
        Daily_Pushups__c,
        Sleep_Time__c,
        Minutes_of_Reading__c,
        Minutes_of_TV__c,
        Screen_Time__c,
        Calories_Burned__c,
        Daily_Points_Scored__c,
        Total_Daily_Points__c,
        Daily_Rating__c,
        Incomplete_Food_Log__c,
        Breakfast__c,
        Lunch__c,
        Dinner__c
    FROM Day__c
    ORDER BY Date__c ASC
"""


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(record, weeks, months, years):
    d = date.fromisoformat(record['Date__c'])
    return {
        'date':                  d,
        'salesforce_id':         record['Id'],
        'calories':              record.get('Calories__c'),
        'protein':               record.get('Protein__c'),
        'fat':                   record.get('Fat__c'),
        'carbs':                 record.get('Net_Carbs__c'),
        'daily_gallons_water':   record.get('Daily_Gallons_of_Water_Drank__c'),
        'daily_liters_water':    record.get('Daily_Liters_of_Water_Drank__c'),
        'daily_alcoholic_drinks':record.get('Daily_Alcoholic_Drinks_Drank__c'),
        'daily_steps':           record.get('Daily_Steps__c'),
        'daily_pushups':         record.get('Daily_Pushups__c'),
        'sleep_time':            record.get('Sleep_Time__c'),
        'minutes_reading':       record.get('Minutes_of_Reading__c'),
        'minutes_tv':            record.get('Minutes_of_TV__c'),
        'screen_time':           record.get('Screen_Time__c'),
        'calories_burned':       record.get('Calories_Burned__c'),
        'daily_points_scored':   record.get('Daily_Points_Scored__c'),
        'total_daily_points':    record.get('Total_Daily_Points__c'),
        'daily_rating':          record.get('Daily_Rating__c'),
        'incomplete_food_log':   record.get('Incomplete_Food_Log__c') or False,
        'breakfast':             record.get('Breakfast__c'),
        'lunch':                 record.get('Lunch__c'),
        'dinner':                record.get('Dinner__c'),
        'week_id':               resolve_week_id(d, weeks),
        'month_id':              resolve_month_id(d, months),
        'year_id':               resolve_year_id(d, years),
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

UPSERT_SQL = """
    INSERT INTO day (
        date, salesforce_id, calories, protein, fat, carbs,
        daily_gallons_water, daily_liters_water, daily_alcoholic_drinks,
        daily_steps, daily_pushups, sleep_time, minutes_reading, minutes_tv,
        screen_time, calories_burned, daily_points_scored, total_daily_points,
        daily_rating, incomplete_food_log, breakfast, lunch, dinner,
        week_id, month_id, year_id,
        source_system, source_object, last_synced_at
    )
    VALUES (
        %(date)s, %(salesforce_id)s, %(calories)s, %(protein)s, %(fat)s,
        %(carbs)s, %(daily_gallons_water)s, %(daily_liters_water)s,
        %(daily_alcoholic_drinks)s, %(daily_steps)s, %(daily_pushups)s,
        %(sleep_time)s, %(minutes_reading)s, %(minutes_tv)s, %(screen_time)s,
        %(calories_burned)s, %(daily_points_scored)s, %(total_daily_points)s,
        %(daily_rating)s, %(incomplete_food_log)s, %(breakfast)s, %(lunch)s,
        %(dinner)s, %(week_id)s, %(month_id)s, %(year_id)s,
        'salesforce', 'Day__c', now()
    )
    ON CONFLICT (date) DO UPDATE SET
        salesforce_id         = EXCLUDED.salesforce_id,
        calories              = EXCLUDED.calories,
        protein               = EXCLUDED.protein,
        fat                   = EXCLUDED.fat,
        carbs                 = EXCLUDED.carbs,
        daily_gallons_water   = EXCLUDED.daily_gallons_water,
        daily_liters_water    = EXCLUDED.daily_liters_water,
        daily_alcoholic_drinks= EXCLUDED.daily_alcoholic_drinks,
        daily_steps           = EXCLUDED.daily_steps,
        daily_pushups         = EXCLUDED.daily_pushups,
        sleep_time            = EXCLUDED.sleep_time,
        minutes_reading       = EXCLUDED.minutes_reading,
        minutes_tv            = EXCLUDED.minutes_tv,
        screen_time           = EXCLUDED.screen_time,
        calories_burned       = EXCLUDED.calories_burned,
        daily_points_scored   = EXCLUDED.daily_points_scored,
        total_daily_points    = EXCLUDED.total_daily_points,
        daily_rating          = EXCLUDED.daily_rating,
        incomplete_food_log   = EXCLUDED.incomplete_food_log,
        breakfast             = EXCLUDED.breakfast,
        lunch                 = EXCLUDED.lunch,
        dinner                = EXCLUDED.dinner,
        week_id               = EXCLUDED.week_id,
        month_id              = EXCLUDED.month_id,
        year_id               = EXCLUDED.year_id,
        last_synced_at        = now(),
        updated_at            = now()
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("Starting Day__c ingestion")

    log.info("Connecting to Salesforce")
    sf = get_salesforce()

    log.info("Connecting to Postgres")
    conn = get_postgres()

    log.info("Loading calendar lookups")
    weeks, months, years = load_calendar_lookups(conn)

    log.info("Querying Day__c from Salesforce")
    result = sf.query_all(SOQL)
    records = result['records']
    log.info(f"Retrieved {len(records)} Day__c records")

    log.info("Transforming records")
    transformed = []
    errors = []
    for r in records:
        try:
            transformed.append(transform(r, weeks, months, years))
        except Exception as e:
            errors.append((r.get('Date__c'), str(e)))
            log.warning(f"Skipping {r.get('Date__c')}: {e}")

    log.info(f"Transformed {len(transformed)} records, skipped {len(errors)}")

    log.info("Upserting into Postgres")
    with conn.cursor() as cur:
        execute_batch(cur, UPSERT_SQL, transformed, page_size=100)
    conn.commit()

    log.info(f"Ingestion complete — {len(transformed)} records upserted")

    if errors:
        log.warning(f"{len(errors)} records skipped:")
        for date_val, msg in errors:
            log.warning(f"  {date_val}: {msg}")

    conn.close()


if __name__ == '__main__':
    main()