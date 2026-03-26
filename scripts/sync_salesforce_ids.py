import os
import logging
from simple_salesforce import Salesforce
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

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
# Sync functions
# ---------------------------------------------------------------------------

def sync_day_ids(sf, conn):
    log.info("Querying Day__c IDs from Salesforce")
    result = sf.query_all("SELECT Id, Date__c FROM Day__c ORDER BY Date__c ASC")
    records = result['records']
    log.info(f"Retrieved {len(records)} Day__c records")

    updated = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for r in records:
            cur.execute("""
                UPDATE day
                SET salesforce_id   = %s,
                    source_system   = 'salesforce',
                    source_object   = 'Day__c',
                    last_synced_at  = now(),
                    updated_at      = now()
                WHERE date = %s
                AND (salesforce_id IS NULL OR salesforce_id <> %s)
            """, (r['Id'], r['Date__c'], r['Id']))
            updated += cur.rowcount
    conn.commit()
    log.info(f"Day: {updated} records updated")


def sync_week_ids(sf, conn):
    log.info("Querying Week__c IDs from Salesforce")
    result = sf.query_all("""
        SELECT Id, Week_Number__c, Year__c
        FROM Week__c
        ORDER BY Year__c ASC, Week_Number__c ASC
    """)
    records = result['records']
    log.info(f"Retrieved {len(records)} Week__c records")

    updated = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for r in records:
            cur.execute("""
                UPDATE week
                SET salesforce_id   = %s,
                    source_system   = 'salesforce',
                    source_object   = 'Week__c',
                    last_synced_at  = now(),
                    updated_at      = now()
                WHERE calendar_year = %s
                AND week_number     = %s
                AND (salesforce_id IS NULL OR salesforce_id <> %s)
            """, (r['Id'], int(r['Year__c']), int(r['Week_Number__c']), r['Id']))
            updated += cur.rowcount
    conn.commit()
    log.info(f"Week: {updated} records updated")


def sync_month_ids(sf, conn):
    log.info("Querying Month__c IDs from Salesforce")
    result = sf.query_all("""
        SELECT Id, Month_Number__c, Year__r.Name
        FROM Month__c
        ORDER BY Year__r.Name ASC, Month_Number__c ASC
    """)
    records = result['records']
    log.info(f"Retrieved {len(records)} Month__c records")

    updated = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for r in records:
            cur.execute("""
                UPDATE month
                SET salesforce_id   = %s,
                    source_system   = 'salesforce',
                    source_object   = 'Month__c',
                    last_synced_at  = now(),
                    updated_at      = now()
                WHERE calendar_year = %s
                AND month_number    = %s
                AND (salesforce_id IS NULL OR salesforce_id <> %s)
            """, (r['Id'], int(r['Year__r']['Name']), int(r['Month_Number__c']), r['Id']))
            updated += cur.rowcount
    conn.commit()
    log.info(f"Month: {updated} records updated")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("Starting Salesforce ID sync")

    sf = get_salesforce()
    conn = get_postgres()

    sync_month_ids(sf, conn)
    sync_week_ids(sf, conn)
    sync_day_ids(sf, conn)

    conn.close()
    log.info("Salesforce ID sync complete")


if __name__ == '__main__':
    main()