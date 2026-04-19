#!/usr/bin/env python3
"""Directly fix the match_mapping table by dropping and recreating."""

import os

from dotenv import load_dotenv

load_dotenv()


def get_connection_string() -> str:
    """Build connection string from environment variables."""
    # Supabase stores connection info in different format
    # We need to use the proper connection
    url = os.getenv("SUPABASE_URL", "")

    # Parse the project ref from URL
    # https://edqfgskzjrzasxaphkuw.supabase.co -> edqfgskzjrzasxaphkuw

    # For Supabase, we use JDBC connection parameters
    # The direct approach is using the anon key with psycopg2 is tricky
    # Let's use the postgresql connection from Supabase dashboard

    # In development, let's try the direct connection string
    # from Supabase settings -> Connection String
    # Format: postgresql://postgres:[password]@db.[project-ref].supabase.co:5432/postgres

    _project_ref = url.replace("https://", "").split(".")[0]

    # We need the actual password - but let's check if there's an easier way
    # Actually, let's try using requests directly!
    return None


def fix_via_api():
    """Fix the constraint via Supabase API using SQL."""
    import requests

    url = os.getenv("SUPABASE_URL", "")
    _key = os.getenv("SUPABASE_KEY", "")

    # For Supabase, we need to enable the SQL editor extension
    # Actually, let's use the pgweb approach - we can create a view workaround

    # Alternative: Just drop constraint via HTTP!
    # Use the pg_catalog endpoint

    # Get service role key for admin operations
    service_key = os.getenv("SUPABASE_SERVICE_KEY", "")

    if not service_key:
        print("No service key found in .env")
        print(
            "Available env vars:",
            [k for k in os.environ.keys() if "SUPABASE" in k.upper()],
        )
        return

    # Try direct SQL via HTTP
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    # Try to execute via postg REST
    resp = requests.post(
        f"{url}/rest/v1/rpc/exec_sql",
        headers=headers,
        json={
            "query": "ALTER TABLE match_mapping DROP CONSTRAINT match_mapping_match_source_check"
        },
    )
    print(f"Response: {resp.status_code} - {resp.text}")


if __name__ == "__main__":
    fix_via_api()
