# Supabase Setup Guide

## Overview

This project uses Supabase (hosted Postgres) as its production database. Follow these steps to provision and configure your Supabase project.

## Step 1: Create a Supabase Project

1. Go to [https://supabase.com](https://supabase.com) and sign up (free tier is sufficient).
2. Click **New Project**.
3. Choose an organization (or create one).
4. Set a project name (e.g., `fpl-pipeline`).
5. Set a strong database password and save it securely.
6. Select a region close to your Streamlit Cloud deployment (e.g., `US East` or `West Europe`).
7. Wait ~2 minutes for the project to provision.

## Step 2: Get Connection Credentials

1. In the Supabase dashboard, go to **Project Settings** → **API**.
2. Copy the following values:
   - **Project URL** (e.g., `https://xxxxx.supabase.co`)
   - **anon public key** (starts with `eyJ...`)
   - **service_role key** (keep this secret — never commit to git)
3. Update `.streamlit/secrets.toml`:
   ```toml
   SUPABASE_URL = "https://xxxxx.supabase.co"
   SUPABASE_KEY = "your-anon-public-key"
   ```

## Step 3: Run SQL Migrations

1. In the Supabase dashboard, go to **SQL Editor**.
2. Open `data/migrations/001_create_tables.sql` from this repo.
3. Paste the contents into the SQL Editor and click **Run**.
4. Repeat for `data/migrations/002_create_indexes.sql`.
5. Verify all 8 tables were created: `players`, `teams`, `fixtures`, `player_stats`, `team_h2h`, `player_vs_team`, `predictions`, `match_simulations`.

## Step 4: Verify Connection

```python
from supabase import create_client

client = create_client("https://xxxxx.supabase.co", "your-anon-key")
result = client.table("players").select("*").limit(1).execute()
print(result.data)  # Should return [] (empty table)
```

## Step 5: (Optional) Configure Row-Level Security

For a personal project, RLS is optional. If you want to enable it:

```sql
-- Enable RLS on all tables
ALTER TABLE players ENABLE ROW LEVEL SECURITY;
ALTER TABLE teams ENABLE ROW LEVEL SECURITY;
ALTER TABLE fixtures ENABLE ROW LEVEL SECURITY;
ALTER TABLE player_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_h2h ENABLE ROW LEVEL SECURITY;
ALTER TABLE player_vs_team ENABLE ROW LEVEL SECURITY;
ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE match_simulations ENABLE ROW LEVEL SECURITY;

-- Allow anon key to read all tables (public dashboard)
CREATE POLICY "Allow anon read" ON players FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON teams FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON fixtures FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON player_stats FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON team_h2h FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON player_vs_team FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON predictions FOR SELECT USING (true);
CREATE POLICY "Allow anon read" ON match_simulations FOR SELECT USING (true);

-- Allow anon key to write to predictions and match_simulations (pipeline output)
CREATE POLICY "Allow anon write predictions" ON predictions FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow anon write simulations" ON match_simulations FOR INSERT WITH CHECK (true);
```

## Free Tier Limits

| Resource | Limit |
|----------|-------|
| Database size | 500 MB |
| API requests | Unlimited |
| Bandwidth | 5 GB/month |
| Concurrent connections | 200 |

Monitor usage in **Project Settings** → **Usage**.
