-- The roles' settings for the database, applied after mgyp_schema.sql.
-- Unlike mgyp_schema.sql, this needs ownership of the database, and CREATEROLE with the ADMIN option on the roles.

-- mgyp_schema.sql's SET search_path applies only to its own session, so each role gets the schema at login.
-- The database is named at run time, since it differs between production, dev and the tests.
DO $$
DECLARE
  r text;
BEGIN
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO mgyp_emgapi', current_database());
  -- Without the right to grant it, the GRANT only warns.
  IF NOT has_database_privilege('mgyp_emgapi', current_database(), 'TEMPORARY') THEN
    RAISE EXCEPTION 'mgyp_emgapi cannot create temporary tables in %', current_database();
  END IF;
  FOREACH r IN ARRAY ARRAY['mgyp_emgapi', 'mgyp_read'] LOOP
    EXECUTE format('ALTER ROLE %I IN DATABASE %I SET search_path = mgyp', r, current_database());
  END LOOP;
END $$;

-- A client that hangs mid-transaction keeps its new hashes locked, and other ingestions meeting them wait.
ALTER ROLE mgyp_emgapi SET idle_in_transaction_session_timeout = '10min';
ALTER ROLE mgyp_emgapi SET work_mem = '256MB';
ALTER ROLE mgyp_emgapi SET temp_buffers = '512MB';
