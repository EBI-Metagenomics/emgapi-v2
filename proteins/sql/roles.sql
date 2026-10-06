-- The runtime roles' settings for the database, applied after tier1.sql.
-- Unlike tier1.sql, this needs ownership of the database, and CREATEROLE with the ADMIN option on the three roles.

-- tier1.sql's SET search_path applies only to its own session, so each role gets the schema at login.
-- The database is named at run time, since it differs between production, dev and the tests.
DO $$
DECLARE
  r text;
BEGIN
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO proteindb_accession, proteindb_load', current_database());
  -- Without the right to grant it, the GRANT only warns.
  FOREACH r IN ARRAY ARRAY['proteindb_accession', 'proteindb_load'] LOOP
    IF NOT has_database_privilege(r, current_database(), 'TEMPORARY') THEN
      RAISE EXCEPTION '% cannot create temporary tables in %', r, current_database();
    END IF;
  END LOOP;
  FOREACH r IN ARRAY ARRAY['proteindb_accession', 'proteindb_load', 'proteindb_read'] LOOP
    EXECUTE format('ALTER ROLE %I IN DATABASE %I SET search_path = proteindb', r, current_database());
  END LOOP;
END $$;

-- A client that hangs mid-transaction keeps its new hashes locked, and other jobs meeting them wait.
ALTER ROLE proteindb_accession CONNECTION LIMIT 256;
ALTER ROLE proteindb_accession SET idle_in_transaction_session_timeout = '10min';
ALTER ROLE proteindb_accession SET statement_timeout = '1h';
ALTER ROLE proteindb_accession SET work_mem = '256MB';
ALTER ROLE proteindb_accession SET temp_buffers = '512MB';
ALTER ROLE proteindb_load      SET work_mem = '1GB';
