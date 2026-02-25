CREATE USER iceberg WITH ENCRYPTED PASSWORD 'iceberg';
CREATE DATABASE iceberg;
ALTER DATABASE iceberg OWNER TO iceberg;

-- CREATE USER superset WITH PASSWORD 'superset';
-- CREATE DATABASE superset OWNER superset;
-- ALTER SCHEMA public OWNER TO superset;