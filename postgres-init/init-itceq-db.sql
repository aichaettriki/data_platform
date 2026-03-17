-- Create a dedicated DB for your Spark → PostgreSQL pipeline
CREATE DATABASE itceq_db;

\connect icteq_db;

-- Create the empty table that Spark will overwrite later
DROP TABLE IF EXISTS equipe;

CREATE TABLE equipe (
    nom TEXT,
    prenom TEXT,
    number INTEGER,
    fullname TEXT,
    timestamp TIMESTAMP
);
