-- 1. Création de la base de données dédiée aux métadonnées Hive
CREATE DATABASE metastore_db
    WITH 
    OWNER = airflow
    ENCODING = 'UTF8';

-- 2. Attribution de tous les privilèges à l'utilisateur 'airflow'
-- (C'est cet utilisateur qui est défini dans votre docker-compose pour le service hive-metastore)
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO airflow;