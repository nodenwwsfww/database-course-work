-- view all extensions
SELECT * FROM pg_extension;

-- Install the required extension

CREATE EXTENSION IF NOT EXISTS postgres_fdw;


-- Create a foreign server that connects to 'datawarehouse'
CREATE SERVER same_server_postgres
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', dbname 'postgres', port '5432');

-- Create a user mapping for the current user
CREATE USER MAPPING FOR CURRENT_USER
    SERVER same_server_postgres
    OPTIONS (user 'postgres', password 'root');


--Import tables from the remote database into the local schema
IMPORT FOREIGN SCHEMA public
    FROM SERVER  same_server_postgres
    INTO public;

-- Create Dimensions
CREATE TABLE DimUser (
                         user_id SERIAL PRIMARY KEY,
                         username VARCHAR(255) NOT NULL,
                         email VARCHAR(255) NOT NULL,
                         role VARCHAR(50) NOT NULL
);

CREATE TABLE DimCharacter (
                              character_id SERIAL PRIMARY KEY,
                              user_id INT NOT NULL,
                              name VARCHAR(255) NOT NULL,
                              age INT NOT NULL,
                              occupation VARCHAR(255) NOT NULL,
                              FOREIGN KEY (user_id) REFERENCES DimUser(user_id)
);

CREATE TABLE DimJob (
                        job_id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        description VARCHAR(255) NOT NULL
);

CREATE TABLE DimItem (
                         item_id SERIAL PRIMARY KEY,
                         item_name VARCHAR(255) NOT NULL
);

CREATE TABLE DimDate (
                         date_id SERIAL PRIMARY KEY,
                         date DATE,
                         day INT,
                         month INT,
                         year INT,
                         quarter INT,
                         week_of_year INT
);

-- Fact Tables
CREATE TABLE FactTransactions (
                                  transaction_id SERIAL PRIMARY KEY,
                                  character_id INT NOT NULL,
                                  amount DECIMAL(10, 2) NOT NULL,
                                  date_id INT NOT NULL,
                                  type VARCHAR(50) NOT NULL,
                                  FOREIGN KEY (character_id) REFERENCES DimCharacter(character_id),
                                  FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

CREATE TABLE FactCharacterJobs (
                                   character_job_id SERIAL PRIMARY KEY,
                                   character_id INT NOT NULL,
                                   job_id INT NOT NULL,
                                   date_id INT NOT NULL,
                                   FOREIGN KEY (character_id) REFERENCES DimCharacter(character_id),
                                   FOREIGN KEY (job_id) REFERENCES DimJob(job_id),
                                   FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

CREATE TABLE FactInventory (
                               inventory_id SERIAL PRIMARY KEY,
                               character_id INT NOT NULL,
                               item_id INT NOT NULL,
                               quantity INT NOT NULL,
                               date_id INT NOT NULL,
                               FOREIGN KEY (character_id) REFERENCES DimCharacter(character_id),
                               FOREIGN KEY (item_id) REFERENCES DimItem(item_id),
                               FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

-- SCD Type 2 for DimJob
ALTER TABLE DimJob
    ADD COLUMN start_date TIMESTAMP,
    ADD COLUMN end_date TIMESTAMP,
    ADD COLUMN current_flag BOOLEAN DEFAULT TRUE;

-- Trigger function to handle SCD Type 2
CREATE OR REPLACE FUNCTION DimJob_update_trigger()
    RETURNS TRIGGER AS $$
BEGIN
    IF (OLD.name <> NEW.name OR OLD.description <> NEW.description) AND OLD.current_flag AND NEW.current_flag THEN
        UPDATE DimJob
        SET end_date = current_timestamp,
            current_flag = FALSE
        WHERE job_id = OLD.job_id AND current_flag = TRUE;

        INSERT INTO DimJob (name, description, start_date, end_date, current_flag)
        VALUES (NEW.name, NEW.description, current_timestamp, '9999-12-31', TRUE);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER DimJob_update
    AFTER UPDATE ON DimJob
    FOR EACH ROW
EXECUTE FUNCTION DimJob_update_trigger();
