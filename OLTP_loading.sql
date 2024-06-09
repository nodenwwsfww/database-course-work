CREATE OR REPLACE FUNCTION load_data_from_csv(
    users_file_path TEXT,
    characters_file_path TEXT,
    vehicles_file_path TEXT,
    properties_file_path TEXT,
    transactions_file_path TEXT,
    jobs_file_path TEXT,
    character_jobs_file_path TEXT,
    inventories_file_path TEXT,
    logs_file_path TEXT
)
    RETURNS VOID AS $$
BEGIN
    -- Create temporary tables
    CREATE TEMP TABLE TempUsers (
                                    "Username" VARCHAR(255),
                                    "Password" VARCHAR(255),
                                    "Email" VARCHAR(255),
                                    "Role" VARCHAR(50)
    );

    CREATE TEMP TABLE TempCharacters (
                                         "UserName" VARCHAR(255),
                                         "Name" VARCHAR(255),
                                         "Age" INT,
                                         "Occupation" VARCHAR(255)
    );

    CREATE TEMP TABLE TempVehicles (
                                       "CharacterName" VARCHAR(255),
                                       "Model" VARCHAR(255),
                                       "LicensePlate" VARCHAR(255),
                                       "Status" VARCHAR(50)
    );

    CREATE TEMP TABLE TempProperties (
                                         "CharacterName" VARCHAR(255),
                                         "Address" VARCHAR(255),
                                         "Value" DECIMAL(10, 2)
    );

    CREATE TEMP TABLE TempTransactions (
                                           "CharacterName" VARCHAR(255),
                                           "Amount" DECIMAL(10, 2),
                                           "Date" TIMESTAMP,
                                           "Type" VARCHAR(50)
    );

    CREATE TEMP TABLE TempJobs (
                                   "Name" VARCHAR(255),
                                   "Description" VARCHAR(255)
    );

    CREATE TEMP TABLE TempCharacterJobs (
                                            "CharacterName" VARCHAR(255),
                                            "JobName" VARCHAR(255)
    );

    CREATE TEMP TABLE TempInventories (
                                          "CharacterName" VARCHAR(255),
                                          "ItemName" VARCHAR(255),
                                          "Quantity" INT
    );

    CREATE TEMP TABLE TempLogs (
                                   "UserName" VARCHAR(255),
                                   "Action" VARCHAR(255),
                                   "Timestamp" TIMESTAMP
    );

    -- Load data into temporary tables
    EXECUTE format('COPY TempUsers FROM %L DELIMITER '','' CSV HEADER', users_file_path);
    EXECUTE format('COPY TempCharacters FROM %L DELIMITER '','' CSV HEADER', characters_file_path);
    EXECUTE format('COPY TempVehicles FROM %L DELIMITER '','' CSV HEADER', vehicles_file_path);
    EXECUTE format('COPY TempProperties FROM %L DELIMITER '','' CSV HEADER', properties_file_path);
    EXECUTE format('COPY TempTransactions FROM %L DELIMITER '','' CSV HEADER', transactions_file_path);
    EXECUTE format('COPY TempJobs FROM %L DELIMITER '','' CSV HEADER', jobs_file_path);
    EXECUTE format('COPY TempCharacterJobs FROM %L DELIMITER '','' CSV HEADER', character_jobs_file_path);
    EXECUTE format('COPY TempInventories FROM %L DELIMITER '','' CSV HEADER', inventories_file_path);
    EXECUTE format('COPY TempLogs FROM %L DELIMITER '','' CSV HEADER', logs_file_path);

    -- Insert into final tables if not exists
    INSERT INTO "User" ("Username", "Password", "Email", "Role")
    SELECT "Username", "Password", "Email", "Role"
    FROM TempUsers
    WHERE NOT EXISTS (
        SELECT 1
        FROM "User" u
        WHERE u."Username" = TempUsers."Username"
    );

    INSERT INTO "Character" ("UserID", "Name", "Age", "Occupation")
    SELECT u."UserID", t."Name", t."Age", t."Occupation"
    FROM TempCharacters t
             JOIN "User" u ON u."Username" = t."UserName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Character" c
        WHERE c."Name" = t."Name"
    );

    INSERT INTO "Vehicle" ("CharacterID", "Model", "LicensePlate", "Status")
    SELECT c."CharacterID", t."Model", t."LicensePlate", t."Status"
    FROM TempVehicles t
             JOIN "Character" c ON c."Name" = t."CharacterName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Vehicle" v
        WHERE v."LicensePlate" = t."LicensePlate"
    );

    INSERT INTO "Property" ("CharacterID", "Address", "Value")
    SELECT c."CharacterID", t."Address", t."Value"
    FROM TempProperties t
             JOIN "Character" c ON c."Name" = t."CharacterName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Property" p
        WHERE p."Address" = t."Address"
    );

    INSERT INTO "Transaction" ("CharacterID", "Amount", "Date", "Type")
    SELECT c."CharacterID", t."Amount", t."Date", t."Type"
    FROM TempTransactions t
             JOIN "Character" c ON c."Name" = t."CharacterName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Transaction" tr
        WHERE tr."Date" = t."Date" AND tr."CharacterID" = c."CharacterID"
    );

    INSERT INTO "Job" ("Name", "Description")
    SELECT t."Name", t."Description"
    FROM TempJobs t
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Job" j
        WHERE j."Name" = t."Name"
    );

    INSERT INTO "CharacterJob" ("CharacterID", "JobID")
    SELECT c."CharacterID", j."JobID"
    FROM TempCharacterJobs t
             JOIN "Character" c ON c."Name" = t."CharacterName"
             JOIN "Job" j ON j."Name" = t."JobName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "CharacterJob" cj
        WHERE cj."CharacterID" = c."CharacterID" AND cj."JobID" = j."JobID"
    );

    INSERT INTO "Inventory" ("CharacterID", "ItemName", "Quantity")
    SELECT c."CharacterID", t."ItemName", t."Quantity"
    FROM TempInventories t
             JOIN "Character" c ON c."Name" = t."CharacterName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Inventory" i
        WHERE i."CharacterID" = c."CharacterID" AND i."ItemName" = t."ItemName"
    );

    INSERT INTO "Log" ("UserID", "Action", "Timestamp")
    SELECT u."UserID", t."Action", t."Timestamp"
    FROM TempLogs t
             JOIN "User" u ON u."Username" = t."UserName"
    WHERE NOT EXISTS (
        SELECT 1
        FROM "Log" l
        WHERE l."UserID" = u."UserID" AND l."Timestamp" = t."Timestamp"
    );

    -- Drop temporary tables
    DROP TABLE TempUsers;
    DROP TABLE TempCharacters;
    DROP TABLE TempVehicles;
    DROP TABLE TempProperties;
    DROP TABLE TempTransactions;
    DROP TABLE TempJobs;
    DROP TABLE TempCharacterJobs;
    DROP TABLE TempInventories;
    DROP TABLE TempLogs;

END;
$$ LANGUAGE plpgsql;

-- Call the function with absolute paths
SELECT load_data_from_csv(
               'C:/Users/noden/Desktop/sql/users.csv',
               'C:/Users/noden/Desktop/sql/characters.csv',
               'C:/Users/noden/Desktop/sql/vehicles.csv',
               'C:/Users/noden/Desktop/sql/properties.csv',
               'C:/Users/noden/Desktop/sql/transactions.csv',
               'C:/Users/noden/Desktop/sql/jobs.csv',
               'C:/Users/noden/Desktop/sql/character_jobs.csv',
               'C:/Users/noden/Desktop/sql/inventories.csv',
               'C:/Users/noden/Desktop/sql/logs.csv'
       );
