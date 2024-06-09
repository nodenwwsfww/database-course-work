CREATE OR REPLACE FUNCTION transferring_data()
    RETURNS void AS $$
BEGIN
    -- Debugging information
    RAISE NOTICE 'Starting ETL process';

    -- Transferring data from User to DimUser
    RAISE NOTICE 'Transferring data to DimUser';
    INSERT INTO DimUser (username, email, role)
    SELECT DISTINCT u."Username", u."Email", u."Role"
    FROM "User" u
             LEFT JOIN DimUser du ON u."Username" = du.username
    WHERE du.username IS NULL;

    -- Check if data is transferred to DimUser
    RAISE NOTICE 'DimUser count: %', (SELECT COUNT(*) FROM DimUser);

    -- Transferring data from Character to DimCharacter
    RAISE NOTICE 'Transferring data to DimCharacter';
    INSERT INTO DimCharacter (user_id, name, age, occupation)
    SELECT du.user_id, c."Name", c."Age", c."Occupation"
    FROM "Character" c
             JOIN "User" u ON c."UserID" = u."UserID"
             JOIN DimUser du ON u."Username" = du.username
             LEFT JOIN DimCharacter dc ON c."Name" = dc.name
    WHERE dc.name IS NULL;

    -- Check if data is transferred to DimCharacter
    RAISE NOTICE 'DimCharacter count: %', (SELECT COUNT(*) FROM DimCharacter);

    -- Transferring data from Job to DimJob
    RAISE NOTICE 'Transferring data to DimJob';
    INSERT INTO DimJob (name, description)
    SELECT DISTINCT j."Name", j."Description"
    FROM "Job" j
             LEFT JOIN DimJob dj ON j."Name" = dj.name
    WHERE dj.name IS NULL;

    -- Check if data is transferred to DimJob
    RAISE NOTICE 'DimJob count: %', (SELECT COUNT(*) FROM DimJob);

    -- Transferring data from Inventory to DimItem
    RAISE NOTICE 'Transferring data to DimItem';
    INSERT INTO DimItem (item_name)
    SELECT DISTINCT i."ItemName"
    FROM "Inventory" i
             LEFT JOIN DimItem di ON i."ItemName" = di.item_name
    WHERE di.item_name IS NULL;

    -- Check if data is transferred to DimItem
    RAISE NOTICE 'DimItem count: %', (SELECT COUNT(*) FROM DimItem);

    -- Ensure DimDate contains all necessary dates, including the current date
    RAISE NOTICE 'Ensuring DimDate contains all necessary dates';
    INSERT INTO DimDate (date, day, month, year, quarter, week_of_year)
    SELECT DISTINCT t."Date"::DATE,
                    EXTRACT(DAY FROM t."Date") AS day,
                    EXTRACT(MONTH FROM t."Date") AS month,
                    EXTRACT(YEAR FROM t."Date") AS year,
                    EXTRACT(QUARTER FROM t."Date") AS quarter,
                    EXTRACT(WEEK FROM t."Date") AS week_of_year
    FROM "Transaction" t
             LEFT JOIN DimDate dd ON t."Date"::DATE = dd.date
    WHERE dd.date IS NULL
    ORDER BY t."Date"::DATE;

    -- Ensure current date is in DimDate
    INSERT INTO DimDate (date, day, month, year, quarter, week_of_year)
    SELECT CURRENT_DATE,
           EXTRACT(DAY FROM CURRENT_DATE),
           EXTRACT(MONTH FROM CURRENT_DATE),
           EXTRACT(YEAR FROM CURRENT_DATE),
           EXTRACT(QUARTER FROM CURRENT_DATE),
           EXTRACT(WEEK FROM CURRENT_DATE)
    WHERE NOT EXISTS (SELECT 1 FROM DimDate WHERE date = CURRENT_DATE);

    -- Check if data is transferred to DimDate
    RAISE NOTICE 'DimDate count: %', (SELECT COUNT(*) FROM DimDate);

    -- Transferring data to FactTransactions
    RAISE NOTICE 'Transferring data to FactTransactions';
    INSERT INTO FactTransactions (character_id, amount, date_id, type)
    SELECT
        dc.character_id, t."Amount", dd.date_id, t."Type"
    FROM
        "Transaction" t
            JOIN
        DimCharacter dc ON t."CharacterID" = dc.character_id
            JOIN
        DimDate dd ON t."Date"::DATE = dd.date
            LEFT JOIN
        FactTransactions ft ON t."TransactionID" = ft.transaction_id
    WHERE
        ft.transaction_id IS NULL;

    -- Check if data is transferred to FactTransactions
    RAISE NOTICE 'FactTransactions count: %', (SELECT COUNT(*) FROM FactTransactions);

    -- Debugging: Check DimCharacter and DimJob content
    RAISE NOTICE 'DimCharacter content: %', array(
            SELECT ARRAY[dc.character_id::TEXT, dc.name]
            FROM DimCharacter dc
                                            );
    RAISE NOTICE 'DimJob content: %', array(
            SELECT ARRAY[dj.job_id::TEXT, dj.name]
            FROM DimJob dj
                                      );

    -- Step-by-step verification of join conditions for FactCharacterJobs
    -- Step 1: CharacterJob and DimCharacter
    RAISE NOTICE 'Join CharacterJob and DimCharacter: %', array(
            SELECT ARRAY[cj."CharacterID"::TEXT, dc.character_id::TEXT]
            FROM "CharacterJob" cj
                     JOIN DimCharacter dc ON cj."CharacterID" = dc.character_id
                                                          );

    -- Step 2: CharacterJob, DimCharacter, and DimJob
    RAISE NOTICE 'Join CharacterJob, DimCharacter, and DimJob: %', array(
            SELECT ARRAY[cj."CharacterID"::TEXT, dc.character_id::TEXT, cj."JobID"::TEXT, dj.job_id::TEXT]
            FROM "CharacterJob" cj
                     JOIN DimCharacter dc ON cj."CharacterID" = dc.character_id
                     JOIN DimJob dj ON cj."JobID" = dj.job_id
                                                                   );

    -- Step 3: CharacterJob, DimCharacter, DimJob, and DimDate
    RAISE NOTICE 'Join CharacterJob, DimCharacter, DimJob, and DimDate: %', array(
            SELECT ARRAY[cj."CharacterID"::TEXT, dc.character_id::TEXT, cj."JobID"::TEXT, dj.job_id::TEXT, dd.date_id::TEXT]
            FROM "CharacterJob" cj
                     JOIN DimCharacter dc ON cj."CharacterID" = dc.character_id
                     JOIN DimJob dj ON cj."JobID" = dj.job_id
                     JOIN DimDate dd ON CURRENT_DATE = dd.date
                                                                            );

    -- Transferring data to FactCharacterJobs
    RAISE NOTICE 'Transferring data to FactCharacterJobs';
    INSERT INTO FactCharacterJobs (character_id, job_id, date_id)
    SELECT
        dc.character_id, dj.job_id, dd.date_id
    FROM
        "CharacterJob" cj
            JOIN
        DimCharacter dc ON cj."CharacterID" = dc.character_id
            JOIN
        DimJob dj ON cj."JobID" = dj.job_id
            JOIN
        DimDate dd ON CURRENT_DATE = dd.date
    WHERE NOT EXISTS (
        SELECT 1 FROM FactCharacterJobs fcj
        WHERE fcj.character_id = dc.character_id
          AND fcj.job_id = dj.job_id
          AND fcj.date_id = dd.date_id
    );

    -- Additional debug info for FactCharacterJobs
    RAISE NOTICE 'CharacterJob data: %', array(
            SELECT ARRAY[cj."CharacterID"::TEXT, cj."JobID"::TEXT]
            FROM "CharacterJob" cj
                                         );

    RAISE NOTICE 'DimCharacter data: %', array(
            SELECT ARRAY[dc.character_id::TEXT, dc.name]
            FROM DimCharacter dc
                                         );

    RAISE NOTICE 'DimJob data: %', array(
            SELECT ARRAY[dj.job_id::TEXT, dj.name]
            FROM DimJob dj
                                   );

    RAISE NOTICE 'DimDate data for current date: %', array(
            SELECT ARRAY[dd.date_id::TEXT, dd.date::TEXT]
            FROM DimDate dd
            WHERE dd.date = CURRENT_DATE
                                                     );

    -- Check if data is transferred to FactCharacterJobs
    RAISE NOTICE 'FactCharacterJobs count: %', (SELECT COUNT(*) FROM FactCharacterJobs);

    -- Transferring data to FactInventory
    RAISE NOTICE 'Transferring data to FactInventory';
    INSERT INTO FactInventory (character_id, item_id, quantity, date_id)
    SELECT
        dc.character_id, di.item_id, i."Quantity", dd.date_id
    FROM
        "Inventory" i
            JOIN
        DimCharacter dc ON i."CharacterID" = dc.character_id
            JOIN
        DimItem di ON i."ItemName" = di.item_name
            JOIN
        DimDate dd ON CURRENT_DATE = dd.date
    WHERE NOT EXISTS (
        SELECT 1 FROM FactInventory fi
        WHERE fi.character_id = dc.character_id
          AND fi.item_id = di.item_id
          AND fi.date_id = dd.date_id
    );

    -- Additional debug info for FactInventory
    RAISE NOTICE 'Inventory data: %', array(
            SELECT ARRAY[i."CharacterID"::TEXT, i."ItemName"::TEXT]
            FROM "Inventory" i
                                      );

    RAISE NOTICE 'DimCharacter data: %', array(
            SELECT ARRAY[dc.character_id::TEXT, dc.name]
            FROM DimCharacter dc
                                         );

    RAISE NOTICE 'DimItem data: %', array(
            SELECT ARRAY[di.item_id::TEXT, di.item_name]
            FROM DimItem di
                                    );

    RAISE NOTICE 'DimDate data for current date: %', array(
            SELECT ARRAY[dd.date_id::TEXT, dd.date::TEXT]
            FROM DimDate dd
            WHERE dd.date = CURRENT_DATE
                                                     );

    -- Check if data is transferred to FactInventory
    RAISE NOTICE 'FactInventory count: %', (SELECT COUNT(*) FROM FactInventory);

    RAISE NOTICE 'ETL process completed';
END;
$$ LANGUAGE plpgsql;

-- Call the function
SELECT transferring_data();
