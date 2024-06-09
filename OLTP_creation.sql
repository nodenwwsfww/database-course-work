CREATE TABLE "User" (
                        "UserID" SERIAL PRIMARY KEY,
                        "Username" VARCHAR(255) NOT NULL,
                        "Password" VARCHAR(255) NOT NULL,
                        "Email" VARCHAR(255) NOT NULL,
                        "Role" VARCHAR(50) NOT NULL
);

CREATE TABLE "Character" (
                             "CharacterID" SERIAL PRIMARY KEY,
                             "UserID" INT NOT NULL,
                             "Name" VARCHAR(255) NOT NULL,
                             "Age" INT NOT NULL,
                             "Occupation" VARCHAR(255) NOT NULL,
                             FOREIGN KEY ("UserID") REFERENCES "User"("UserID")
);

CREATE INDEX idx_userid ON "Character" ("UserID");

CREATE TABLE "Vehicle" (
                           "VehicleID" SERIAL PRIMARY KEY,
                           "CharacterID" INT NOT NULL,
                           "Model" VARCHAR(255) NOT NULL,
                           "LicensePlate" VARCHAR(255) NOT NULL,
                           "Status" VARCHAR(50) NOT NULL,
                           FOREIGN KEY ("CharacterID") REFERENCES "Character"("CharacterID")
);

CREATE INDEX idx_characterid_vehicle ON "Vehicle" ("CharacterID");

CREATE TABLE "Property" (
                            "PropertyID" SERIAL PRIMARY KEY,
                            "CharacterID" INT NOT NULL,
                            "Address" VARCHAR(255) NOT NULL,
                            "Value" DECIMAL(10, 2) NOT NULL,
                            FOREIGN KEY ("CharacterID") REFERENCES "Character"("CharacterID")
);

CREATE INDEX idx_characterid_property ON "Property" ("CharacterID");

CREATE TABLE "Transaction" (
                               "TransactionID" SERIAL PRIMARY KEY,
                               "CharacterID" INT NOT NULL,
                               "Amount" DECIMAL(10, 2) NOT NULL,
                               "Date" TIMESTAMP NOT NULL,
                               "Type" VARCHAR(50) NOT NULL,
                               FOREIGN KEY ("CharacterID") REFERENCES "Character"("CharacterID")
);

CREATE INDEX idx_characterid_transaction ON "Transaction" ("CharacterID");

CREATE TABLE "Job" (
                       "JobID" SERIAL PRIMARY KEY,
                       "Name" VARCHAR(255) NOT NULL,
                       "Description" VARCHAR(255) NOT NULL
);

CREATE TABLE "CharacterJob" (
                                "CharacterJobID" SERIAL PRIMARY KEY,
                                "CharacterID" INT NOT NULL,
                                "JobID" INT NOT NULL,
                                FOREIGN KEY ("CharacterID") REFERENCES "Character"("CharacterID"),
                                FOREIGN KEY ("JobID") REFERENCES "Job"("JobID")
);

CREATE INDEX idx_characterid_characterjob ON "CharacterJob" ("CharacterID");
CREATE INDEX idx_jobid_characterjob ON "CharacterJob" ("JobID");

CREATE TABLE "Inventory" (
                             "InventoryID" SERIAL PRIMARY KEY,
                             "CharacterID" INT NOT NULL,
                             "ItemName" VARCHAR(255) NOT NULL,
                             "Quantity" INT NOT NULL,
                             FOREIGN KEY ("CharacterID") REFERENCES "Character"("CharacterID")
);

CREATE INDEX idx_characterid_inventory ON "Inventory" ("CharacterID");

CREATE TABLE "Log" (
                       "LogID" SERIAL PRIMARY KEY,
                       "UserID" INT NOT NULL,
                       "Action" VARCHAR(255) NOT NULL,
                       "Timestamp" TIMESTAMP NOT NULL,
                       FOREIGN KEY ("UserID") REFERENCES "User"("UserID")
);

CREATE INDEX idx_userid_log ON "Log" ("UserID");
