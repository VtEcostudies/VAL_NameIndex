--DROP TABLE IF EXISTS species_err;

CREATE TABLE species_err
(
"errId" SERIAL NOT NULL,
"key" INTEGER NOT NULL,
"nubKey" INTEGER NOT NULL,
"taxonId" VARCHAR NOT NULL,
"scientificName" VARCHAR NOT NULL,
"canonicalName" VARCHAR,
"errorCode" INTEGER NOT NULL,
"errorMessage" VARCHAR,
"errorObj" jsonb,
"inpFilePath" VARCHAR,
"inpFileLine" INTEGER,
"createdAt" timestamp without time zone DEFAULT now(),
"updatedAt" timestamp without time zone DEFAULT now(),
PRIMARY KEY ("errId"),
UNIQUE ("taxonId", "errorCode")
);

--create triggers for each table having the column "updatedAt"
CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE
    ON species_err
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();
