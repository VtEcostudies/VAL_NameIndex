--DROP TABLE IF EXISTS val_distribution;

--CREATE TYPE val_location_type AS ENUM ('state', 'county', 'town', 'village', 'biophysical_region');

CREATE TABLE val_distribution
(
"distributionId" SERIAL UNIQUE PRIMARY KEY,
"taxonId" VARCHAR NOT NULL,
"stateRank" VARCHAR,
"federalListing" VARCHAR,
"sgcn" BOOLEAN,
"locationType" val_location_type,
"locationId" VARCHAR,
"locality" VARCHAR,
"countryCode" VARCHAR,
"lifeStage" VARCHAR,
"occurrenceStatus" VARCHAR,
"threatStatus" VARCHAR,
"establishmentMeans" VARCHAR,
"appendixCITES" VARCHAR,
"eventDate" DATE,
"startDayOfYear" INTEGER, 
"endDayOfYear" INTEGER,
"source" VARCHAR,
"occurrenceRemarks" VARCHAR,
"datasetId" VARCHAR,
"createdAt" timestamp without time zone DEFAULT now(),
"updatedAt" timestamp without time zone DEFAULT now(),
CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES val_species ("taxonId")
);

ALTER TABLE val_distribution OWNER to VAL;

--alter table val_species add column "createdAt" timestamp without time zone DEFAULT now();
--alter table val_species add column "updatedAt" timestamp without time zone DEFAULT now();
--create generic trigger function to set "updatedAt"=now() for each table having that column
CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS trigger
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
   NEW."updatedAt" = now(); 
   RETURN NEW;
END;
$BODY$;
--create triggers for each table having the column "updatedAt"
CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE 
    ON val_distribution
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();