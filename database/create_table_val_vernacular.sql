DROP TABLE IF EXISTS val_vernacular;

CREATE TABLE val_vernacular
(
"vernacularId" SERIAL UNIQUE PRIMARY KEY,
"taxonId" VARCHAR NOT NULL,
"scientificName" VARCHAR NOT NULL,
"vernacularName" VARCHAR NOT NULL,
"lifeStage" VARCHAR,
"sex" VARCHAR,
"language" VARCHAR,
"countryCode" VARCHAR,
"source" VARCHAR,
"createdAt" timestamp without time zone DEFAULT now(),
"updatedAt" timestamp without time zone DEFAULT now(),
CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES val_species ("taxonId"),
CONSTRAINT "unique_taxonId_vernacularName" UNIQUE("taxonId","vernacularName")
);
--NOTE: apply unique constraint to pair of fields. We may have the same vernacular name applied to more than
--one taxonId.
--ALTER TABLE val_vernacular DROP CONSTRAINT "val_vernacular_vernacularName_key";
--ALTER TABLE val_vernacular ADD CONSTRAINT "unique_taxonId_vernacularName" UNIQUE("taxonId","vernacularName");

ALTER TABLE val_vernacular OWNER to VAL;

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
    ON val_vernacular
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();