--DROP TABLE IF EXISTS val_species;

CREATE TABLE val_species
(
"gbifId" INTEGER NOT NULL,
"taxonId" VARCHAR NOT NULL,
"scientificName" VARCHAR,
"acceptedNameUsageId" VARCHAR NOT NULL,
"acceptedNameUsage" VARCHAR NOT NULL,
"parentNameUsageId" VARCHAR NOT NULL,
"taxonRank" VARCHAR NOT NULL,
"taxonomicStatus" VARCHAR,
"nomenclaturalCode" VARCHAR,
"scientificNameAuthorship" VARCHAR,
"specificEpithet" VARCHAR,
"infraspecificEpithet" VARCHAR,
"vernacularName" VARCHAR,
"taxonRemarks" VARCHAR,
"datasetName" VARCHAR,
"datasetId" VARCHAR,
"kingdom" VARCHAR,
"kingdomId" INTEGER,
"phylum" VARCHAR,
"phylumId" INTEGER,
"class" VARCHAR,
"classId" INTEGER,
"order" VARCHAR,
"orderId" INTEGER,
"family" VARCHAR,
"familyId" INTEGER,
"genus" VARCHAR,
"genusId" INTEGER,
"species" VARCHAR,
"speciesId" INTEGER,
"bibliographicCitation" VARCHAR,
"references" VARCHAR,
"institutionCode" VARCHAR,
"collectionCode" VARCHAR,
"createdAt" timestamp without time zone DEFAULT now(),
"updatedAt" timestamp without time zone DEFAULT now(),
UNIQUE ("taxonId", "acceptedNameUsageId")--12,292 initial import
--UNIQUE ("taxonId", "acceptedNameUsageId", "parentNameUsageId", "specificEpithet", "infraspecificEpithet")--18,978 initial import
--PRIMARY KEY ("taxonId"), --by definition UNIQUE, so we can't use this?
--CONSTRAINT fk_accept_taxon_id FOREIGN KEY ("acceptedNameUsageId") REFERENCES val_species ("taxonId"),
--CONSTRAINT fk_parent_taxon_id FOREIGN KEY ("parentNameUsageId") REFERENCES val_species ("taxonId")
);

ALTER TABLE val_species OWNER to VAL;

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
    ON val_species
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();