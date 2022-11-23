--DROP TABLE IF EXISTS new_species;

CREATE TABLE new_species
(
"key" INTEGER NOT NULL,
"nubKey" INTEGER NOT NULL,
"taxonId" VARCHAR NOT NULL,
"scientificName" VARCHAR NOT NULL,
"canonicalName" VARCHAR NOT NULL,
"scientificNameAuthorship" VARCHAR,
"acceptedNameUsageId" VARCHAR NOT NULL,
"acceptedNameUsage" VARCHAR NOT NULL,
"parentNameUsageId" VARCHAR NOT NULL,
"parentNameUsage" VARCHAR,
"taxonRank" VARCHAR NOT NULL,
"taxonomicStatus" VARCHAR,
"nomenclaturalCode" VARCHAR,
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
"establishmentMeans" VARCHAR,
"createdAt" timestamp without time zone DEFAULT now(),
"updatedAt" timestamp without time zone DEFAULT now(),
UNIQUE ("taxonId"),
PRIMARY KEY ("taxonId")
);

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
    ON new_species
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();
