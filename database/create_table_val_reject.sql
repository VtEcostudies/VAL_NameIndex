DROP TABLE IF EXISTS val_reject;

CREATE TABLE val_reject
(
"rejectReason" VARCHAR NOT NULL,
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
"speciesId" INTEGER
);

ALTER TABLE val_reject OWNER to VAL;