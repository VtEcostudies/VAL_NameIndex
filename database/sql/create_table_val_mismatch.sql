--DROP TABLE IF EXISTS val_mismatch;

CREATE TABLE val_mismatch
(
id int,
"taxonId" int,
"scientificName" text,
"acceptedNameUsageID" int,
"acceptedNameUsage" text,
"taxonRank" text,
"taxonomicStatus" text,
"parentNameUsageID" int,
"nomenclaturalCode" text,
"scientificNameAuthorship" text,
"specificEpithet" text,
"infraSpecificEpithet" text,
"vernacularName" text,
"taxonRemarks" text,
"datasetName" text,
"datasetID" text
);