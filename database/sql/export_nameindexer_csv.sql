-- USE THIS FILE FOR REGULAR EXPORT OF NAME-INDEXER
-- NOTE: to successfully copy, pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
-- Hm. The below should not be necessary, but for some reason I did this. Let's try the whole downstream process
-- without it and hope it works.
-- NOTE: you must post-process the output to convert NULL fields (|) to empty double-quotes ("").
-- Postgres COPY command NEVER quotes NULL values, and won't allow NULL export as ("").
copy (select
	"taxonId" AS "id",
	"taxonId",
	"scientificName",
	"scientificNameAuthorship",
	"taxonRank",
	"taxonomicStatus",
	"acceptedNameUsageId",
	"acceptedNameUsage",
	"parentNameUsageId",
	"nomenclaturalCode",
	"specificEpithet",
	"infraspecificEpithet",
	"taxonRemarks",
	"vernacularName",
	"datasetName",
	"datasetId",
	"kingdom",
	"phylum",
	"class",
	"order",
	"family",
	"genus",
	"species",
    "bibliographicCitation",
    "references",
    "establishmentMeans",
    "institutionCode",
    "collectionCode"
	from val_species)
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_species.csv'
WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *);
--WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');

copy (select
"taxonId" as "id",
"taxonId",
"scientificName",
"vernacularName",
"lifeStage",
"sex",
"language",
"countryCode",
"source",
"preferred"
from val_vernacular)
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_vernacular.csv' 
WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *);
--WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');