--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
--NOTE: you must post-process the output to convert NULL fields (|) to empty double-quotes ("").
--	Postgres COPY command NEVER quotes NULL values, and won't allow NULL export as ("").
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
with (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');

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
with (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');

copy (select
"taxonId" as "id",
"taxonId",
"scientificName",
"SGCN",
"stateRank",
"stateList",
"globalRank",
"federalList"
from val_conservation_status)
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_conservation_status.csv' 
with (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');
