--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
--NOTE: you must post-process the output to convert NULL fields (|) to empty double-quotes ("").
--Postgres COPY command NEVER quotes NULL values, and won't allow NULL export as ("").
--NOTE: TO IMPORT VERNACULAR LIST INTO ALA BIE USING THE ADMIN TOOLS AT THE URL 
-- 'https://bie-ws.vtatlasoflife.org/admin/import/specieslist'
-- AND A CALL TO 'Import vernacular name species lists', you MUST alter the file 'vernacular-lists-val.json' and verify
-- that the value of the field "vernacularNameField" is equal to the resultant field name in the Species List Tool. Eg.
/*
  "lists": [
    {
      "uid": "drt1632393133806",
      "vernacularNameField": "vernacular name",
      "isPreferredField": "preferred",
      "taxonRemarksField": "taxonRemarks",
      "defaultLanguage": "en",
      "defaultStatus": "common"
    }
*/
copy (select
	"taxonId" as "id",
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
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_species.txt' 
with (FORMAT CSV, DELIMITER E'\t', HEADER TRUE, FORCE_QUOTE *, NULL '|');

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
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_vernacular.txt'
with (FORMAT CSV, DELIMITER E'\t', HEADER TRUE, FORCE_QUOTE *, NULL '|');

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
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_conservation_status.txt'
with (FORMAT CSV, DELIMITER E'\t', HEADER TRUE, FORCE_QUOTE *, NULL '|');