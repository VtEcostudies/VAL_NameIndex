--NOTE: to successfully copy, pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy (select
	"taxonKey" AS "id",
	"taxonKey" AS "taxonId",
	"canonicalName" AS "scientificName",
	"authorship" AS "scientificNameAuthorship",
	"taxonRank",
	"taxonomicStatus",
	"acceptedTaxonKey" AS "acceptedNameUsageId",
	"acceptedScientificName" AS "acceptedNameUsage",
	"parentKey" AS "parentNameUsageId",
	"parent" AS "parentNameUsage",
	"nomenclaturalCode",
	"specificEpithet",
	"infraspecificEpithet",
	"taxonRemarks",
	"vernacularName",
	--"datasetName",
	--"datasetId",
	"kingdom",
	"phylum",
	"class",
	"order",
	"family",
	"genus",
	"species"
    --"bibliographicCitation",
    --"references",
    --"establishmentMeans",
    --"institutionCode",
    --"collectionCode"
from val_species_from_occs)
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_species_from_occs.csv'
WITH (FORMAT CSV, HEADER TRUE);

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
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_vernacular.csv' 
WITH (FORMAT CSV, HEADER TRUE);
--WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');

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
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_conservation_status.csv' 
WITH (FORMAT CSV, HEADER TRUE);
--WITH (FORMAT CSV, HEADER TRUE, FORCE_QUOTE *, NULL '|');
