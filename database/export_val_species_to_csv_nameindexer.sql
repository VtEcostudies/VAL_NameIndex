--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy val_species(
	"gbifId",
	"taxonId",
	"scientificName",
	"scientificNameAuthorship",
	"taxonRank",
	"taxonomicStatus",
	"acceptedNameUsageId",
	"acceptedNameUsage",
	"parentNameUsageId",
	"nomenclaturalCode",
	--"specificEpithet",
	--"infraspecificEpithet",
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
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\name-indexer\val_species.csv' 
delimiter ','
csv header