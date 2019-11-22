--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy val_species(
	"gbifId",
	"taxonId",
	"scientificName",
	"acceptedNameUsageId",
	"acceptedNameUsage",
	"parentNameUsageId",
	"taxonRank",
	"taxonomicStatus",
	"nomenclaturalCode",
	"scientificNameAuthorship",
	"specificEpithet",
	"infraspecificEpithet",
	"vernacularName",
	"taxonRemarks",
	"datasetName",
	"datasetId",
	"kingdom",
	"phylum",
	"class",
	"order",
	"family",
	"genus"
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_species_pg_export.csv' 
delimiter ','
csv header