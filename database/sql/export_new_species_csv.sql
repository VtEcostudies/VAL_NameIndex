--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy (select
	"taxonId",
	"taxonId",
	"canonicalName",
	"scientificNameAuthorship",
	"taxonRank",
	"taxonomicStatus",
	"acceptedNameUsageId",
	"acceptedNameUsage",
	"parentNameUsageId",
	"parentNameUsage",
	"nomenclaturalCode",
	"specificEpithet",
	"infraspecificEpithet",
	"taxonRemarks",
	"vernacularName",
	"datasetName",
	"datasetId",
    "bibliographicCitation",
    "references",
    "establishmentMeans"
    --,"institutionCode" --this field is not part of taxonCore
	from new_species)
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\ipt\new_species.csv' delimiter ',' csv header;