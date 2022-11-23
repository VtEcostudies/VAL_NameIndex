--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy (select
"taxonId",
"taxonId",
"scientificName",
"vernacularName",
"lifeStage",
"sex",
"language",
"countryCode",
"source",
"preferred"
from new_vernacular)
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\new_vernacular.csv' delimiter ',' csv header;