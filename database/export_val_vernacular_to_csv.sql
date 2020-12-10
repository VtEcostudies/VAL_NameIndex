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
from val_vernacular)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_vernacular.csv' delimiter ',' csv header;