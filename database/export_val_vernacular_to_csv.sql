--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy val_vernacular(
"taxonId",
"scientificName",
"vernacularName",
"lifeStage",
"sex",
"countryCode",
"language",
"source"
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_vernacular.csv' 
delimiter ','
csv header