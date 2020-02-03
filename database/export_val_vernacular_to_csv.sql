--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy val_conservation_status(
"taxonId",
"scientificName",
"SGCN",
"stateRank",
"stateList",
"globalRank",
"federalList"
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_conservation_status.csv' 
delimiter ','
csv header