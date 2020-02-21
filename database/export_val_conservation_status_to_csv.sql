
copy (select
"taxonId",
"taxonId",
"scientificName",
"SGCN",
"stateRank",
"stateList",
"globalRank",
"federalList"
from val_conservation_status)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_conservation_status.csv' delimiter ',' csv header;