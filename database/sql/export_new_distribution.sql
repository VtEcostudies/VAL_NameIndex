--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
--gbifId,establishmentMeans,locationID,locality,CountryCode
--5389012,,ISO3166-2:US-VT,Vermont-US,US
copy (select
--"taxonId",
"taxonId" AS "gbifID",
null AS "establishmentMeans",
'ISO3166-2:US-VT' AS "locationID",
'Vermont-US' AS "locality",
'US' AS "CountryCode"
from new_species)
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\ipt\new_distribution.csv' delimiter ',' csv header;