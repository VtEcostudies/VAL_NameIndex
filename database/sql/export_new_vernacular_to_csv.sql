--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy (select
v."taxonId",
v."taxonId",
v."scientificName", --s."canonicalName",
v."vernacularName",
v."lifeStage",
v."sex",
v."language",
v."countryCode",
v."source",
v."preferred"
from new_vernacular v
inner join new_species s on s."taxonId"=v."taxonId"
)
to 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\ipt\new_vernacular.csv' delimiter ',' csv header;