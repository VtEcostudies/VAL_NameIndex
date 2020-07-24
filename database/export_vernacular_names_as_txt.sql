copy (select
v."taxonId",
v."scientificName",
v."vernacularName",
s."kingdom",
s."family"
from val_vernacular v
inner join val_species s
on v."taxonId"=s."taxonId"
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_vernacular.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;
