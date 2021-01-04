copy (
select
s."taxonId",
s."scientificName",
--s."scientificNameAuthorship" as author,
v."vernacularName",
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList"
from val_species s
left join val_conservation_status c on s."taxonId"=c."taxonId"
left join val_vernacular v on s."taxonId"=v."taxonId"
where s."scientificName" IN ('Ambystoma maculatum','Catharus bicknelli')
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\favorites\val_favorites.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;
