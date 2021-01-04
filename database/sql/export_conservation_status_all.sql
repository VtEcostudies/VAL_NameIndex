copy (select
--c."taxonId" as "catalogNumber",
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
--v."vernacularName",
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
--inner join val_vernacular v on c."taxonId"=v."taxonId"
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_all.txt'
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;
