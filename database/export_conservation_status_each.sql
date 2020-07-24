copy (
select
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList",
c."stateList"[1] as status,
case
	when c."stateList"[1] like 'T%' then 'Threatened' 
	when c."stateList"[1] like 'E%' then 'Endangered' 
	else 'Other' 
end as "statusSource"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
where "stateList" is not null
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_listed.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;

copy (select
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList",
c."stateRank"[1] as status,
'Critically Imperiled' as "statusSource"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
where "stateRank"[1]='S1' or "stateRank"[1]='S1S2'
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_rank_S1_S1S2.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;

copy (select
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList",
c."stateRank"[1] as status,
'Imperiled' as "statusSource"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
where "stateRank"[1]='S2' or "stateRank"[1]='S2S3'
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_rank_S23_S2S3.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;

copy (select
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList",
c."stateRank"[1] as status,
'Vulnerable' as "statusSource"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
where "stateRank"[1]='S3' or "stateRank"[1]='S3S4'
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_rank_S3_S3S4.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;

copy (select
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList",
c."stateRank"[1] as status,
case
	when c."stateRank"[1] like 'SH%' then 'Possibly extinct/extirpated' 
	when c."stateRank"[1] like 'SX%' then 'Presumed extinct/extirpated' 
	else 'Other' 
end as "statusSource"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
where "stateRank"[1] like 'SH%' or "stateRank"[1] like 'SX%'
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_rank_SH_SX.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;

copy (select
c."taxonId",
c."scientificName",
--s."scientificNameAuthorship" as author,
s."kingdom",
s."family",
c."SGCN",
c."stateRank",
c."stateList",
c."globalRank",
c."federalList",
'SGCN' as status,
'Species of Greatest Conservation Need' as "statusSource"
from val_conservation_status c
inner join val_species s
on c."taxonId"=s."taxonId"
where "SGCN"='t'
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_SGCN.txt' 
with NULL '' DELIMITER E'\t' QUOTE '"' FORCE QUOTE * HEADER CSV;
