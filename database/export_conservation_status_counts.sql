--ALL S1s and S2s
--
copy (
select 1 as order_id, 'SGCN' as "Status_Type", count("taxonId") from val_conservation_status where "SGCN"='t'
union
select 2 as order_id, 'Listed Threatened' as "Status_Type", count("taxonId") from val_conservation_status where "stateList"[1]='T'
union
select 3 as order_id, 'Listed Endangered' as "Status_Type", count("taxonId") from val_conservation_status where "stateList"[1]='E'
union
select 4 as order_id, 'Rank Critically Imperiled' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1]='S1'
union
select 5 as order_id, 'Rank Critically Imperiled OR Imperiled' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1]='S1S2'
union
select 6 as order_id, 'Rank Imperiled' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1]='S2'
union
select 7 as order_id, 'Rank Imperiled OR Vulnerable' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1]='S2S3'
union
select 8 as order_id, 'Rank Vulnerable' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1]='S3'
union
select 9 as order_id, 'Rank Vulnerable OR Apparently Secure' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1]='S3S4'
union
select 10 as order_id, 'Rank Possibly Extirpated' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1] LIKE 'SH%'
union
select 11 as order_id, 'Rank Presumed Extirpated' as "Status_Type", count("taxonId") from val_conservation_status where "stateRank"[1] LIKE 'SX%'
order by order_id
)
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\conservation-status\val_conservation_status_counts.csv' 
delimiter ','
csv header;

--select "taxonId", "stateList" from val_conservation_status where "stateList"[2] is not null;
--update val_conservation_status set "stateList"[2]='RT' where "taxonId"='2480446';
--update val_conservation_status set "stateList"[2]='RE' where "taxonId"='2287890';
--update val_conservation_status set "stateList"[2]='RDL' where "taxonId"='7258874';