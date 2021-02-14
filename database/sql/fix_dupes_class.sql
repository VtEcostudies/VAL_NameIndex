SELECT
    "scientificName", "taxonRank", COUNT(*) as dupes
FROM
    val_species
GROUP BY
    "scientificName", "taxonRank"
HAVING 
    COUNT(*) > 1
	AND "taxonRank" = 'class'
ORDER BY 
	dupes desc, "taxonRank" desc;

select * from val_species where "scientificName"='Insecta' and "taxonRank"='class';
select min("classId"::INT) from val_species where "scientificName"='Insecta' and "taxonRank"='class'; --216
select * from val_species where "classId"!='216' and "class"='Insecta';
--update val_species set "classId"='216' where "classId"!='216' and "class"='Insecta';
select * from val_species where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Insecta' and "taxonRank"='class' and "taxonId" != '216'
)
--update val_species set "parentNameUsageId" = '216' where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Insecta' and "taxonRank"='class' and "taxonId" != '216'
)
--delete from val_species where "taxonRank"='class' and "scientificName"='Insecta' and "taxonId"!='216';
select * from val_species where "scientificName"='Insecta' and "taxonRank"='class';
select * from val_species where "acceptedNameUsage" = 'Insecta'; --1, itself

select * from val_species where "scientificName"='Bryopsida' and "taxonRank"='class';
select min("classId"::INT) from val_species where "scientificName"='Bryopsida' and "taxonRank"='class'; --327
select * from val_species where "classId"!='327' and "class"='Bryopsida';
--update val_species set "classId"='327' where "classId"!='327' and "class"='Bryopsida';
select * from val_species where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Bryopsida' and "taxonRank"='class' and "taxonId" != '327'
)
--update val_species set "parentNameUsageId" = '327' where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Bryopsida' and "taxonRank"='class' and "taxonId" != '327'
)
--delete from val_species where "taxonRank"='class' and "scientificName"='Bryopsida' and "taxonId"!='327';
select * from val_species where "scientificName"='Bryopsida' and "taxonRank"='class';
select * from val_species where "acceptedNameUsage" = 'Bryopsida'; --1, itself