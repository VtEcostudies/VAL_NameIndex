SELECT
    "scientificName", "taxonRank", COUNT(*) as dupes
FROM
    val_species
GROUP BY
    "scientificName", "taxonRank"
HAVING 
    COUNT(*) > 1
	AND "taxonRank" = 'phylum'
ORDER BY 
	dupes desc, "taxonRank" desc;

--oops: deleted bad Arthropoda phylums before fixing bad parentNameUsageIds
select * from val_species where "taxonRank"='phylum' and "scientificName"='Arthropoda';
select min("phylumId"::INT) from val_species where "scientificName"='Arthropoda' and "taxonRank"='phylum'; --54
select * from val_species where "phylum"='Arthropoda' and "phylumId" != '54';
--update val_species set "phylumId" = 54 where "phylum" = 'Arthropoda' and "phylumId" != '54';
--delete from val_species where "taxonRank"='phylum' and "scientificName"='Arthropoda' and "taxonId" != '54';
select * from val_species where "acceptedNameUsage" = 'Arthropoda'; --1, itself

--oops: deleted bad Bryophyta phylums before fixing bad parentNameUsageIds
select * from val_species where "scientificName"='Bryophyta' and "taxonRank"='phylum';
select min("phylumId"::INT) from val_species where "scientificName"='Bryophyta' and "taxonRank"='phylum'; --35
select * from val_species where "phylumId"!='35' and "phylum"='Bryophyta';
--update val_species set "phylumId"='35' where "phylumId"!='35' and "phylum"='Bryophyta';
--delete from val_species where "taxonRank"='phylum' and "scientificName"='Bryophyta' and "taxonId"!='35';
select * from val_species where "acceptedNameUsage" = 'Bryophyta'; --2. delete the one below
--delete from val_species where "taxonId"='102988419';

--yup: did fix parentNameUsageIds for bad Tracheophyta
select * from val_species where "scientificName"='Tracheophyta' and "taxonRank"='phylum';
select min("phylumId"::INT) from val_species where "scientificName"='Tracheophyta' and "taxonRank"='phylum'; --7707728
select * from val_species where "phylumId"!='7707728' and "phylum"='Tracheophyta';
--update val_species set "phylumId"='7707728' where "phylumId"!='7707728' and "phylum"='Tracheophyta';
select * from val_species where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Tracheophyta' and "taxonRank"='phylum' and "taxonId" != '7707728'
)
--update val_species set "parentNameUsageId" = '7707728' where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Tracheophyta' and "taxonRank"='phylum' and "taxonId" != '7707728'
)
--delete from val_species where "taxonRank"='phylum' and "scientificName"='Tracheophyta' and "taxonId"!='7707728';
select * from val_species where "scientificName"='Tracheophyta' and "taxonRank"='phylum';
select * from val_species where "acceptedNameUsage" = 'Tracheophyta'; --2. delete the one below.
--delete from val_species where "taxonId"='103003101';

--yup: did fix parentNameUsageIds for bad Marchantiophyta
select * from val_species where "scientificName"='Marchantiophyta' and "taxonRank"='phylum';
select min("phylumId"::INT) from val_species where "scientificName"='Marchantiophyta' and "taxonRank"='phylum'; --9
select * from val_species where "phylumId"!='9' and "phylum"='Marchantiophyta';
--update val_species set "phylumId"='9' where "phylumId"!='9' and "phylum"='Marchantiophyta';
select * from val_species where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Marchantiophyta' and "taxonRank"='phylum' and "taxonId" != '9'
)
--update val_species set "parentNameUsageId" = '9' where "parentNameUsageId" in (
	select "taxonId" from val_species 
	where "scientificName"='Marchantiophyta' and "taxonRank"='phylum' and "taxonId" != '9'
)
--delete from val_species where "taxonRank"='phylum' and "scientificName"='Marchantiophyta' and "taxonId"!='9';
select * from val_species where "scientificName"='Marchantiophyta' and "taxonRank"='phylum';
select * from val_species where "acceptedNameUsage" = 'Marchantiophyta'; --2. delete the one below.
--delete from val_species where "taxonId"='102997495';
