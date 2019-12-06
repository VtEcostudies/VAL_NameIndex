SELECT
    "taxonId", COUNT(*)
FROM
    val_species
GROUP BY
    "taxonId"
HAVING 
    COUNT(*) > 1;
select * from val_species where "taxonId"='4298895'
delete from val_species where "taxonId"='4298895' and "acceptedNameUsageId"='4298895';

select * from val_species where "taxonId"='2138089';
delete from val_species where "taxonId"='2138089' and "acceptedNameUsageId"='2138089';

select * from val_species where "taxonId"='2135998';
delete from val_species where "taxonId"='2135998' and "acceptedNameUsageId"='2135998';

select * from val_species where "taxonId"='4519719';
delete from val_species where "taxonId"='4519719' and "acceptedNameUsageId"='4519719';

select * from val_species where "taxonId"='4519733';
delete from val_species where "taxonId"='4519733' and "acceptedNameUsageId"='4519733';
