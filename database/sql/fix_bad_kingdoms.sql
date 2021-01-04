select * from val_species where "scientificName" like '%incertae sedis%';
update val_species set "taxonId"='-1', "gbifId"=-1, "acceptedNameUsageId"='-1', "parentNameUsageId"='-1' where "scientificName"='incertae sedis';

select * from val_species where "taxonRank" = 'kingdom' order by "scientificName" desc;
select * from val_species where "taxonRank"='kingdom' and "parentNameUsageId"!="taxonId" order by "scientificName" desc;
select * from val_species where "taxonId"='132923727'; --has super kingdom
select * from val_species where "taxonId"='102974827';
select * from val_species where "taxonId"='103504663';
select * from val_species where "parentNameUsageId"='0' and "taxonRank"='kingdom';

update val_species set "parentNameUsageId"="taxonId" where "parentNameUsageId"='0' and "taxonRank"='kingdom';