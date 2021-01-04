update val_species set "scientificName"='Animalia' where "gbifId"=1;
update val_species set "scientificName"='Plantae' where "gbifId"=6;
update val_species set "phylum"=null, "phylumId"=0 where "gbifId"=6;
update val_species set "parentNameUsageId"="taxonId" where "taxonRank"='kingdom';

select * from val_species where "taxonRank"='kingdom' order by "gbifId";