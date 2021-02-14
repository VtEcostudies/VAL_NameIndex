select * from val_species where "taxonId" like 'VTSR:4';
update val_species set "genusId"='3256102', "parentNameUsageId"='3256102' where "taxonId"='VTSR:4';
select * from val_species where "taxonId" like 'VTSR:4';