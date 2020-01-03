select * from val_species 
where "scientificName" = '' AND "taxonId" = "acceptedNameUsageId";

update val_species 
set "scientificName"="acceptedNameUsage" 
where "scientificName" = '' AND "taxonId" = "acceptedNameUsageId";

select * from val_species 
where "scientificName" is null or "scientificName" = '';