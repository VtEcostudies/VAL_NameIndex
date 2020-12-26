update val_species set 
"taxonomicStatus"='accepted',
"acceptedNameUsage"='Propylea quatuordecimpunctata',
"acceptedNameUsageId"='4452216',
genus='Propylea', 
species='Propylea quatuordecimpunctata', 
"genusId"=4405073, 
"speciesId"=4452216, 
"parentNameUsageId"=4405073 
where "taxonId"='4452216';

select * from val_species where "scientificName" like 'Propylea%';