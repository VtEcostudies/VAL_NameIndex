--in Feb of 2022, discussed with Spencer Hardy these issues and more to make corrections 
select * from val_species where "scientificName"='Melissodes brevipygus' or species='Melissodes brevipygus'; -- just itself!
select * from val_species where "scientificName"='Lasioglossum divergens';
select * from val_species where "scientificName"='Lasioglossum macoupinense';

delete from val_species where "scientificName"='Melissodes brevipygus';
update val_species set
"taxonomicStatus"='synonym',
"acceptedNameUsageId"='1354198',
"acceptedNameUsage"='Lasioglossum macoupinense'
where "scientificName"='Lasioglossum divergens';
