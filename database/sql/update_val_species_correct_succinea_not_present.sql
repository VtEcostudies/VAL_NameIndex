select * from val_species where family='Succineidae' and "taxonRank"='family'; --taxonId 6531
select * from val_species where species='Succinea putris'; --taxonId 2297206
select * from val_species where genus='Succinea'; --taxonId 2297197

update val_species set 
"taxonomicStatus"='not_present',
"acceptedNameUsage"='Succineidae',
"acceptedNameUsageId"='6531',
"nomenclaturalCode"='VTSR'
where
("scientificName"='Succinea putris' and "taxonRank"='species')
or
("scientificName"='Succinea' and "taxonRank"='genus');
