
copy (
select * from val_species where family in ('Apidae','Andrenidae','Halictidae','Melittadae','Colletidae','Megachilidae')
)
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_bee_taxa.txt'
with HEADER CSV;

select * from val_species where "scientificName"='Melissodes brevipygus' or species='Melissodes brevipygus'; -- just itself!
select * from val_species where "scientificName"='Lasioglossum divergens';
select * from val_species where "scientificName"='Lasioglossum macoupinense';

delete from val_species where "scientificName"='Melissodes brevipygus';
update val_species set
"taxonomicStatus"='synonym',
"acceptedNameUsageId"='1354198',
"acceptedNameUsage"='Lasioglossum macoupinense'
where "scientificName"='Lasioglossum divergens';
