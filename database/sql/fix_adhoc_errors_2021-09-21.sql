--select count(*) from val_species; --27564 to begin --34605 to end

select * from val_species where "taxonId"='140387071';

select * from val_species 
	where "taxonRank"='species' and "taxonomicStatus"='accepted' and ("speciesId" IS NULL OR "species" IS NULL);
--update val_species set
	"species"="scientificName",
	"speciesId"="taxonId"
	where  "taxonRank"='species' and "taxonomicStatus"='accepted' and ("speciesId" IS NULL OR "species" IS NULL);
	
select * from val_species where "acceptedNameUsageId"="taxonId" and "taxonomicStatus"='synonym';

select * from val_species where "scientificName"='Iphthiminus opacus';
select * from val_species where "scientificName"='Iphthimus opacus';
select * from val_species where"taxonId"='140387071';
select * from val_species where"taxonId"='11024043'
--delete from val_species where "taxonId"='140387071';
--delete from val_species where "taxonId"='11024043';
--update val_species set 
	"acceptedNameUsageId"='7894847',
	"acceptedNameUsage"='Iphthiminus opacus',
	"parentNameUsageId"='4406048',
	"genus"='Iphthiminus',
	"genusId"='4406048'
	where "scientificName"='Iphthimus opacus';