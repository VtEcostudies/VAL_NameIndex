--delete from val_species where "gbifId" IN (
	
select s."gbifId" from val_gbif_taxon_id g inner join val_species s on g."gbifId"=s."gbifId"

);

--truncate table val_gbif_taxon_id;