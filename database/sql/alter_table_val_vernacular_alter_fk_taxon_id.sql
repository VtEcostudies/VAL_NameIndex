ALTER table  val_vernacular
    DROP CONSTRAINT fk_taxon_id,
    ADD CONSTRAINT fk_taxon_id 
    FOREIGN KEY ("taxonId") REFERENCES val_species("taxonId") 
		ON UPDATE NO ACTION
		ON DELETE CASCADE;