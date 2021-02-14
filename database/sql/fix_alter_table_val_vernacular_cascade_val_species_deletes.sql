--cascade val_species deletes to val_vernacular
ALTER TABLE val_vernacular DROP CONSTRAINT fk_taxon_id;

ALTER TABLE val_vernacular    
ADD CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId")
	REFERENCES public.val_species ("taxonId") MATCH SIMPLE
	ON UPDATE NO ACTION
	ON DELETE CASCADE;
