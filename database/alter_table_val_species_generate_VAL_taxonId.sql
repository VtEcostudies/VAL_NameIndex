--DROP TRIGGER trigger_insert_new_taxon ON public.val_species;
--DROP FUNCTION public.trigger_new_val_taxon_id();
--DROP FUNCTION public.generate_new_val_taxon_id();

--add a function and trigger to auto-generate a VTSR taxonId for custom/canonical taxa
CREATE OR REPLACE FUNCTION generate_new_val_taxon_id()
RETURNS TEXT AS $$
DECLARE
	next_int integer;
	next_val text;
BEGIN

	SELECT max(new_val) AS new_max FROM (
		SELECT
			"taxonId",
			TO_NUMBER(substr("taxonId",5,10), '99999') AS new_val
		FROM val_species
		WHERE "taxonId" LIKE 'VTSR:%'
	) max_new
	INTO next_int;

	next_int := next_int + 1;
	next_val := concat('VTSR:', next_int::TEXT );

	RETURN next_val;
END;
$$ language 'plpgsql';

--select generate_new_val_taxon_id();

CREATE OR REPLACE FUNCTION trigger_new_val_taxon_id()
RETURNS TRIGGER AS $$
DECLARE
	next_val text;
	taxon_exists text;
BEGIN
	--check to see if we're inserting a VAL custom taxon
	IF (substr(NEW."taxonId",1,6) = 'VTSR:*') THEN

		SELECT generate_new_val_taxon_id() INTO next_val;

		NEW."taxonId" := next_val;
		NEW."speciesId" := next_val;
		
		--if missing, assign acceptedNameUsage to the incoming scientificName
		IF (NEW."acceptedNameUsage" IS NULL) THEN
			NEW."acceptedNameUsage" := NEW."scientificName";
		END IF;
		
		--if missing, assign acceptedNameUsageId to the new taxonId
		IF (NEW."acceptedNameUsageId" IS NULL) THEN
			NEW."acceptedNameUsageId" := next_val;
		END IF;
		
		--if missing, assign taxonomicStatus to 'accepted'
		IF (NEW."taxonomicStatus" IS NULL) THEN
			NEW."taxonomicStatus" := 'accepted';
		END IF;
		
		SELECT "taxonId" FROM val_species WHERE "scientificName"=NEW."scientificName" INTO taxon_exists;
		
		IF taxon_exists IS NOT NULL THEN
			--RAISE EXCEPTION 'scientificName % already exists.', NEW."scientificName";
			RAISE SQLSTATE '23505'; --this is necessary to enable proper response in 09_ingest_species_list_new_not_found.js
		END IF;

	END IF;

	RETURN NEW;
END;
$$ language 'plpgsql';

--trigger before INSERT only
CREATE TRIGGER trigger_insert_new_taxon BEFORE INSERT
    ON val_species FOR EACH ROW EXECUTE PROCEDURE
    trigger_new_val_taxon_id();
