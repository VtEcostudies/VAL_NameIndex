DROP TRIGGER trigger_insert_val_vernacular ON val_vernacular;
DROP FUNCTION insert_val_vernacular();

--add a function and trigger to check for same vernacular name with non-matching case
CREATE OR REPLACE FUNCTION insert_val_vernacular()
RETURNS TRIGGER AS $$
DECLARE
	exists record;
BEGIN

	SELECT "vernacularName", "scientificName", "taxonId"
	FROM val_vernacular
	WHERE LOWER("vernacularName") = LOWER(NEW."vernacularName") AND "taxonId" = NEW."taxonId"
	INTO exists;

	IF exists."vernacularName" IS NOT NULL THEN
		--IF exists."taxonId" = NEW."taxonId" THEN
			--RAISE EXCEPTION 'vernacularName % already exists.', NEW."vernacularName";
			RAISE SQLSTATE '23505';
		--END IF;
	END IF;
	
	RETURN NEW;
END;
$$ language 'plpgsql';

--trigger before INSERT only
CREATE TRIGGER trigger_insert_val_vernacular BEFORE INSERT
    ON val_vernacular FOR EACH ROW EXECUTE PROCEDURE
    insert_val_vernacular();
