drop table IF EXISTS noo_vernacular;

select *
	into noo_vernacular
	from new_vernacular
limit 0;

ALTER TABLE noo_vernacular ALTER column "updatedAt" SET default now();
ALTER TABLE noo_vernacular ALTER column "createdAt" SET default now();
ALTER TABLE noo_vernacular ADD CONSTRAINT unique_taxonid_vernacularname_noo UNIQUE("taxonId", "vernacularName");
ALTER TABLE noo_vernacular ADD CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES new_species ("taxonId");

DROP FUNCTION IF EXISTS vernacular_case();
CREATE OR REPLACE FUNCTION vernacular_case()
    RETURNS trigger
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
   NEW."vernacularName" = INITCAP(NEW."vernacularName");
   RETURN NEW;
END;
$BODY$;

DROP TRIGGER IF EXISTS trigger_insert_vernacular_name ON noo_vernacular;
CREATE TRIGGER trigger_insert_vernacular_name
    BEFORE INSERT 
    ON noo_vernacular
    FOR EACH ROW
    EXECUTE FUNCTION vernacular_case();

DROP TRIGGER IF EXISTS trigger_update_vernacular_name ON noo_vernacular;
CREATE TRIGGER trigger_update_vernacular_name
    BEFORE UPDATE
    ON noo_vernacular
    FOR EACH ROW
    EXECUTE FUNCTION vernacular_case();

insert into noo_vernacular
	select *
	from new_vernacular
ON CONFLICT ON CONSTRAINT unique_taxonid_vernacularname_noo DO NOTHING;

insert into noo_vernacular
	select ov.*
	from val_vernacular ov 
	inner join new_species ns on ns."taxonId"=ov."taxonId"
ON CONFLICT ON CONSTRAINT unique_taxonid_vernacularname_noo DO NOTHING;

select * from noo_vernacular;

ALTER TABLE IF EXISTS new_vernacular RENAME TO new_vernacular_backup;

ALTER TABLE IF EXISTS noo_vernacular RENAME TO new_vernacular;

SELECT * FROM new_vernacular;