alter table val_species add column "bibliographicCitation" varchar;
alter table val_species add column "createdAt" timestamp without time zone DEFAULT now();
alter table val_species add column "updatedAt" timestamp without time zone DEFAULT now();
alter table val_species add column "references" varchar;
alter table val_species add column "institutionCode" varchar;
alter table val_species add column "collectionCode" varchar;
alter table val_species add column "establishmentMeans" varchar;
--create generic trigger function to set "updatedAt"=now() for each table having that column
CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS trigger
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
   NEW."updatedAt" = now(); 
   RETURN NEW;
END;
$BODY$;
--create triggers for each table having the column "updatedAt"
CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE 
    ON val_species
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();