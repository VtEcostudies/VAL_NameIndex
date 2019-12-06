DROP TABLE IF EXISTS val_conservation_status;

--CREATE TYPE val_staterank_type AS ENUM ('XXX', 'YYY');

CREATE TABLE val_conservation_status
(
	"gbifId" INTEGER NOT NULL,
	"taxonId" VARCHAR NOT NULL UNIQUE PRIMARY KEY,
	"SGCN" VARCHAR, --Species of Greatest Conservation Need
	"stateRank" VARCHAR,
	"globalRank" VARCHAR,
	"federalListed" VARCHAR, --Federally endangered or threatened
	"stateListed" VARCHAR, --State endangered or threatened
	"createdAt" timestamp without time zone DEFAULT now(),
	"updatedAt" timestamp without time zone DEFAULT now(),
	CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES val_species ("taxonId")
);

ALTER TABLE val_conservation_status OWNER to VAL;

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
    ON val_conservation_status
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();