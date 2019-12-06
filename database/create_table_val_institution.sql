DROP TABLE IF EXISTS val_institution;

CREATE TABLE val_institution
(
	"institutionCode" VARCHAR NOT NULL UNIQUE PRIMARY KEY,
	"institutionName" VARCHAR NOT NULL,
	"institutionDesc" VARCHAR,
	"institutionUrl" VARCHAR,
	"createdAt" timestamp without time zone DEFAULT now(),
	"updatedAt" timestamp without time zone DEFAULT now()
);

ALTER TABLE val_distribution OWNER to VAL;

--create triggers for each table having the column "updatedAt"
CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE 
    ON val_institution
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();