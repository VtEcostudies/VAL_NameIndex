DROP TABLE IF EXISTS val_collection;

CREATE TABLE val_collection
(
	"collectionCode" VARCHAR UNIQUE PRIMARY KEY NOT NULL,
	"collectionName" VARCHAR NOT NULL,
	"collectionDesc" VARCHAR,
	"collectionUrl" VARCHAR,
	"institutionCode" VARCHAR,
	"createdAt" timestamp without time zone DEFAULT now(),
	"updatedAt" timestamp without time zone DEFAULT now(),
	CONSTRAINT fk_institution_code FOREIGN KEY ("institutionCode") REFERENCES val_institution ("institutionCode")
);

ALTER TABLE val_distribution OWNER to VAL;

--create triggers for each table having the column "updatedAt"
CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE 
    ON val_collection
    FOR EACH ROW
    EXECUTE PROCEDURE public.set_updated_at();