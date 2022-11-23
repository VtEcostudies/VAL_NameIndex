--DROP TABLE IF EXISTS public.new_vernacular;

CREATE TABLE IF NOT EXISTS public.new_vernacular
(
    "vernacularId" SERIAL UNIQUE PRIMARY KEY,
    "taxonId" VARCHAR NOT NULL,
    "scientificName" VARCHAR NOT NULL,
    "vernacularName" VARCHAR NOT NULL,
    "lifeStage" VARCHAR,
    "sex" VARCHAR,
    "language" VARCHAR,
    "countryCode" VARCHAR,
    "source" VARCHAR,
    "preferred" BOOLEAN,
    "createdAt" timestamp without time zone DEFAULT now(),
    "updatedAt" timestamp without time zone DEFAULT now(),
    CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES new_species ("taxonId")
);

CREATE UNIQUE INDEX taxonid_vernacularname_unique_idx on new_vernacular ("taxonId", LOWER("vernacularName"));

-- DROP TRIGGER IF EXISTS trigger_updated_at ON public.new_vernacular;

CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE 
    ON public.new_vernacular
    FOR EACH ROW
    EXECUTE FUNCTION public.set_updated_at();

drop table if exists new_vernacular;
select ov.*
into new_vernacular
from val_vernacular ov 
inner join new_species ns on ns."taxonId"=ov."taxonId"

ALTER TABLE new_vernacular ADD CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES new_species ("taxonId");
CREATE UNIQUE INDEX taxonid_vernacularname_unique_idx on new_vernacular ("taxonId", LOWER("vernacularName"));
