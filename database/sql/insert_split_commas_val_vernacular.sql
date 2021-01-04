CREATE OR REPLACE FUNCTION vernacular_split_names ()
RETURNS text
AS $$
DECLARE
  i integer := 1;
BEGIN 
LOOP
    RAISE NOTICE 'Splitting %the comma-separated pieces...', i;
	insert into val_vernacular ("taxonId","scientificName","vernacularName","lifeStage","sex","language","countryCode","source")
	select 
		"taxonId", "scientificName",TRIM(split_part("vernacularName", ',', i)),"lifeStage","sex","language","countryCode","source"
	from val_vernacular 
	where 
		"vernacularName" like '%,%' 
		and split_part("vernacularName", ',', i) <> ''
		and split_part("vernacularName", ',', i) NOT like '%subspecies%'
		and split_part("vernacularName", ',', i) NOT like '% ssp %'
		and split_part("vernacularName", ',', i) NOT like '% ssp. %'
		and split_part("vernacularName", ',', i) NOT like '%variety%'
		and split_part("vernacularName", ',', i) NOT like '% var. %'
	ON CONFLICT("taxonId", "vernacularName") DO NOTHING;	
    i := i + 1;
	EXIT WHEN i > 4;
END LOOP;

RAISE NOTICE 'Splitting & pieces...';

INSERT INTO val_vernacular ("taxonId","scientificName","vernacularName","lifeStage","sex","language","countryCode","source")
	SELECT "taxonId","scientificName",TRIM(split_part("vernacularName", ' & ', 2)),"lifeStage","sex","language","countryCode","source"
	FROM val_vernacular 
	WHERE "vernacularName" LIKE '% & %' 
	AND split_part("vernacularName", ' & ', 2) != ''
ON CONFLICT("taxonId", "vernacularName") DO NOTHING;


RAISE NOTICE 'Splitting `and` pieces...';

INSERT INTO val_vernacular ("taxonId","scientificName","vernacularName","lifeStage","sex","language","countryCode","source")
	SELECT "taxonId","scientificName",TRIM(split_part("vernacularName", ' and ', 2)),"lifeStage","sex","language","countryCode","source"
	FROM val_vernacular 
	WHERE "vernacularName" LIKE '% and %' 
	AND split_part("vernacularName", ' and ', 2) != ''
ON CONFLICT("taxonId", "vernacularName") DO NOTHING;

RETURN 'DONE';
END; $$ 
 
LANGUAGE 'plpgsql';