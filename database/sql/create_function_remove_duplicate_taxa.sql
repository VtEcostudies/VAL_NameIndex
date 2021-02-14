
CREATE OR REPLACE FUNCTION remove_duplicate_taxa()
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	next_int integer;
	next_val text;
BEGIN
--1) get dupes for a taxonRank
--2) iterate over the list
--3) for each taxon, find the lowest/first canonical taxonId
--4) set all taxonRankIds to that value
--5) set all parentNameUsageIds to that value
--6) if species, figure out whether to apply this to acceptedNameUsageId

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
$BODY$;

ALTER FUNCTION public.remove_duplicate_taxa()
    OWNER TO postgres;
