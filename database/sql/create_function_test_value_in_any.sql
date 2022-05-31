CREATE OR REPLACE FUNCTION test_rank_in_list(t_rank text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	rank_list text[] := '{kingdom,phylum,class,order,family,genus,species}'::text[];
BEGIN
	IF t_rank = ANY(rank_list) THEN
		RETURN 'true';
	ELSE 
		RETURN 'false';
	END IF;
END;
$BODY$;

select test_rank_in_list('family');
