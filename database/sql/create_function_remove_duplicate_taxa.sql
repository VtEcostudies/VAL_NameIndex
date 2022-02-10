--drop function remove_duplicate_taxa(t_rank text);

CREATE OR REPLACE FUNCTION remove_duplicate_taxa_rank(t_rank text)
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	t_rank_id text;
	d_recs record;
	min_id integer;
	tax_id text;
	sql_upd text;
	sql_del text;
BEGIN
--1) get dupes for a taxonRank. NOTE the updated query for dupes inluding taxonomicStatus and kingdom.
--2) iterate over the list
--3) for each taxon, find the lowest/first canonical taxonId
--4) set all taxonRankIds to that value
--5) set all parentNameUsageIds to that value
--6) if species, figure out whether to apply this to acceptedNameUsageId

	t_rank_id := t_rank || 'Id';

	FOR d_recs IN
		SELECT
			"scientificName", "taxonRank", COUNT(*) as dupes
			--, "taxonomicStatus"
			, "kingdom"
		FROM
			val_species
		GROUP BY
			"scientificName", "taxonRank"
			--, "taxonomicStatus"
			, "kingdom"
		HAVING 
			COUNT(*) > 1
			AND "taxonRank" = t_rank
		ORDER BY 
			"taxonRank" desc, dupes desc
		LIMIT 1
	LOOP
		RAISE NOTICE '%',d_recs."scientificName";
		SELECT MIN("gbifId") FROM val_species 
			WHERE LOWER("taxonomicStatus")='accepted' AND "scientificName"=d_recs."scientificName" into min_id;
		IF min_id IS NULL THEN --no accepted taxa found
			SELECT MIN("gbifId") FROM val_species WHERE "scientificName"=d_recs."scientificName" into min_id;
		END IF;
		SELECT min_id::TEXT INTO tax_id;
		IF min_id = 0 THEN --found gbifId==0, a VTSR:XX taxon, use it
			SELECT "taxonId" FROM val_species WHERE "scientificName"=d_recs."scientificName" AND "gbifId"=0 into tax_id;
		END IF;
		sql_upd := format('UPDATE val_species SET %I=%L WHERE %I=%L', t_rank_id, tax_id, t_rank, d_recs."scientificName");
		RAISE NOTICE '%', sql_upd;
		EXECUTE sql_upd;
		--update val_species set "parentNameUsageId"=format(%L, tax_id) where "parentNameUsageId"=format(%L, tax_id);
		--delete all taxa with sciName and taxonId NOT equal to tax_id
		sql_del := format('DELETE FROM val_species WHERE "scientificName"=%L AND "taxonId"!=%L', d_recs."scientificName", tax_id);
		RAISE NOTICE '%', sql_del;
		EXECUTE sql_del;
	END LOOP;

	RETURN d_recs;
END;
$BODY$;

ALTER FUNCTION public.remove_duplicate_taxa_rank(text)
    OWNER TO postgres;

CREATE OR REPLACE FUNCTION remove_duplicate_taxa_all()
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	t_recs record;
BEGIN
	FOR t_recs IN
		SELECT DISTINCT(LOWER("taxonRank")) AS t_rank FROM val_species
	LOOP
		RAISE NOTICE '%', t_recs.t_rank;
	END LOOP;
END;
$BODY$;
