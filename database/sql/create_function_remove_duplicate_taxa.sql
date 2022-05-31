--drop function remove_duplicate_taxa_rank(t_rank text);

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
	rank_list text[] := '{kingdom,phylum,class,order,family,genus,species}'::text[];
BEGIN
--1) get dupes for a taxonRank. NOTE the updated query for dupes inluding taxonomicStatus and kingdom.
--2) iterate over the list
--3) for each taxon, find the lowest/first canonical taxonId
--4) set all taxonRankIds to that value
--5) set all parentNameUsageIds to that value
--6) if species, figure out whether to apply this to acceptedNameUsageId
--7) DEPRECATED: update val_vernacular taxonId foreign keys to canonical taxonId
--8) update val_conservation_status taxonId foreign keys to canonical taxonId

	IF t_rank = ANY(rank_list) THEN
		t_rank_id := t_rank || 'Id';
	END IF;

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
		--LIMIT 1
	LOOP
		RAISE NOTICE 'scientificName:%|taxonRank:%', d_recs."scientificName", t_rank;

		--get a canonicalId for scientificName
		SELECT MIN("gbifId") FROM val_species 
			WHERE LOWER("taxonomicStatus")='accepted' AND "scientificName"=d_recs."scientificName" into min_id;
		IF min_id IS NULL THEN --no status=accepted taxa found, use any status as canonicalId
			SELECT MIN("gbifId") FROM val_species WHERE "scientificName"=d_recs."scientificName" into min_id;
		END IF;
		SELECT min_id::TEXT INTO tax_id;
		IF min_id = 0 THEN --found gbifId==0, a VTSR:XX taxon, use it
			SELECT "taxonId" FROM val_species WHERE "scientificName"=d_recs."scientificName" AND "gbifId"=0 into tax_id;
		END IF;
		
		--check if target taxonRank is in a list of explicit columns; others do not have dependent taxa pointing at them
		IF t_rank = ANY(rank_list) THEN
			--update val_species dependent taxa's rank_id to point to canonicalId (tax_id)
			sql_upd := format('UPDATE val_species SET %I=%L WHERE %I=%L', t_rank_id, tax_id, t_rank, d_recs."scientificName");
			RAISE NOTICE '%', sql_upd;
			EXECUTE sql_upd;
			--update val_species acceptedNameUsageId's to point to canonicalId (tax_id) for eg. synonyms having non-canonical Id
			sql_upd := format('UPDATE val_species SET "acceptedNameUsageId"=%L WHERE accepted=%L', tax_id, t_rank, d_recs."scientificName");
			RAISE NOTICE '%', sql_upd;
			EXECUTE sql_upd;
		ELSE
			RAISE NOTICE 'scientificName:%|taxonRank:%|Does not have dependent rankId pointers', d_recs."scientificName", t_rank;
		END IF;
		
		--NOTE: tried this and it doesn't work. val_vernacular has unique constraint on taxonId, vernacularName.
		--update val_vernacular taxonId foreign keys to point to canonicalId (tax_id)
		--sql_upd := format('UPDATE val_vernacular SET "taxonId"=%L WHERE "scientificName"=%L', tax_id, d_recs."scientificName");
		--RAISE NOTICE '%', sql_upd;
		--EXECUTE sql_upd;
		
		--update val_conservation_status taxonId foreign keys to point to canonicalId (tax_id)
		sql_upd := format('UPDATE val_conservation_status SET "taxonId"=%L WHERE "scientificName"=%L', tax_id, d_recs."scientificName");
		RAISE NOTICE '%', sql_upd;
		EXECUTE sql_upd;
	
		--delete all val_species taxa for scientificName where taxonId != canonicalID (tax_id)
		sql_del := format('DELETE FROM val_species WHERE "scientificName"=%L AND "taxonId"!=%L', d_recs."scientificName", tax_id);
		RAISE NOTICE '%', sql_del;
		EXECUTE sql_del;
	END LOOP;

	RETURN d_recs;
END;
$BODY$;

ALTER FUNCTION public.remove_duplicate_taxa_rank(text)
    OWNER TO postgres;

drop function remove_duplicate_taxa_all();

CREATE OR REPLACE FUNCTION remove_duplicate_taxa_all()
    RETURNS record
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
	t_recs record;
	ret record;
BEGIN
	FOR t_recs IN
		SELECT DISTINCT(LOWER("taxonRank")) AS t_rank FROM val_species
	LOOP
		RAISE NOTICE 'remove_duplicate_taxa_rank(%)', t_recs.t_rank;
		PERFORM remove_duplicate_taxa_rank(t_recs.t_rank);
	END LOOP;
	SELECT COUNT(*) AS val_species_count from val_species INTO ret;
	RETURN ret;
END;
$BODY$;

ALTER FUNCTION public.remove_duplicate_taxa_all()
    OWNER TO postgres;
