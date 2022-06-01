SELECT
"missingColumn", "missingCount"
FROM (
	SELECT count(*) AS "missingCount", 'parentKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "parentKey" IS NULL
		AND UPPER("taxonRank") NOT IN ('KINGDOM') 
UNION 
	SELECT count(*) AS "missingCount", 'acceptedTaxonKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "acceptedTaxonKey" IS NULL
		AND "taxonKey" != 0
UNION 
	SELECT count(*) AS "missingCount", 'kingdomKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "kingdomKey" IS NULL
UNION
	SELECT count(*) AS "missingCount", 'phylumKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "phylumKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM') 
UNION
	SELECT count(*) AS "missingCount", 'classKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "classKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM') 
UNION
	SELECT count(*) AS "missingCount", 'orderKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "orderKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS') 
UNION
	SELECT count(*) AS "missingCount", 'familyKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "familyKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER') 
UNION
	SELECT count(*) AS "missingCount", 'genusKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "genusKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER', 'FAMILY') 
UNION
	SELECT count(*) AS "missingCount", 'speciesKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "speciesKey" IS NULL
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER', 'FAMILY', 'GENUS') 
) agg
