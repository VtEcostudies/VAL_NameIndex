SELECT DISTINCT ON ("taxonKey") --1253 -> 1144
"missingColumn", "taxonKey"
FROM (
	SELECT "taxonKey", 'parentKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "parentKey" IS NULL
		AND UPPER("taxonRank") NOT IN ('KINGDOM') 
UNION 
	SELECT "taxonKey", 'acceptedTaxonKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "acceptedTaxonKey" IS NULL
		AND "taxonKey" != 0
UNION 
	SELECT "taxonKey", 'kingdomKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "kingdomKey" IS NULL
UNION
	SELECT "taxonKey", 'phylumKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "phylumKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM') 
UNION
	SELECT "taxonKey", 'classKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "classKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM') 
UNION
	SELECT "taxonKey", 'orderKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "orderKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS') 
UNION
	SELECT "taxonKey", 'familyKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "familyKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER') 
UNION
	SELECT "taxonKey", 'genusKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "genusKey" IS NULL 
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER', 'FAMILY') 
UNION
	SELECT "taxonKey", 'speciesKey' AS "missingColumn" FROM val_species_from_occs 
		WHERE "speciesKey" IS NULL
		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER', 'FAMILY', 'GENUS') 
) agg
