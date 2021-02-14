SELECT
    "scientificName", "taxonRank", COUNT(*) as dupes
FROM
    val_species
GROUP BY
    "scientificName", "taxonRank"
HAVING 
    COUNT(*) > 1
	AND "taxonRank" = 'family'
ORDER BY 
	dupes desc, "taxonRank" desc;
