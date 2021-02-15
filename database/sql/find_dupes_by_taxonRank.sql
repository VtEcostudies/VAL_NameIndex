SELECT
    "scientificName", "taxonRank", COUNT(*) as dupes
FROM
    val_species
GROUP BY
    "scientificName", "taxonRank"
HAVING 
    COUNT(*) > 1
	AND "taxonRank" = 'genus'
ORDER BY 
	dupes desc, "taxonRank" desc;

--select * from val_species;