SELECT
    "taxonId", COUNT(*)
FROM
    all_butterfly_species
GROUP BY
    "taxonId"
HAVING 
    COUNT(*) > 1;
