SELECT
    "specificEpithet", COUNT(*)
FROM
    val_species
GROUP BY
    "specificEpithet"
HAVING 
    COUNT(*) > 1;

--select * from val_species where "specificEpithet" like 'gerardii%';