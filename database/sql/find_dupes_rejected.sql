SELECT
    "taxonId", kingdom, COUNT(*)
FROM
    val_reject
GROUP BY
    "taxonId", kingdom
HAVING 
    COUNT(*) > 1 AND kingdom='Animalia'
	
--select * from val_reject where "taxonId"='2427583';
select * from val_species where 
	"scientificName" LIKE '%Rana clamitans%'
	OR "scientificName" LIKE '%Hyla versicolor%'
	OR "scientificName" LIKE '%Rana pipiens%'
	OR "scientificName" LIKE '%Rana sylvatica%'
	;
