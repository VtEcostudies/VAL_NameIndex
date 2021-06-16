--Kent says 'Callophrys polia' ain't found in Vermont
select * from val_species where "scientificName"='Callophrys polia';
--delete from val_species where "scientificName"='Callophrys polia';

select * from val_vernacular where "scientificName"='Hygrohypnum eugyrium';

SELECT
    "scientificName", preferred, COUNT(*) as dupes
FROM
    val_vernacular
GROUP BY
    "scientificName", "preferred"
HAVING 
    COUNT(*) > 1
	AND "preferred" = 't'
ORDER BY 
	"scientificName";

select * from val_species where "vernacularName"=''; --many
--update val_species set "vernacularName"=null where "vernacularName"='';
select * from val_species where "vernacularName" is not null and "vernacularName" != '';

select s."vernacularName",s."scientificName",s."taxonId",v."vernacularName" as v_vernacular from val_species s
left join val_vernacular v on LOWER(s."vernacularName")=LOWER(v."vernacularName")
where s."vernacularName" is not null and v."vernacularName" is null;

select s."taxonId", s."scientificName", s."taxonRank"
from val_species s
left join val_vernacular v on s."taxonId"=v."taxonId"
where v."taxonId" is null; --11,207
