insert into val_vernacular ("taxonId","scientificName","vernacularName","lifeStage","sex","language","countryCode","source")
--repeat the following for split_part 1,2,3,4
select 
"taxonId", "scientificName",
TRIM(split_part("vernacularName", ',', 1)) AS "vernacularNameTwo",
"lifeStage","sex","language","countryCode","source"
from val_vernacular 
where "vernacularName" like '%,%' 
and split_part("vernacularName", ',', 1) <> ''
and split_part("vernacularName", ',', 1) NOT like '%subspecies%'
and split_part("vernacularName", ',', 1) NOT like '%ssp%'
ON CONFLICT("taxonId", "vernacularName") DO NOTHING
;

insert into val_vernacular ("taxonId","scientificName","vernacularName","lifeStage","sex","language","countryCode","source")
--repeat the following for split_part ' and ', ' & '
select 
"taxonId", "scientificName", --"vernacularName",
TRIM(split_part("vernacularName", ' & ', 2)) AS "vernacularNameTwo",
"lifeStage","sex","language","countryCode","source"
from val_vernacular 
where "vernacularName" like '% & %' 
and split_part("vernacularName", ' & ', 2) <> ''
ON CONFLICT("taxonId", "vernacularName") DO NOTHING
;

delete from val_vernacular
where "vernacularName" like '%,%'
;
