
insert into val_vernacular ("taxonId", "scientificName", "vernacularName")
select s."taxonId", s."scientificName", s."vernacularName"
from val_species s
left join val_vernacular v on s."vernacularName"=v."vernacularName"
where s."vernacularName" <> '' and v."vernacularName" is null;