--find preferred vernacularNames in val_vernacular not in val_species...
select s."taxonId", s."scientificName", s."vernacularName" as spenac, v."vernacularName" as vernac, "preferred" from val_species s
join val_vernacular v on s."taxonId"=v."taxonId"
where 
s."vernacularName" is null 
and v."preferred" is true;

--make a backup before we run this, below
select val_species.* into val_species_backup from val_species;

update val_species s
    set "vernacularName" = v."vernacularName"
    from val_vernacular v
    where s."taxonId"=v."taxonId" and s."vernacularName" is null and v."preferred" is true; --UPDATE 1081

--check for properly updated values...
select s."taxonId", s."scientificName", s."vernacularName" as spenac, s."updatedAt", v."vernacularName" as vernac, "preferred" 
from val_species s
join val_vernacular v on s."taxonId"=v."taxonId"
where 
s."updatedAt" > NOW() - interval '1 hour'
and v."preferred" is true; --SELECT 1081 updatedat 2022-01-28:13:53

--looks good!