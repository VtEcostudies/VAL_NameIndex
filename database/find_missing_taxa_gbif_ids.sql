--retrieve a list of acceptedNameUsageId which lack a primary definition (no taxonId)
select cast(va."acceptedNameUsageId" as int8) as "taxonId"
from val_species vs
right join val_species va
on vs."taxonId" = va."acceptedNameUsageId"
where vs."taxonId" is null
union
--retrieve a list of kingdomId which lack a primary definition (no taxonId)
select b."kingdomId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."kingdomId"
where a."gbifId" is null and b."kingdomId" is not null
union
--retrieve a list of phylumId which lack a primary definition (no taxonId)
select b."phylumId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."phylumId"
where a."gbifId" is null and b."phylumId" is not null
union
--retrieve a list of classId which lack a primary definition (no taxonId)
select b."classId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."classId"
where a."gbifId" is null and b."classId" is not null
union
--retrieve a list of orderId which lack a primary definition (no taxonId)
select b."orderId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."orderId"
where a."gbifId" is null and b."orderId" is not null
union
--retrieve a list of familyId which lack a primary definition (no taxonId)
select b."familyId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."familyId"
where a."gbifId" is null and b."familyId" is not null
union
--retrieve a list of genusId which lack a primary definition (no taxonId)
select b."genusId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."genusId"
where a."gbifId" is null and b."genusId" is not null
union
--retrieve a list of speciesId which lack a primary definition (no taxonId)
select b."speciesId" as "taxonId"
from val_species a
right join val_species b
on a."gbifId" = b."speciesId"
where a."gbifId" is null and b."speciesId" is not null
