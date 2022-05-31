--retrieve a list of acceptedNameUsageId which lack a primary definition (no taxonId)
select cast(a."acceptedNameUsageId" as text) as "missingId", a."taxonId" as "sourceId", 'acceptedNameUsageId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species b
right join val_species a
on b."taxonId" = a."acceptedNameUsageId"
where b."taxonId" is null and a."acceptedNameUsageId" != '' and a."acceptedNameUsageId" != '0'
union
--retrieve a list of parentNameUsageId which lack a primary definition (no taxonId)
select cast(a."parentNameUsageId" as text) as "missingId", a."taxonId" as "sourceId", 'parentNameUsageId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species b
right join val_species a
on b."taxonId" = a."parentNameUsageId"
where b."taxonId" is null and a."parentNameUsageId" != '' and a."parentNameUsageId" != '0'
union
--retrieve a list of kingdomId which lack a primary definition (no taxonId)
select b."kingdomId" as "missingId",  b."taxonId" as "sourceId", 'kingdomId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."kingdomId"
where a."taxonId" is null and b."kingdomId" is not null and b."kingdomId" != '0'
union
--retrieve a list of phylumId which lack a primary definition (no taxonId)
select b."phylumId" as "missingId",  b."taxonId" as "sourceId", 'phylumId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."phylumId"
where a."taxonId" is null and b."phylumId" is not null and b."phylumId" != '0'
union
--retrieve a list of classId which lack a primary definition (no taxonId)
select b."classId" as "missingId",  b."taxonId" as "sourceId", 'classId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."classId"
where a."taxonId" is null and b."classId" is not null and b."classId" != '0'
union
--retrieve a list of orderId which lack a primary definition (no taxonId)
select b."orderId" as "missingId",  b."taxonId" as "sourceId", 'orderId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."orderId"
where a."taxonId" is null and b."orderId" is not null and b."orderId" != '0'
union
--retrieve a list of familyId which lack a primary definition (no taxonId)
select b."familyId" as "missingId",  b."taxonId" as "sourceId", 'familiyId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."familyId"
where a."taxonId" is null and b."familyId" is not null and b."familyId" != '0'
union
--retrieve a list of genusId which lack a primary definition (no taxonId)
select b."genusId" as "missingId",  b."taxonId" as "sourceId", 'genusId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."genusId"
where a."taxonId" is null and b."genusId" is not null and b."genusId" != '0'
union
--retrieve a list of speciesId which lack a primary definition (no taxonId)
select b."speciesId" as "missingId",  b."taxonId" as "sourceId", 'speciesId' as column
, a."scientificName", a."taxonRank", a."taxonomicStatus"
from val_species a
right join val_species b
on a."taxonId" = b."speciesId"
where a."taxonId" is null and b."speciesId" is not null and b."speciesId" != '0'
