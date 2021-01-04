--retrieve a list of accpetedNameUsageIds which lack a primary definition (no taxonId)
select b."acceptedNameUsageId"
from val_species a
right join val_species b
on a."taxonId" = b."acceptedNameUsageId"
where a."taxonId" is null;
