select vg."gbifId" 
from val_species vs
right join val_gbif_taxon_id vg
on vs."gbifId" = vg."gbifId"
where vs."gbifId" is null;