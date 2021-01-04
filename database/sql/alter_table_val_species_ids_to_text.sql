alter table val_species alter column "gbifId" set default 0;
alter table val_species alter column "scientificName" set not null;
alter table val_species alter column "speciesId" type text;
alter table val_species alter column "genusId" type text;
alter table val_species alter column "familyId" type text;
alter table val_species alter column "orderId" type text;
alter table val_species alter column "classId" type text;
alter table val_species alter column "phylumId" type text;
alter table val_species alter column "kingdomId" type text;
