DROP TABLE IF EXISTS val_gbif_taxon_id;

CREATE TABLE val_gbif_taxon_id
(
"gbifId" SERIAL UNIQUE PRIMARY KEY
);

ALTER TABLE val_gbif_taxon_id OWNER to VAL;
