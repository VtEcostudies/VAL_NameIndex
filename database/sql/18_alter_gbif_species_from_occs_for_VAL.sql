--Species from occurrences don't have the column 'parentNameUsageID', which points to the parent taxon.
--The ALA nameindexer requires parentNameUsageID, so we have to add that column, then derive all its values.
--In the case of taxonomicStatus==ACCEPTED, we use taxonRank to find the next-highest taxonRank's taxonKey.
--In the case of taxonomicStatus==SYNONYM, we do the same, but we might have the wrong parent? What does the SYNONYM'S ACCEPTED
--taxon use for parentKey?
--Initial decision: since, below, we find we lack many ACCEPTED values for SYNONYMS, and retrieving those is a lot of work, initially
--we will treat SYNONYMS like ACCEPTED taxa.
ALTER TABLE val_species_from_occs ADD COLUMN "nubKey" BIGINT;
ALTER TABLE val_species_from_occs ADD COLUMN "canonicalName" TEXT;
ALTER TABLE val_species_from_occs ADD COLUMN "parent" TEXT;
ALTER TABLE val_species_from_occs ADD COLUMN "parentKey" BIGINT;
ALTER TABLE val_species_from_occs ADD COLUMN "authorship" TEXT;

SELECT DISTINCT("taxonRank") FROM val_species_from_occs;
SELECT * FROM val_species_from_occs WHERE "taxonRank"='UNRANKED'; --150 these are all BOLD:AAA#### taxa
SELECT * FROM val_species_from_occs WHERE "taxonRank"='FORM'; --100 These are all ID'd to SPECIES.

UPDATE val_species_from_occs SET "parentKey" = "speciesKey" WHERE "taxonRank"='SUBSPECIES'; --859
UPDATE val_species_from_occs SET "parentKey" = "speciesKey" WHERE "taxonRank"='VARIETY'; --1238
UPDATE val_species_from_occs SET "parentKey" = "speciesKey" WHERE "taxonRank"='FORM'; --100
UPDATE val_species_from_occs SET "parentKey" = "genusKey" WHERE "taxonRank"='SPECIES'; --17010
UPDATE val_species_from_occs SET "parentKey" = "familyKey" WHERE "taxonRank"='GENUS'; --2202
UPDATE val_species_from_occs SET "parentKey" = "orderKey" WHERE "taxonRank"='FAMILY'; --378
UPDATE val_species_from_occs SET "parentKey" = "classKey" WHERE "taxonRank"='ORDER'; --94
UPDATE val_species_from_occs SET "parentKey" = "phylumKey" WHERE "taxonRank"='CLASS'; --60
UPDATE val_species_from_occs SET "parentKey" = "kingdomKey" WHERE "taxonRank"='PHYLUM'; --35
UPDATE val_species_from_occs SET "parentKey" = "kingdomKey" WHERE "taxonRank"='KINGDOM'; --6

ALTER TABLE val_species_from_occs ADD COLUMN "updatedAt" TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
CREATE TRIGGER trigger_updated_at
    BEFORE UPDATE
    ON public.val_species_from_occs
    FOR EACH ROW
    EXECUTE FUNCTION public.set_updated_at();

--find all the primary (source) taxonKeys whose acceptedTaxonKeys are missing (count: 2223)
--Species from occurrences only contain species IDs provided with the occurrence. All those missing IDs are SYNONYMS

SELECT DISTINCT ON ("missingKey")
"sourceKey", "missingKey", "taxonRank", "column"
FROM (
  select distinct ON (b."acceptedTaxonKey")
  b."taxonKey" as "sourceKey", b."acceptedTaxonKey" as "missingKey", b."taxonRank", 'acceptedTaxonKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."acceptedTaxonKey"
  where (a."taxonKey" IS NULL) and b."acceptedTaxonKey" IS NOT NULL and b."acceptedTaxonKey" != 0
  union
  --retrieve a list of parentKey which lack a primary definition (no taxonKey)
  select distinct ON (b."parentKey")
  b."taxonKey" as "sourceKey", b."parentKey" as "missingKey", b."taxonRank", 'parentKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."parentKey"
  where (a."taxonKey" IS NULL) and b."parentKey" IS NOT NULL and b."parentKey" != 0
  union
  --retrieve a list of kingdomKey which lack a primary definition (no taxonKey)
  select distinct ON (b."kingdomKey")
  b."taxonKey" as "sourceKey", b."kingdomKey" as "missingKey", b."taxonRank", 'kingdomKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."kingdomKey"
  where (a."taxonKey" IS NULL) and b."kingdomKey" IS NOT NULL and b."kingdomKey" != 0
  union
  --retrieve a list of phylumKey which lack a primary definition (no taxonKey)
  select distinct ON (b."phylumKey")
  b."taxonKey" as "sourceKey", b."phylumKey" as "missingKey", b."taxonRank", 'phylumKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."phylumKey"
  where (a."taxonKey" IS NULL) and b."phylumKey" IS NOT NULL and b."phylumKey" != 0
  union
  --retrieve a list of classKey which lack a primary definition (no taxonKey)
  select distinct ON (b."classKey")
  b."taxonKey" as "sourceKey", b."classKey" as "missingKey", b."taxonRank", 'classKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."classKey"
  where (a."taxonKey" IS NULL) and b."classKey" IS NOT NULL and b."classKey" != 0
  union
  --retrieve a list of orderKey which lack a primary definition (no taxonKey)
  select distinct ON (b."orderKey")
  b."taxonKey" as "sourceKey", b."orderKey" as "missingKey", b."taxonRank", 'orderKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."orderKey"
  where (a."taxonKey" IS NULL) and b."orderKey" IS NOT NULL and b."orderKey" != 0
  union
  --retrieve a list of familyKey which lack a primary definition (no taxonKey)
  select distinct ON (b."familyKey")
  b."taxonKey" as "sourceKey", b."familyKey" as "missingKey", b."taxonRank", 'familyKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."familyKey"
  where (a."taxonKey" IS NULL) and b."familyKey" IS NOT NULL and b."familyKey" != 0
  union
  --retrieve a list of genusKey which lack a primary definition (no taxonKey)
  select distinct ON (b."genusKey")
  b."taxonKey" as "sourceKey", b."genusKey" as "missingKey", b."taxonRank", 'genusKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."genusKey"
  where (a."taxonKey" IS NULL) and b."genusKey" IS NOT NULL and b."genusKey" != 0
  union
  --retrieve a list of speciesKey which lack a primary definition (no taxonKey)
  select distinct ON (b."speciesKey")
  b."taxonKey" as "sourceKey", b."speciesKey" as "missingKey", b."taxonRank", 'speciesKey' as column
  from val_species_from_occs a
  right join val_species_from_occs b
  on a."taxonKey" = b."speciesKey"
  where (a."taxonKey" IS NULL) and b."speciesKey" IS NOT NULL and b."speciesKey" != 0
) agg
