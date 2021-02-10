/*
Investigating the ALA default CoL index and Spain's use of the GBIF backbone, I find that they both handle synonyms differently,
and different from how I've handled it. For SYNONYMS:
1) CoL NEVER has a value for parentNameUsageID

Spain's index does not include a column for speces AT ALL.

Experimental method:
1) try changing all species/Id to scientificName/Id, not acceptedName/Id
if that doesn't work:
2) try setting all parentnameUsage/Id to null
if that doesn't work:
3) try emulating what Spain does...
*/
select * from val_species 
WHERE "taxonomicStatus" LIKE '%synonym' 
AND "taxonRank" = 'species' 
AND "scientificName" != "species"; --789

update val_species set "species"="scientificName", "speciesId"="taxonId" 
WHERE "taxonomicStatus" LIKE '%synonym' 
AND "taxonRank" = 'species' 
AND "scientificName" != "species"; --789

select * from val_species where "taxonomicStatus" LIKE '%synonym' 
AND "taxonRank" IN ('subspecies','variety','form') 
AND "scientificName" != "species"; --431

select * from val_species where "taxonomicStatus" NOT LIKE '%synonym' 
AND "taxonRank" IN ('subspecies','variety','form');
