  Author: Jason Loomis

  Project: VAL_Species

  Parse GBIF species occurrence download DwCA into a VAL species list DwCA that 
  can be used by the VAL ALA nameindexer. This processed output will also serve
  as the basis for the VAL Vermont Life List (or the VT Species Registry).

  As of the fall of 2019, the GBIF API does not provide a species download that
  includes checklists. Instead, they provide an occurrence download that
  enumerates species.

  File: 01_convert_gbif_to_val.js

  Notes:

  Specifics:

  index GBIF name              index	ALA name
  1  taxonKey                  1	taxonID
  2  scientificName            2	scientificName
  3  acceptedTaxonKey          3	acceptedNameUsageID
  4  acceptedScientificName    4	acceptedNameUsage
  5  taxonRank                 5	taxonRank
  6  taxonomicStatus           6	taxonomicStatus
  8  kingdomKey	               7	parentNameUsageID
  10 phylumKey                 8	nomenclaturalCode
  12 classKey                  9	scientificNameAuthorship
  14 orderKey                  10	specificEpithet
  16 familyKey                 11	infraspecificEpithet
  18 genusKey                  12	taxonRemarks
  20 speciesKey
                              ...also add these for future checklists:
                              13  datasetName
                              14  datasetID

  - Conversions for index values 1-6 are 1:1.

  - To derive ALA index 7, parentNameUsageID, we find the second-to-last value of
  GBIF index 8-20 and use that.

  - ALA index 8, nomenclaturalCode, will be assigned the static value 'GBIF'
  because the source nomenclatural index is the GBIF backbone, which itself
  comprises multiple nomenclatureal indices and is where this initial dataset
  originated.

  - ALA index 9, scientificNameAuthorship, will be derived from the parsed ending
  of GBIF index 4. We find authorship by removing the leading 1, 2 or 3 tokens of
  scientificName.

  File: 02_add_missing_accepted_id_from_gbif.js

  Specifics:

  While converting gbif species to val species, we found that the dataset
  had taxa rows with acceptedNameUsageID which were not defined within the file.
  This causes a referential dead-end that needs remedy.

  In the previous step, created by the file 01_convert_gbif_to_val.js, we created
  2 files:

  1) val_species.txt
  2) val_species_mistmatch.txt

  File (2) is all rows in val_species.txt where taxonId != acceptedNameUsageID. That mismatch
  is not itself a problem. However, a sub-set of those values refer to an acceptedNameUsageID
  which is not elsewhere defined with a primary taxonID within the same file val_species.txt.
  This is a problem: accepted taxa have no definition.

  There was not an easy way to write code to search for each missing acceptedNameUsageID
  within the file, and that's what relational databases are for, so we imported both
  into postgres and used a query to select just those acceptedNameUsageIDs that were
  not defined as primary taxonIDs.

  The solution:

  1) Load val_species.txt into postgres table val_species
  2) Load val_mismatch.txt into postgres table val_mismatch
  3) Query RIGHT JOIN on val_mismatch.acceptedNameUsageID NOT in
  val_species.taxonId and output to file. => 1357 results
  4) Iterate over acceptedNameUsageIDs in result set, hit GBIF API for those
  taxonIds, add them to our original val_species.txt.

  File: 03_post_process_val_species_for_nameindexer.js

  Specifics:

  The resulting output from files 01 and 02 was used as source DwCA for the ALA
  nameindexer. This did not work, because several rows of data were missing
  scientificName.

  This file uses the post-processed output file from the databse and amends it
  by copying accpetedNameUsage to scientificName where scientificName is null.

  TO-DO: make scientificName NOT NULL in the pg db.

  File: 04_gbif_to_val_db.js

  Purpose: Populate a PostGRES database with the output of the previous steps.

  Specifics:

  index GBIF name
  1  taxonKey
  2  scientificName
  3  acceptedTaxonKey
  4  acceptedScientificName
  5  taxonRank
  6  taxonomicStatus
  7  kingdom
	8  kingdomKey
  9  phylum
	10 phylumKey
  11 class
	12 classKey
  13 order
  14 orderKey
  15 family
  16 familyKey
  17 genus
  18 genusKey
  19 species
  20 speciesKey

  - The database ingests all columns, but renames them to DwCA-compliant names,
  which are used by VAL.

  - To derive parentNameUsageId, we find the second-to-last non-zero value of
  GBIF kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, or
  speciesKey. Special cases:

      - If kingdomKey is the last key, we assign parentNameUsageId to itself.
      - If acceptedScientificName contains subsp. or var., we assign speciesKey
      to parentNameUsageId.

  - nomenclaturalCode, will be assigned the static value 'GBIF' for
  Catalogue Of Life, the source nomenclatural index of the GBIF backbone, which
  is where this initial dataset originated.

  - scientificNameAuthorship, will be derived from the parsed ending of GBIF
  acceptedScientificName. We find authorship by removing the leading 1 or 2
  tokens of scientificName.

  File: 05_list_all_taxon_ids.js

  Purpose: Create a list of taxonIds for all referenced higher-order taxa in the
  source species list file. Populate a single-column table in the PostGRES db
  with that list of unique taxonIds.

  Later, in file 06, we query missing primary taxonIds in table val_species which
  are listed in table val_gbif_taxon_id, then add them to the val_species table.
