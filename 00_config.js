/*
VAL DE Vermont Registry of Names Process Roadmap

These are the steps to add new species/names to the Vermont Registry of Names (VCE nomenclature), and to
integrate that name index into the VAL DE ALA/GBIF Server for indexing of occurrences.

This process and system were purpose-built after trying to do without one. These are it's parts:

- a postgres database serves as a local repository for species names
- data inputs are:
  - (A) Species lists (checklists) hand-built by VAL scientists
  - (B) GBIF SPECIES_LIST updates (nominally quarterly) which accompany GBIF Occurrence Updates
- A set of node.js processing scripts are used to ingest checklists into Postgres
- we export a complete species registry from postgres (also a DwCA-format file)
- deliver the registry to the VAL server and build the nameindex from it

Originally, we bootstrapped a dataset from GBIF by downloading all species within a geo bounding-box of VT.
A series of steps no longer used - files 01 thru 05 - resulted from the learning process of massaging the
incoming and outgoing data-dumps to the point where an export file from postgres could be consistently used
as an input to the VAL DE ALA/GBIF nameindexer.

(A) For each new species dataset the following steps must be done:

0) Edit 00_config.js (this file):
  - change exports.paths.baseName to the sub-directory and filename containing the input DwCA

1) node 08_ingest_species_list_val_db_create_dwca.js
  - Process incoming records
  - Match each record against GBIF to get a GBIF taxonId
  - Insert into the local Postgres db (you must set the flag dbInsert=1)
  - Write each record matched to a new output DwCA file with all fields filled (for publishing to our IPT)
  - Write each record not matched to err_{incoming_file_name}

2) node 09_ingest_species_list_new_not_found.js
  - We created this ad-hoc procedure to deal with taxa not found in GBIF, or for which VCE's scientists have
    expertise that clearly finds GBIF's taxonomy to be in error.
  - To do this, and since these taxa lack a GBIF taxonKey, we must create a custom taxonId in val_species

3) node 06_find_missing_gbif_id_add_to_val_db.js
  - Queries our db and finds missing entries in the taxonomic tree
  - Uses GBIF API to fill in missing taxa
  - Use repeatedly to complete the process
  - NOTE: 'duplicate key value' errors on first and subsequent passes are usually
    the result of the initial query for errors getting multiple instances for the
    same missing key.

(B) Each time we update Occurrences in VAL DE, we must update val_species using the SPECIES_LIST downlaoded
with GBIF Occurrences. We created a new workflow for this:

1) Download and extract GBIF SPECIES_LIST(s) and put into main folder
    ...\VAL_Data_Pipelines\VAL_NameIndex\gbif_species_update
  with a subfolder named like:
    'gbif_species_2022_01_19'
  and name the species-update files like eg:
    'gbif_species_update_w_loc.tsv'
    'gbif_species_update_wo_loc.tsv'

2) BACKUP THE val_species table before you do step (4) below

3) Open 08_ingest_gbif_species_update.js and ALTER these values to reflect the above:

    const subDir = 'gbif_species_2022_01_19/';
    var fileName = 'gbif_species_update_wo_loc';
    var fileExt = '.tsv'; (default is 'csv' for a tab-delimted file from GBIF which bends the mind)

4) RUN: node 08_ingest_gbif_species_update.js
  - logging to screen and file shows how many MISSING, ERRORS, INSERTS

5) node 06_find_missing_gbif_id_add_to_val_db.js
  - Queries our db and finds missing entries in the taxonomic tree
  - Uses GBIF API to fill in missing taxa
  - Use repeatedly to complete the process
  - NOTE: 'duplicate key value' errors on first and subsequent passes are usually
    the result of the initial query for errors getting multiple instances for the
    same missing key.

6) LOAD vernacular names:
  - Open a VPN Connection to keep iNat from blacklisting you, then:
  - node 10_3_get_insert_inat_vernacular.js

7) Export a new set of DwCA source files for the VAL DE nameindexer, run the sql
    script for your needs, one of:
  - C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\sql\export_all_csv.sql
  - C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\sql\export_all_species_from_occs_csv.sql

8) Move files to VAL DE server using this file:
  - C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\scp_to_val-core_server.bat
  Which runs a command like this:
  - scp -i "C:/Users/jtloo/.ssh/vce_live_aws_key_pair.pem" ./name-indexer/*.* ubuntu@vtatlasoflife.org:~/nameindex/val-dwca

9) Log on to VAL DE core server using a file like this:
  -

10) On ALA Core Server, move source and build new name indexer:
  $ cp ~/nameindex/val-dwca/* to /data/lucene/sources/val-dwca
  $ cd /data/lucene/sources
  $ ./val_before_nameindexer.sh
  $ ./val_nameindexer.sh

  You should see output indicating successful indexing of taxa and common names. If so, proceed to index occurrences.
*/
exports.tables = {
  source: {
    speciesTable: 'new_species',
    vernacularTable: 'new_vernacular',
    errorTable: 'species_err'
  },
  output: {
    speciesTable: 'mval_species',
    vernacularTable: 'mval_vernacular',
    errorTable: 'species_err'
  }
};

exports.paths = {
  dwcaDir: "../",
  dataDir: "../datasets/",
  gbifDir: "../gbif_species_update/",
  //----------------------fileNames
  //fileName: "Moths_Vermont_V5",
  //----------------------baseNames
  //baseName = 'Add_Hoc_Taxa';
  //fileName = 'species_Propylea_quatuordecimpunctata';
  //fileName = 'Moths_Errata';
  //baseName: 'Bees_Vermont'
  //baseName: "Moths_Vermont"
  //baseName: "Hippoboscidae_Vermont"
  //baseName: 'spidersVTlist'
  //baseName: 'WhirligigBeetles'
  //baseName: 'BombusChecklist'
  //baseName= 'CicadaVermont'
  //baseName= 'Orthoptera_Vermont'
  //baseName= 'Ticks_Vermont'
  //baseName= 'Spiders_Vermont'
  //baseName= 'Amphibian_Reptile_Vermont'
  //baseName: 'Robber_Flies_Vermont'
  //baseName: 'Butterflies_Vermont'
  //baseName: 'Crayfish_Vermont'
  //baseName: 'Dragonflies_Damselflies_Vermont'
  //baseName: 'Fish_Vermont'
  //baseName: 'Freshwater_Mussels_Vermont'
  //baseName: 'Plants_Vermont'
  //baseName: 'Syrphids_Vermont'
  //baseName: 'Error_Corrections'
  //baseName: 'Springtails_VT'
  //baseName: 'Bryophytes_Vermont'
  //baseName: 'Vermont_Conservation_Missing' //the not-found taxa from adding Vermont_Conservation_Status
  //baseName: 'Cluster_Flies_Vermont'
  //baseName: 'Bark_Beetles_Vermont'
  //baseName: 'Ants_Vermont'
  //baseName: 'Introduced_Fungi_Vermont'
};

//infield: field-delimiter for incoming gbif_species.txt
//outfield: field-delimiter for outgoing val_spceies.txt
exports.delim = {
  infield: "\t",
  outfield: ","
};

exports.urls = {
  val_docker_beta: "https://beta.vtatlasoflife.org",
  primary:  "https://vtatlasoflife.org",
  collectory: "https://collectory.vtatlasoflife.org"
};
