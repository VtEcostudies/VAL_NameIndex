/*
VAL DE Vermont Registry of Names Process Roadmap

These are the steps to add new species/names to the Vermont Registry of Names (VCE nomenclature), and to
integrate that name index into the VAL DE ALA/GBIF Server for indexing of occurrences.

This process and system were purpose-built after trying to do without one. These are it's parts:

- a postgres database serves as a local repository for species names
- data inputs are species lists (checklists) hand-built by VAL scientists
- a set of node.js processing scripts are used to ingest checklists into Postgres
- we export a complete species registry from postgres (also a DwCA-format file)
- deliver the registry to the VAL server and buld the nameindex from it

Originally, we bootstrapped a dataset from GBIF by downloading all species within a geo bounding-box of VT.
A series of steps no longer used - files 01 thru 05 - resulted from the learning process of massaging the
incoming and outgoing data-dumps to the point where an export file from postgres could be consistently used
as an input to the VAL DE ALA/GBIF nameindexer.

For each new species dataset the following steps must be done:

0) Edit 00_config.js (this file):
  - change exports.paths.baseName to the sub-directory and filename containing the input DwCA

1) node 08_ingest_species_list_val_db_create_dwca.js
  - Process incoming records
  - Match each record against GBIF to get a GBIF taxonId
  - Insert into the local Postgres db (you must set the flag dbInsert=1)
  - Write each record matched to a new DwCA file with all fields completed
  - Write each record not matched to err_{incoming_file_name}

2) node 09_ingest_species_list_new_not_found.js

3) node 06_find_missing_gbif_id_add_to_val_db.ss
  - Queries our db and finds missing entries in the taxonomic tree
  - Uses GBIF API to fill in missing taxa
  - Use repeatedly to complete the process.

*/
exports.paths = {
  dwcaDir: "../",
  dataDir: "../datasets/",
  //baseName: "Moths_Vermont"
  baseName: "Hippoboscidae_Vermont"
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
  //baseName: 'Bryophytes_VT'
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
