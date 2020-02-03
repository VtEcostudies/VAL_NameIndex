/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 09_find_missing_taxon_ids_from_results.js

  Purpose: Fill-in higher-order taxon data in val_speces db for a single input
  file.

  NOTE: THIS FILE IS DEPRECATED. File 06 now does this for the entire database
  in a single step.

  Specifics:

  Create a local, unique list of taxonIds and INSERT them into a temporary
  table, 'val_gbif_taxon_id', for processing by 06 file.

  Before inserting, this truncates val_gbif_taxon_id.

  After running this, run 06 file to GET and INSERT missing data.

*/

//const readline = require('readline');
//const fs = require('fs');
const Request = require("request");
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const csvFileTo2DArray = require('./99_parse_csv_to_array').csvFileTo2DArray;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding missing taxonId data file - INCLUDING TRAILING SLASH
var baseName = '';
//baseName = 'spidersVTlist';
//baseName = 'WhirligigBeetles';
//baseName = 'BombusChecklist';
//baseName = 'Orthoptera_Vermont';
//baseName = 'Ticks_Vermont';
//baseName = 'Spiders_Vermont';
//baseName = 'Amphibian_Reptile_Vermont';
//baseName = 'Robber_Flies_Vermont';
//baseName = 'Butterflies_Vermont';
//baseName = 'Crayfish_Vermont';
//baseName = 'Dragonflies_Damselflies_Vermont';
//baseName = 'Fish_Vermont';
//baseName = 'Freshwater_Mussels_Vermont';
//baseName = 'Plants_Vermont';
baseName = 'Syrphids_Vermont';

var subDir = 'dwca-checklist-crickets_katydids-vt-v1.4/'; // - INCLUDING TRAILING SLASH
subDir = 'dwca-checklist_vermont_mammals-v1.2/';
subDir = baseName + '/';
var baseFileName = baseName + '.csv'; //'taxon.txt';
var inputFileName = 'val_' + baseFileName;
var logFileName = 'inserted_taxonIds_' + baseFileName;
var taxonIdObj = {}; //an object with keys for all taxonIds referenced here
var wStream = {};

getResults().then(async (res) => {
  for (var i=0; i<res.rows.length; i++) {
    await buildTaxonIdArr(i, res.rows[i]); //this 'await' works.
  }
  //console.dir(taxonIdObj);
  truncateTableValGbifTaxonId().then(async () => {
    for (const id in taxonIdObj) {
      //console.log(id);
      insertValGbifTaxonId(id)
        .then((res) => {}).catch((err) => {});
    }
  }).catch((err) => {});
}).catch((err) => {console.log('getResults ERROR', err.message);});

/*
Get the resulting output file from a successful 08 process, and parse into
a 2D array of rows for processing here.
*/
async function getResults() {
  return await csvFileTo2DArray(dataDir+subDir+inputFileName);
}

/*
Process one row of the incoming file. Extract all taxonId keys listed and add
them to a local object that lists all higher-order keys or other refernced
taxonIds.

Create a complete list of gbif taxonIds NOT in the incoming dataset that fill
out the higher order taxonomic tree for those incoming data.
*/
function buildTaxonIdArr(idx, val) {
  log(`${idx} | buildTaxonIdArr | gbifId:${val.taxonId}`);
  try {
    if (val.taxonId != val.acceptedNameUsageID) {
      taxonIdObj[val.acceptedNameUsageId] = 1;}
    taxonIdObj[val.kingdomId] = 1;
    taxonIdObj[val.phylumId] = 1;
    taxonIdObj[val.classId] = 1;
    taxonIdObj[val.orderId] = 1;
    taxonIdObj[val.familyId] = 1;
    taxonIdObj[val.genusId] = 1;
    if (val.taxonId != val.speciesId) {
      taxonIdObj[val.speciesId] = 1;}
  } catch (err) {
    log(`buildTaxonIdArr | ERROR:${err}`);
  }
}

async function insertValGbifTaxonId(taxonId) {
  var vals = [taxonId]; //has to be an array for node-pg
  const text = `insert into val_gbif_taxon_id ("gbifId") values ($1)`;
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text, vals);
      log('insertValGbifTaxonId INSERTED', taxonId, 'into val_gbif_taxon_id.');
      resolve(res);
    } catch (err) {
      console.log('insertValGbifTaxonId ERROR', err.message, err.detail);
      reject(err);
    }
  });
}

async function truncateTableValGbifTaxonId() {
  const text = `truncate table val_gbif_taxon_id`;
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text);
      console.log('truncateTableValGbifTaxonId SUCCESS');
      resolve(res);
    } catch (err) {
      console.log('truncateTableValGbifTaxonId ERROR', err.message, err.detail);
      reject(err);
    }
  });
}

function log(out) {
  console.log(out);
  if (!wStream['log']) {
    wStream['log']=fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
  }
  wStream['log'].write(`${out}\n`);
}
