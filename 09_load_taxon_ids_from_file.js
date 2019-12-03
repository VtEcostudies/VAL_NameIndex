/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 09_load_taxon_ids_from_file.js

  Purpose: Fill-in higher-order taxon data in val_speces db.

  Specifics:

  Populate the val_gbif_taxon_id table with taxonIds from a local file.

  Prior to running this, truncate val_gbif_taxon_id.

  After running this, run 06 file to GET and INSERT missing data.

*/

const logHand = require('why-is-node-running');
const readline = require('readline');
const fs = require('fs');
const Request = require("request");
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;

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
baseName = 'Plants_Vermont';

var subDir = 'dwca-checklist-crickets_katydids-vt-v1.4/'; // - INCLUDING TRAILING SLASH
subDir = 'dwca-checklist_vermont_mammals-v1.2/';
subDir = baseName + '/';
var baseFileName = baseName + '.csv'; //'taxon.txt';
var inputFileName = 'missing_taxonIds_' + baseFileName;
var outputFileName = 'inserted_taxonIds_' + baseFileName;

var idx = 0; //read file row index
var taxonIdObj = {}; //an object with keys for all taxonIds referenced here

var fRead = readline.createInterface({
  input: fs.createReadStream(`${dataDir}${subDir}${inputFileName}`)
});

//read missing taxonId file and add to local object
fRead.on('line', function (row) {
  console.log(idx, row);
  taxonIdObj[row] = 1;
  idx++;
});

fRead.on('close', async function() {
  var i = 0;
  console.log('Read file closed.');
  //Iterate over missing taxonIds and attempt to insert them into the table
  //val_gbif_taxon_id. Later, a query is used to reconcile taxonIds in that
  //table that are missing from val_species.
  truncateTableValGbifTaxonId()
    .then(() => {
    for (var key in taxonIdObj) {
      insertValGbifTaxonId(i++, key).catch(err => {});
    }
  })
  .catch((err) => {});

  setTimeout(function () {logHand();}, 1000) //attempt to find why process hangs

});

async function insertValGbifTaxonId(idx, val) {
  var vals = [val];
  const text = `insert into val_gbif_taxon_id ("gbifId") values ($1)`;
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text, vals);
      resolve(res);
    } catch (err) {
      console.log(idx, 'insertValGbifTaxonId', err.message, err.detail);
      reject(err);
    }
  });
}

async function truncateTableValGbifTaxonId() {
  const text = `truncate table val_gbif_taxon_id`;
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text);
      resolve(res);
    } catch (err) {
      console.log('truncateTableValGbifTaxonId ERROR', err.message, err.detail);
      reject(err);
    }
  });
}
