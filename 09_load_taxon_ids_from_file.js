/*
  Author: Jason Loomis

  Project: GBIF_Species_to_VAL_Species

  Parse GBIF species download DWcA into a VAL species list DwCA that can be
  used by the VAL ALA nameindexer.

  As of the fall of 2019, the GBIF API does not provide a species download that
  includes checklists. Instead, they provide an occurrence download that
  enumarates species.

  File: 09_load_taxon_ids_from_file.js

*/

var readline = require('readline');
var fs = require('fs');
var Request = require("request");
var paths = require('./00_config').paths;

const query = require('./database/db_postgres').query;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding missing taxonId data file - INCLUDING TRAILING SLASH
var baseName = 'spidersVTlist';
baseName = 'WhirligigBeetles';
baseName = 'BombusChecklist';
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
  for (var key in taxonIdObj) {
    insertValGbifTaxonId(i++, key).catch(err => {});
  }
});

async function insertValGbifTaxonId(idx, val) {
  var vals = [val];
  const text = `insert into val_gbif_taxon_id ("gbifId") values ($1)`;
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text, vals);
      resolve(res);
    } catch (err) {
      console.log(idx , 'insertValGbifTaxonId', err.message, err.detail);
      reject(err);
    }
  });
}
