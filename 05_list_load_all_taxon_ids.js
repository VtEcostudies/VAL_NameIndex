/*
  Author: Jason Loomis

  Project: GBIF_Species_to_VAL_Species

  Parse GBIF species download DWcA into a VAL species list DwCA that can be
  used by the VAL ALA nameindexer.

  As of the fall of 2019, the GBIF API does not provide a species download that
  includes checklists. Instead, they provide an occurrence download that
  enumarates species.

  File: 05_list_all_taxon_ids.js

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

*/

//https://nodejs.org/api/readline.html
var readline = require('readline');
var fs = require('fs');
var Request = require("request");
var paths = require('./00_config').paths;
var delim = require('./00_config').delim;
var csvLineTo1DArray = require('./99_parse_csv_to_array').csvLineTo1DArray;

const query = require('./database/db_postgres').query;

const id = delim.infield;
const od = delim.outfield;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dDir = paths.dwcaDir; //path to directory holding extracted GBIF DWcA species files
var readFile = 'gbif_species.txt';
var wStream = []; //array of write streams
var writeFile = 'val_taxon_id.txt';

var idx = 0; //read file row index
var gbif = [];

var taxonIdObj = {}; //an object with keys for all taxonIds referenced here

var fRead = readline.createInterface({
  input: fs.createReadStream(`${dDir}/${readFile}`)
});

//read gbif species file
fRead.on('line', function (row) {
    gbif = csvLineTo1DArray(row, id);

    //add to the object of all taxon keys
    if (idx > 0) {
      taxonIdObj[gbif[0]] = 1;
      taxonIdObj[gbif[2]] = 1;
      taxonIdObj[gbif[7]] = 1;
      taxonIdObj[gbif[9]] = 1;
      taxonIdObj[gbif[11]] = 1;
      taxonIdObj[gbif[13]] = 1;
      taxonIdObj[gbif[15]] = 1;
      taxonIdObj[gbif[17]] = 1;
    }

    idx++;
});

fRead.on('close', async function() {
  console.log('Read file closed.');
  wStream[0] = fs.createWriteStream(`${dDir}/${writeFile}`, {flags: 'w'});
  for (var key in taxonIdObj) {
    wStream[0].write(`${key}\n`);
    insertValGbifTaxonId(idx, key).catch(err => {});
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
      err.val = val; //need to return incoming object to caller - attach to err object
      err.idx = idx; //need to return incoming index to caller - attach to err object
      reject(err);
    }
  });
}
