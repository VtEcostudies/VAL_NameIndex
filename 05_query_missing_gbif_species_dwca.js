/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 05_list_all_taxon_ids.js

  Purpose: Create a list of taxonIds for all referenced higher-order taxa in the
  source species list file. Populate a single-column table in the PostGRES db
  with that list of unique taxonIds.

  Later, in file 06, we query missing primary taxonIds in table val_species which
  are listed in table val_gbif_taxon_id, then add them to the val_species table.

  Specifics:

  index GBIF name
  0	taxonKey
  1	scientificName
  2	acceptedTaxonKey
  3	acceptedScientificName
  4	numberOfOccurrences
  5	taxonRank
  6	taxonomicStatus
  7	kingdom
  8	kingdomKey
  9	phylum
  10	phylumKey
  11	class
  12	classKey
  13	order
  14	orderKey
  15	family
  16	familyKey
  17	genus
  18	genusKey
  19	species
  20	speciesKey
  21	iucnRedListCategory

*/

//https://nodejs.org/api/readline.html
var readline = require('readline');
var fs = require('fs');
var Request = require("request");
var paths = require('./00_config').paths;
var delim = require('./00_config').delim;
const query = require('./database/db_postgres').query;
const csvLineTo1DArray = require('./VAL_Utilities/99_parse_csv_to_array').csvLineTo1DArray;
const csvFileToArrayOfObjects = require('./VAL_Utilities/99_parse_csv_to_array').csvFileTo2DArray;
const parseCanonical = require('./VAL_Utilities/97_utilities').parseCanonFromScientificRank;

const id = delim.infield;
const od = delim.outfield;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dwcaDir = paths.dwcaDir; //path to directory holding extracted GBIF DWcA species files
var dataDir = paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var baseName = paths.baseName; //moved this setting to 00_config.js, as it's used in downstream processing
var fileName = paths.fileName;
if (!fileName) {fileName = baseName;}
//fileName = fileName + '_test';
var subDir = baseName + '/';
var fileExtn = ".txt";
var fileDelim = "\t";

var wStream = []; //array of write streams
var writeFile = 'gbif_taxon_id.txt';
var missingFile = 'val_db_missing_gbif_name.txt';
var foundFile = 'val_db_found_gbif_name.txt';
var discardFile = 'val_db_discard_gbif_name.txt';

var idx = 0; //read file row index
var gbif = [];
var test = 1; //don't insert, just query db for taxonKey (gbifId)

var taxonIdObj = {}; //an object with keys for all taxonIds referenced here
var taxonNames = {}; //an object with scientificNames
var taxonCanon = {}; //an object with canonicalNames

missCount = 0;
foundCount = 0;
discardCount = 0;
rowTotal = 0;
uniqueTotal = 0;

process.on('exit', function(code) {
  console.log(`missing:${missCount}|found:${foundCount}|discard:${discardCount}|rowTotal:${rowTotal}|uniqueTotal:${uniqueTotal}`);
  return console.log(`About to exit with code ${code}`);
});

var fRead = readline.createInterface({
  input: fs.createReadStream(`${dataDir}${subDir}${fileName}${fileExtn}`)
});

//read gbif species file
fRead.on('line', function (row) {
    //console.log(row);
    gbif = csvLineTo1DArray(row, fileDelim);
    //console.log(gbif);
    //add to the object of all taxon keys
    if (idx > 0) {

      taxonIdObj[gbif[0]] = 1;
      taxonIdObj[gbif[2]] = 1;
      taxonIdObj[gbif[8]] = 1;
      taxonIdObj[gbif[10]] = 1;
      taxonIdObj[gbif[12]] = 1;
      taxonIdObj[gbif[14]] = 1;
      taxonIdObj[gbif[16]] = 1;
      taxonIdObj[gbif[18]] = 1;
      taxonIdObj[gbif[20]] = 1;

      taxonNames[gbif[1]] = gbif[5];
      canon = parseCanonical(gbif[1], gbif[5]).canon;
      taxonCanon[canon] = gbif;
    }
    idx++;
});

fRead.on('close', async function() {
  console.log('Read file closed.');
  rowTotal = idx-1; //0th row is header
  if (test) {
    wStream[1] = fs.createWriteStream(`${dataDir}${subDir}${missingFile}`, {flags: 'w'});
    wStream[2] = fs.createWriteStream(`${dataDir}${subDir}${foundFile}`, {flags: 'w'});
    wStream[3] = fs.createWriteStream(`${dataDir}${subDir}${discardFile}`, {flags: 'w'});
    wStream[4] = fs.createWriteStream(`${dataDir}${subDir}taxonCanon.txt`, {flags: 'w'});
    wStream[4].write(JSON.stringify(taxonCanon));
  }
  else {
    wStream[0] = fs.createWriteStream(`${dataDir}${subDir}${writeFile}`, {flags: 'w'});
  }

  var i = 0, key = 0;
  uniqueTotal = Object.keys(taxonCanon).length;
  //for (var sci in taxonNames) { i++; key = parseCanonical(sci, taxonNames[sci]).canon;
  for (var key in taxonCanon) { i++;
    console.log(i, '|', taxonCanon[key][5], '|', key); //continue;
    queryValSciName(key, i, gbif)
      .then(res => {
        foundCount++;
        console.log(`FOUND|${res.key}|COUNT|${foundCount}`);
        wStream[2].write(`FOUND|${res.key}|COUNT|${foundCount}\n`);
      })
      .catch(err => {
        if (err.missing) { //our flag that an error is a valid 'NOT FOUND'
          missCount++;
          console.log(`MISSING|${err.key}|COUNT|${missCount}`);
          wStream[1].write(`MISSING|${err.key}|COUNT|${missCount}\n`);
        } else if (err.discard) {
          discardCount++;
          console.log(`DISCARD|${err.key}|COUNT|${discardCount}`);
          wStream[3].write(`DISCARD|${err.key}|COUNT|${discardCount}\n`);
        } else {
          console.log('queryValSciName | ERROR | key:', err.key, 'message:', err.message);
        }
      });
    }

/*
  for (var key in taxonIdObj) { i++;
    //console.log(i, key);
    if (test) {
      queryValGbifTaxonId(key, i)
        .then(res => {
          foundCount++;
          console.log(`FOUND|${res.key}|COUNT|${foundCount}`);
          wStream[2].write(`FOUND|${res.key}|COUNT|${foundCount}\n`);
        })
        .catch(err => {
          if (err.missing) {
            missCount++;
            console.log(`MISSING|${err.key}|COUNT|${missCount}`);
            wStream[1].write(`MISSING|${err.key}|COUNT|${missCount}\n`);
          } else {
            console.log('queryValGbifTaxonId | ERROR | key:', err.key, 'message:', err.message);
          }
        });
    } else {
      //wStream[0].write(`${key}\n`);
      insertValGbifTaxonId(key, i)
      .catch(err => {
        console.log('insertValGbifTaxonId ERROR', err.message);
      });
    }
  }
*/

});

async function insertValGbifTaxonId(key, idx) {
  const sql = `insert into val_gbif_taxon_id ("gbifId") values ($1)`;
  return new Promise((resolve, reject) => {
    query(sql, [key])
      .then(res => {
        console.log(idx , 'insertValGbifTaxonId', res);
        resolve(res);
      })
      .catch(err => {
      console.log(idx , 'insertValGbifTaxonId', err.message, err.detail);
      err.key = key; //need to return incoming object to caller - attach to err object
      err.idx = idx; //need to return incoming index to caller - attach to err object
      reject(err);
    });
  });
}

function queryValGbifTaxonId(key, idx) {
  var sql_select = `select * from val_species where "gbifId"=$1`;
  return new Promise((resolve, reject) => {
    if (0 == Number(key)) {reject({message:'Empty key.', key:key, idx:idx});}
    //console.log('queryValGbifTaxonId', sql_select, key, idx);
    query(sql_select, [key])
      .then(res => {
        //console.log('queryValGbifTaxonId', key, idx, res);
        res.key = key;
        res.idx = idx;
        if (0 === res.rows.length) {
          res.message=`Not found: ${key}`;
          res.missing = 1; //flag a simple missing value
          reject(res);
        } else {
          resolve(res);
        }
      })
      .catch(err => {
        //console.log('queryValGbifTaxonId', key, idx, err);
        err.key = key;
        err.idx = idx;
        reject(err);
      })
  })
}

function queryValSciName(key, idx, gbif) {
  var sql_select = `select * from val_species where "scientificName"=$1`;
  return new Promise((resolve, reject) => {
    if (0 == Number(key)) {reject({message:'Empty key.', key:key, idx:idx});}
    console.log('queryValSciName', sql_select, key, idx);
    query(sql_select, [key])
      .then(res => {
        //console.log('queryValSciName', key, idx, res);
        res.key = key;
        res.idx = idx;
        res.gbf = gbif;
        if (0 === res.rows.length) {
          res.message=`Not found: ${key}`;
          if (discardCheck(key)) {res.discard = 1;} //flag a junk value
          else {res.missing = 1;} //flag a simple missing value
          //res.missing = 1;
          reject(res);
        } else {
          resolve(res);
        }
      })
      .catch(err => {
        //console.log('queryValSciName', key, idx, err);
        err.key = key;
        err.idx = idx;
        err.gbf = gbif;
        reject(err);
      })
  })
}

function discardCheck(name) {
  var discard = 0;
  try {
    var test = name.substring(0,5).toUpperCase();
    //console.log(test);
    switch(test) {
      case 'BOLD:':
        discard = 1;
        break;
      default:
        break;
    }
  } catch(err) {
    console.log('::reject | ERROR', err);
  }

  return discard;
}
