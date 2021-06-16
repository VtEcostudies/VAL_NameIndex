/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 05_2_insert_missing_gbif_species_dwca.js

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
const pgUtil = require('./database/db_pg_util');
const query = require('./database/db_postgres').query;
const csvFileToArrayOfObjects = require('./VAL_Utilities/99_parse_csv_to_array').csvFileTo2DArray;
const csvLineTo1DArray = require('./VAL_Utilities/99_parse_csv_to_array').csvLineTo1DArray;
const parseCanonAuthor = require('./VAL_Utilities/98_gbif_to_val_columns').parseCanonAuthorFromScientificRank;
const gbifOccSpeciesToValDirect = require('./VAL_Utilities/98_gbif_to_val_columns').gbifOccSpeciesToValDirect;
const log = require('./VAL_Utilities/97_utilities').log;

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

var staticColumns = [];

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

var wStream = []; //array of write streams
wStream[0] = fs.createWriteStream(`${dataDir}${subDir}${fileName}.log`);
wStream[1] = fs.createWriteStream(`${dataDir}${subDir}${missingFile}`, {flags: 'w'});
wStream[2] = fs.createWriteStream(`${dataDir}${subDir}${foundFile}`, {flags: 'w'});
wStream[3] = fs.createWriteStream(`${dataDir}${subDir}${discardFile}`, {flags: 'w'});

var insert = 1;

process.on('exit', function(code) {
  console.log(`missing:${missCount}|found:${foundCount}|discard:${discardCount}|rowTotal:${rowTotal}|uniqueTotal:${uniqueTotal}`);
  return console.log(`About to exit with code ${code}`);
});

getColumns()
  .then(col => {
    getSpeciesFile(dataDir+subDir+fileName+fileExtn)
      .then(async src => {
        log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
        rowCount = src.rows.length;
        for (var i=0; i<src.rows.length; i++) {
          var gbif = src.rows[i];
          var canonAuthor = parseCanonAuthor(src.rows[i].scientificName, src.rows[i].taxonRank);
          gbif.canonicalName = canonAuthor.canon; //gbif field name
          gbif.authorship = canonAuthor.author; //gbif field name
          queryValSciName(gbif.canonicalName, i, gbif)
            .then(res => {
              foundCount++;
              //console.log(`FOUND|${res.key}|COUNT|${foundCount}`);
              wStream[2].write(`FOUND|${res.key}|COUNT|${foundCount}\n`);
            })
            .catch(err => {
              if (err.missing) { //our flag that an error is a valid 'NOT FOUND'
                missCount++;
                console.log(`MISSING|${err.key}|COUNT|${missCount}`);
                wStream[1].write(`MISSING|${err.key}|COUNT|${missCount}\n`);
                if (insert) {
                  var val = gbifOccSpeciesToValDirect(err.gbf);
                  console.log(val);
                  insertValTaxon(val, missCount) //count missing taxa inserts not file indexes
                    .then(res => {
                      log(`${res.idx} | insertValTaxon SUCCESS | ${JSON.stringify(res.rows[0])}`, wStream[0], true);
                    })
                    .catch(err => {
                      log(`${err.idx} | insertValTaxon ERROR | ${err.code} | ${err.message} | ${err.detail}`, wStream[0], true);
                    });
                }
              } else if (err.discard) {
                discardCount++;
                //console.log(`DISCARD|${err.key}|COUNT|${discardCount}`);
                wStream[3].write(`DISCARD|${err.key}|COUNT|${discardCount}\n`);
              } else {
                console.log('queryValSciName | ERROR | key:', err.key, 'message:', err.message);
              }
            });
        }
      })
      .catch(err => {
        log(`getSpeciesFile ERROR | ${JSON.stringify(err)}`, wStream[0], true);
      });
    })
    .catch(err => {
      log(`getColumns ERROR | ${JSON.stringify(err)}`, wStream[0], true);
    })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_species", staticColumns);
}

/*
Parse the input file into an array of objects for processing.
*/
async function getSpeciesFile(inpFileName) {
  try {
    return await csvFileToArrayOfObjects(inpFileName, fileDelim, true, true);
  } catch(err) {
    throw(err);
  }
}

/*
Insert the fixed-up val database object. On error, return the val object for
downstream processing.
*/
async function insertValTaxon(val, idx) {

  var qryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
  var sql_insert = `insert into val_species (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;

  return new Promise((resolve, reject) => {
    query(sql_insert, qryColumns.values)
      .then(res => {
        res.val = val;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        err.val = val;
        err.idx = idx;
        reject(err);
      })
  })
}

function queryValSciName(key, idx, gbif) {
  var sql_select = `select * from val_species where "scientificName"=$1`;
  return new Promise((resolve, reject) => {
    if (0 == Number(key)) {reject({message:'Empty key.', key:key, idx:idx});}
    //console.log('queryValSciName', sql_select, key, idx);
    query(sql_select, [key])
      .then(res => {
        res.key = key;
        res.idx = idx;
        res.gbf = gbif;
        if (0 === res.rows.length) {
          res.message=`Not found: ${key}`;
          if (discardCheck(key)) {res.discard = 1;} //flag a junk value
          else {res.missing = 1;} //flag a simple missing value
          reject(res);
        } else {
          resolve(res);
        }
      })
      .catch(err => {
        err.key = key;
        err.idx = idx;
        err.gbf = gbif;
        reject(err);
      })
  })
}

/*
  Discard records that are clearly not useful to us:

  eg. 'BOLD:XXXX'
*/
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
