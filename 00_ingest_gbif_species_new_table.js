/*
  Project: VAL_species

  File: 00_ingest_gbif_species_new_table.js

  Purpose: Create a new VAL Vermont Species Checklist from scratch

  Datasets:
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=false&state_province=Vermont%20(State)&state_province=Vermont&advanced=1


  Details:
    - bounce incoming taxonKey against speciesTable first
    - if key is found in speciesTable, ignore
    - if key is NOT found, use that key on GBIF taxon api to key full result
    - use that result to INSERT new taxon into speciesTable.
    - Fix some missing values on insert:
      - if GBIF accepted/acceptedKey are null, set acceptedNameUsage and acceptedNameUsageId as self-referential (this is a leftover from ALA, where acceptedNameUsageId is required)
      - taxonRank == SPECIES we set specificEpithet to the 2nd name token
      - taxonRank == SUBSPECIES, VARIETY we set infraspecificEpithet to the dangling (3rd) name token

  To-Do:
    - Add updates later
*/

const fs = require('fs');
const readline = require('readline');
const Request = require('request');
const moment = require('moment');
const paths = require('./00_config').paths;
const dbConfig = require('./db_config').dbConfig;
const connect = require('./VAL_Utilities/db_postgres').connect;
const query = require('./VAL_Utilities/db_postgres').query;
const pgUtil = require('./VAL_Utilities/db_pg_util');
const csvFileToArrayOfObjects = require('./VAL_Utilities/99_parse_csv_to_array').csvFileToArrayOfObjects;
const parseCanonicalName = require('./VAL_Utilities/97_utilities').parseCanonicalName;
const log = require('./VAL_Utilities/97_utilities').log;
const gbifToValSpecies = require('./VAL_Utilities/98_gbif_to_val_columns').gbifToValSpecies;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.gbifDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var subDir = 'gbif_species_2022_11_01/';
var fileName = 'species_gbif_vt_gadm'; // 1 of 2 download files with species from occs
fileName = 'species_gbif_vt_state_province_no_coordinates'; // 2 of 2 download files with species from occs

//paths and names for Marth's Vineyard species data
dataDir = 'C:/Users/jtloo/Documents/VCE/VAL_GBIF_Wordpress-Staging/species_datasets/mva_species_list/';
subDir = '';
fileName = 'mva_species_list';

var inpFileExt = '.tsv';
var inpFileName = fileName + inpFileExt;
const inpFileDelim = "\t";

const dateTime = moment().format('YYYYMMDD-HHmmsss');
var logFileName = 'log_' + dateTime + '_' + fileName + '.log';
var errFileName = 'err_' + dateTime + '_' + fileName + '.log';

var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`);

var headRow = true;
var rowCount = 0; //count records available
var notCount = 0; //count records NOT found in val_species
var fndCount = 0; //count records found in val_species
var insCount = 0; //count records inserted
var misCount = 0; //count records where key != nubKey
var errCount = 0; //count errors

const sourceTable = 'new_species'; //template table to create new table from
const speciesTable = 'mval_species'; //new table name
const errorTable = 'species_err';

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

connect(dbConfig.pg) //this produces an error message on failure
  .then(async msg => {
    pgUtil.copyTableEmpty(sourceTable, speciesTable)
    .then(res => {
      setColumns()
        .then(col => {
          getSpeciesFile(dataDir+subDir+inpFileName)
            .then(async src => {
              log(`Input file rowCount:${src.rowCount}`)
              log(`Header Row: ${src.header}`);
              rowCount = src.rows.length;
              if (rowCount) {log(`First Row: ${JSON.stringify(src.rows[0])}`);}
              for (var i=0; i<src.rows.length; i++) {
              //for (var i=0; i<1; i++) {
                await getValTaxon(src.rows[i], i)
                  .then(async val => {
                    if (val.rowCount) {
                      fndCount++
                      //log(`${val.idx} | getValTaxon FOUND taxonKey:${val.gbif.taxonKey} | ${val.gbif.scientificName}`, logStream, true);
                    } else {
                      notCount++;
                      await getGbifTaxon(val.gbif.taxonKey, val.idx)
                        .then(async gbf => {
                          await insertGbifTaxon(speciesTable, gbf, gbf.idx)
                            .then(ins => {
                              insCount++;
                              log(`${ins.idx} | insertGbifTaxon SUCCESS | key:${ins.val.key} | nubKey:${ins.val.nubKey} | Inserted:${insCount}`, logStream, true);
                            })
                            .catch(err => {
                              errCount++;
                              if (errorTable) {
                                insertError(err)
                                  .then(res => {
                                    console.log('ERROR inserted into', errorTable);
                                  })
                                  .catch(err => {
                                    console.log('ERROR inserting ERROR into table', errorTable, err.message);
                                  })
                                }
                              log(`${err.idx} | insertGbifTaxon ERROR | key:${err.gbif.key} | nubKey:${err.gbif.nubKey} | error:${err.message}`, logStream, true);
                              //console.log(`${err.idx} | insertGbifTaxon ERROR | GBIF taxon input:`, err.gbif);
                              //log(`${err.idx} | insertGbifTaxon ERROR |  key:${err.val.key} | nubKey:${err.val.nubKey} | error:${err.message}`, errStream, false);
                              //log(`${err.idx} | insertGbifTaxon ERROR | gbif result object: ${JSON.stringify(err.gbif)}`, errStream, false);
                            });
                        })
                        .catch(err => {
                          log(`${err.idx} | getGbifTaxon ERROR | key:${ins.val.key} | nubKey:${ins.val.nubKey} | error:${err.message}`, logStream, true);
                        })
                    }
                  })
                  .catch(err => {
                    log(`getValTaxon ERROR | ${JSON.stringify(err)}`, logStream, true);
                  });
                }
            })
            .catch(err => {
              log(`getSpeciesFile ERROR | ${JSON.stringify(err)}`, logStream, true);
            });
        })
        .catch(err => {
          log(`setColumns ERROR | ${JSON.stringify(err)}`, logStream, true);
        })
    })
    .catch(err => {
      log(`copyTableEmpty ERROR | ${err.message} | ${err.code}`, logStream, true);
    })
}) //end connect - no need to catch error, the call handles that

function setColumns() {
  return pgUtil.setColumns(speciesTable) //new method stores table column arrays in db_pg_util by tableName
    .then(ret => {
      pgUtil.setColumns(errorTable);
    })
}

/*
Parse the input file into an array of objects for processing.
*/
async function getSpeciesFile(inpFileName) {
  try {
    return await csvFileToArrayOfObjects(inpFileName, inpFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
  Query SELECT Species table where table.key == gbif taxonKey
*/
async function getValTaxon(gbif, idx) {

  var sql_select = `select * from ${speciesTable} where "key"=$1`;

  return new Promise((resolve, reject) => {
    query(sql_select, [gbif.taxonKey])
      .then(res => {
        res.gbif = gbif;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        console.log('getValTaxon ERROR', err, 'on', sql_select, 'with', gbif.taxonKey);
        err.gbif = gbif;
        err.idx = idx;
        reject(err);
      })
  })
}

/*
  Get a GBIF species with a GBIF species key (key, usageKey, ...taxonKey)
  eg. http://api.gbif.org/v1/species/4334
*/
function getGbifTaxon(key, idx) {

  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        log(`getGbifTaxon|err.code: ${err.code}`, logStream);
        err.key = key;
        err.idx = idx;
        reject(err);
      } else {
        log(`${idx} | getGbifTaxon(${key}) | ${res.statusCode} | gbifKey:${body.key?key:undefined}`, logStream, true);
        //body.key = key;
        body.idx = idx;
        resolve(body);
      }
    });
  });
}

/*
Insert the fixed-up val database object. On error, return the val object for
downstream processing.

Inputs:
  tableName - val species table name
  gbif - must be the return object from api.gbif.org/v1/species/{key}
  idx - index of iteration to display in messaging
*/
async function insertGbifTaxon(tableName, gbif, idx) {
return new Promise(async (resolve, reject) => {

  try { //wrap whole thing in trap to deal with random errors during development...

    var val = gbifToValSpecies(gbif); //translate gbif api values to val columns
    var qryColumns = pgUtil.parseColumns(val, 1, [], [], [], tableName);
    var sqlInsert = `insert into ${tableName} (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;

    await query(sqlInsert, qryColumns.values)
      .then(res => {
        res.gbif = gbif;
        res.val = val;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        err.val = val;
        err.idx = idx;
        reject(err);
      })

    } catch(err) {
      console.log('insertGbifTaxon try/catch ERROR', err);
      err.gbif = gbif; err.val = val; err.idx = idx;
      reject(err);
    }
  }) //end promise
}

/*
  err object has this structure from failed call to insertGbifTaxon:
  err.gbif
  err.val
  err.idx
*/
function insertError(obj) {

  var ins = {};
  ins.key = obj.gbif.key;
  ins.nubKey = obj.gbif.nubKey;
  ins.taxonId = obj.val.taxonId;
  ins.scientificName = obj.gbif.scientificName;
  ins.canonicalName = obj.gbif.canonicalName;
  ins.errorCode = obj.code;
  ins.errorMessage = obj.message;
  ins.errorObj = obj;
  ins.inpFilePath = dataDir+subDir+inpFileName;
  ins.inpFileLine = obj.idx;

  var qryColumns = pgUtil.parseColumns(ins, 1, [], [], [], errorTable);
  var sqlInsert = `insert into ${errorTable} (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;

  return new Promise((resolve, reject) => {
    query(sqlInsert, qryColumns.values)
      .then(res => {
        res.idx = obj.idx;
        resolve(res);
      })
      .catch(err => {
        err.idx = obj.idx;
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|missing:${notCount}|found:${fndCount}|inserted:${insCount}|key-mismatch:${misCount}|errors:${errCount}`, logStream, true);
  log(`Log file name: ${dataDir+subDir+logFileName}`, logStream, true);
  log(`Error file name: ${dataDir+subDir+errFileName}`, logStream, true);
}
