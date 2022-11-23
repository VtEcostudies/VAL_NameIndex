/*
  Project: VAL_species

  File: 00_ingest_gbif_species_new_table.js

  Purpose: Create a new VAL Vermont Species Checklist from scratch

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
const dbConfig = require('./db_config.json');
const connect = require('./VAL_Utilities/db_postgres').connect;
const query = require('./VAL_Utilities/db_postgres').query;
const pgUtil = require('./VAL_Utilities/db_pg_util');
const csvFileToArrayOfObjects = require('./VAL_Utilities/99_parse_csv_to_array').csvFileToArrayOfObjects;
const gbifToValIngest = require('./VAL_Utilities/98_gbif_to_val_columns').gbifToValIngest;
const parseCanonicalName = require('./VAL_Utilities/97_utilities').parseCanonicalName;
const addTaxonRank = require('./VAL_Utilities/97_utilities').addTaxonRank;
const log = require('./VAL_Utilities/97_utilities').log;
const jsonToString = require('./VAL_Utilities/97_utilities').jsonToString;

console.log(`config paths: ${JSON.stringify(paths)}`);

const dataDir = paths.gbifDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
const subDir = 'gbif_species_2022_11_01/';
var fileName = 'vt_gadm_gbif_species_list';
var fileExt = '.tsv';
var inpFileName = fileName + fileExt;

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

const speciesTable = 'new_species';
const errorTable = 'species_err';

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

connect(dbConfig.pg) //this produces an error message on failure
  .then(msg => {
    setColumns()
      .then(col => {
        getSpeciesFile(dataDir+subDir+inpFileName)
          .then(async src => {
            log(`Input file rowCount:${src.rowCount}`)
            log(`Header Row: ${src.header}`);
            rowCount = src.rows.length;
            if (rowCount) {log(`First Row: ${JSON.stringify(src.rows[0])}`);}
            for (var i=0; i<src.rows.length; i++) {
            //for (var i=2000; i<10000; i++) {
              await getValTaxon(src.rows[i], i)
                .then(async val => {
                  if (val.rowCount) {
                    fndCount++
                    //log(`${val.idx} | getValTaxon FOUND taxonKey:${val.gbif.taxonKey} | ${val.gbif.scientificName}`, logStream, true);
                  } else {
                    notCount++;
                    await getGbifTaxon(val.gbif.taxonKey, val.idx)
                      .then(async gbf => {
                        await insertValTaxon(gbf, gbf.idx)
                          .then(ins => {
                            insCount++;
                            log(`${ins.idx} | insertValTaxon SUCCESS | key:${ins.val.key} | nubKey:${ins.val.nubKey} | Inserted:${insCount}`, logStream, true);
                          })
                          .catch(err => {
                            errCount++;
                            insertError(err)
                              .then(res => {
                                console.log('ERROR inserted into', errorTable);
                              })
                              .catch(err => {
                                console.log('ERROR inserting ERROR into', errorTable, err);
                              })
                            log(`${err.idx} | insertValTaxon ERROR | key:${err.gbif.key} | nubKey:${err.gbif.nubKey} | error:${err.message}`, logStream, true);
                            //console.log(`${err.idx} | insertValTaxon ERROR | GBIF taxon input:`, err.gbif);
                            //log(`${err.idx} | insertValTaxon ERROR |  key:${err.val.key} | nubKey:${err.val.nubKey} | error:${err.message}`, errStream, false);
                            //log(`${err.idx} | insertValTaxon ERROR | gbif result object: ${JSON.stringify(err.gbif)}`, errStream, false);
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
  gbif - must be the return object from api.gbif.org/v1/species/{key}
*/
async function insertValTaxon(gbif, idx) {
return new Promise(async (resolve, reject) => {
  try { //wrap whole thing in trap to deal with random errors during development...
    var val = {};
    var nub = gbif.nubKey ? gbif.nubKey : gbif.key; //always use the nubKey if there is one
    if (gbif.nubKey && gbif.key != gbif.nubKey) {misCount++;}
    val.key = Number(gbif.key);
    val.nubKey = gbif.nubKey ? Number(gbif.nubKey) : 0;
    val.taxonId = nub;
    val.scientificName = gbif.scientificName;
    val.canonicalName = gbif.canonicalName;
    val.scientificNameAuthorship = gbif.authorship ? gbif.authorship : null;
    val.acceptedNameUsageId = gbif.acceptedKey;
    val.acceptedNameUsage = gbif.accepted;
    val.taxonRank = gbif.rank;
    val.taxonomicStatus = gbif.taxonomicStatus;
    val.taxonRemarks = gbif.remarks;
    val.parentNameUsageId = gbif.parentKey;
    val.parentNameUsage = gbif.parent ? gbif.parent: null;
    val.vernacularName = gbif.vernacularName ? gbif.vernacularName : null;
    val.kingdom = gbif.kingdom;
    val.kingdomId = gbif.kingdomKey;
    val.phylum = gbif.phylum;
    val.phylumId = gbif.phylumKey;
    val.class = gbif.class;
    val.classId = gbif.classKey;
    val.order = gbif.order;
    val.orderId = gbif.orderKey;
    val.family = gbif.family;
    val.familyId = gbif.familyKey;
    val.genus = gbif.genus;
    val.genusId = gbif.genusKey;
    val.species = gbif.species;
    val.speciesId = gbif.speciesKey;

    /*
      GBIF may not provide 'accepted' or 'acceptedKey' for taxonomicStatus == 'DOUBTFUL', or
      for random taxa. The 'accepted' values do not appear to be reliable at this API endpoint.
      VAL DE requires 'acceptedNameUsage' and 'acceptedNameUsageId', so here we hack those in.
      These anomalies are easy to find in the db. As of 2022-01-27, there were 619 of these:
      select count(*) from val_species where LOWER("taxonomicStatus") like '%doubt%';
      Also: GBIF does not provide accepted when key == nubKey, for obvious reasons, bolstering
      our decisionto make these self-referential (circular) when they're missing.
   */
    if (!gbif.acceptedKey || !gbif.accepted) {
      log(`insertValTaxon | MISSING 'acceptedKey' OR 'accepted' (REQUIRED) | taxononmicStatus: ${gbif.taxonomicStatus} | name: ${gbif.canonicalName} | key: ${gbif.key}`, logStream, true);
      val.acceptedNameUsage = gbif.scientificName;
      val.acceptedNameUsageId = nub; //not certain about using nub, here
    }
    if (!gbif.canonicalName) {
      let res = parseCanonicalName(val);
      val.canonicalName = res.canonicalName;
      if (!gbif.authorship) {
          val.scientificNameAuthorship = res.scientificNameAuthorship;
      }
    }
    if ('SPECIES' == gbif.rank) { //pluck dangling token from end of canonicalName by removing @genus...
      const canon = val.canonicalName;
      const genus = gbif.genus;
      log(`insertValTaxon | specificEpithet from canon and genus | canon: ${canon} | species: ${genus}`, logStream, true);
      val.specificEpithet = (canon.replace(genus, '')).trim();
    }
    if (['SUBSPECIES','VARIETY'].includes(gbif.rank)) { //species is ALWAYS a 2-token name, so this works by removing @species
      const canon = val.canonicalName;
      const species = gbif.species;
      log(`insertValTaxon | infraspecificEpithet from canon and species | canon: ${canon} | species: ${species}`, logStream, true);
      val.infraspecificEpithet = (canon.replace(species, '')).trim();
    }
    if ('kingdom' == gbif.rank.toLowerCase() && !gbif.parentKey) {
      val.parentNameUsageId = 0;
    }

    var qryColumns = pgUtil.parseColumns(val, 1, [], [], [], speciesTable);
    var sqlInsert = `insert into ${speciesTable} (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;

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
      console.log('insertValTaxon try/catch ERROR', err);
      err.gbif = gbif; err.val = val; err.idx = idx;
      reject(err);
    }
  }) //end promise
}

/*
  err object has this structure from failed call to insertVAlTaxon:
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
