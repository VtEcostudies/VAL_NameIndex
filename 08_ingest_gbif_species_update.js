/*
  Project: VAL_species

  File: 08_ingest_gbif_species_update.js

  Purpose: Incorporate periodic updates to VT Species backbone, usually when we
  update VAL DE occurrences quarterly.

  Details:
    - bounce incoming taxonKey against val_species first
    - if key is found in val_species, ignore
    - if key is NOT found, use that key on GBIF taxon api to key full result
    - use that result to INSERT new taxon into val_species.
    - GBIF doesn't handle required fields the same way the ALA nameindexer does:
      - taxonomicStatus = DOUBTFUL we set acceptedNameUsage and acceptedNameUsageId as self-referential
        (NOTE: have not verified this is actually correct in ALA)
      - taxonRank = SUBSPECIES, VARIETY, ... we set infraspecificEpithet to the dangling (3rd) name token

  To-Do:
    - Determine if we'll ever want to use this for UPDATES. If we do, it's a rewrite.
*/

const fs = require('fs');
const readline = require('readline');
const Request = require('request');
const moment = require('moment');
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const csvFileToArrayOfObjects = require('./VAL_Utilities/99_parse_csv_to_array').csvFileToArrayOfObjects;
const gbifToValIngest = require('./VAL_Utilities/98_gbif_to_val_columns').gbifToValIngest;
const addCanonicalName = require('./VAL_Utilities/97_utilities').addCanonicalName;
const addTaxonRank = require('./VAL_Utilities/97_utilities').addTaxonRank;
const log = require('./VAL_Utilities/97_utilities').log;
const jsonToString = require('./VAL_Utilities/97_utilities').jsonToString;

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

const dataDir = paths.gbifDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
const subDir = 'gbif_species_2022_01_19/';
var fileName = 'gbif_species_update_wo_loc';
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
var notCount = 0; //count records NOT found in val_speies
var insCount = 0; //count records inserted

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

getColumns()
  .then(col => {
    getSpeciesFile(dataDir+subDir+inpFileName)
      .then(async src => {
        log(`Input file rowCount:${src.rowCount}`)
        log(`Header Row: ${src.header}`);
        rowCount = src.rows.length;
        if (rowCount) {log(`First Row: ${JSON.stringify(src.rows[0])}`);}
        for (var i=0; i<src.rows.length; i++) {
        //for (var i=0; i<1; i++) {
          getValTaxon(src.rows[i], i)
            .then(async val => {
              if (val.rowCount) {
                log(`getValTaxon FOUND ${val.gbif.scientificName}`, logStream, false);
              } else {
                notCount++;
                await getGbifTaxon(val.gbif.taxonKey, val.idx)
                  .then(async gbf => {
                    await insertValTaxon(gbf, gbf.idx)
                      .then(ins => {
                        insCount++;
                        log(`${ins.idx} | insertValTaxon SUCCESS | gbifId:${ins.val.taxonId} | Inserted:${insCount}`, logStream, true);
                      })
                      .catch(err => {
                        errCount++;
                        log(`${err.idx} | insertValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, logStream, true);
                        //console.log(`${err.idx} | insertValTaxon ERROR | GBIF taxon input:`, err.gbif);
                        log(`${err.idx} | insertValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, errStream, false);
                        log(`${err.idx} | insertValTaxon ERROR | gbif result object: ${JSON.stringify(err.gbif)}`, errStream, false);
                      });
                  })
                  .catch(err => {
                    log(`${err.idx} | getGbifTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, logStream, true);
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
    log(`getColumns ERROR | ${JSON.stringify(err)}`, logStream, true);
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
    return await csvFileToArrayOfObjects(inpFileName, inpFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
  Get a GBIF species with a GBIF species key (key, usageKey, ...taxonKey)
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
  Get VAL species from val_species table by gbif taxonKey
*/
async function getValTaxon(gbif, idx) {

  var sql_select = `select * from val_species where "taxonId"=$1`;

  return new Promise((resolve, reject) => {
    query(sql_select, [gbif.taxonKey])
      .then(res => {
        res.gbif = gbif;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        err.idx = idx;
        reject(err);
      })
  })
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
    val.gbifId = Number(gbif.key);
    val.taxonId = gbif.key;
    val.scientificName = gbif.canonicalName;
    val.scientificNameAuthorship = gbif.authorship;
    val.acceptedNameUsageId = gbif.acceptedKey;
    val.acceptedNameUsage = gbif.accepted;
    val.taxonRank = gbif.rank;
    val.taxonomicStatus = gbif.taxonomicStatus;
    val.taxonRemarks = gbif.remarks;
    val.parentNameUsageId = gbif.parentKey;
    val.vernacularName = gbif.vernacularName;
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
      GBIF does not provide 'accepted' or 'acceptedKey' for taxonomicStatus == 'doubtful', or
      for random taxa. The 'accepted' values do not appear to be reliable at this API endpoint.
      VAL DE requires 'acceptedNameUsage' and 'acceptedNameUsageId', so here we hack those in.
      These anomalies are easy to find in the db. As of 2022-01-27, there were 619 of these:
      select count(*) from val_species where LOWER("taxonomicStatus") like '%doubt%';
     */
    //if ('DOUBTFUL' == gbif.taxonomicStatus) {
    if (!gbif.acceptedKey || !gbif.accepted) {
      log(`insertValTaxon | MISSING 'acceptedKey' OR 'accepted' (REQUIRED) | taxononmicStatus: ${gbif.taxonomicStatus} | name: ${gbif.canonicalName} | key: ${gbif.key}`, logStream, true);
      val.acceptedNameUsage = gbif.scientificName;
      val.acceptedNameUsageId = gbif.key;
    }
    if ('SPECIES' == gbif.rank) { //pluck dangling token from end of canonicalName...
      const canon = gbif.canonicalName;
      const genus = gbif.genus;
      val.specificEpithet = (canon.replace(genus, '')).trim();
    }
    if (['SUBSPECIES','VARIETY'].includes(gbif.rank)) { //species is ALWAYS a 2-token name, so this works
      const canon = gbif.canonicalName;
      const species = gbif.species;
      val.infraspecificEpithet = (canon.replace(species, '')).trim();
    }

    var qryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
    var sql_insert = `insert into val_species (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;

    await query(sql_insert, qryColumns.values)
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
      reject({err:err, gbif: gbif, val:val, idx:idx})
    }
  }) //end promise
}

/*
Update the fixed-up val database object. On error, return the val object for
downstream processing.
*/
async function updateValTaxon(val, idx) {

  var qryColumns = pgUtil.parseColumns(val, 2, [val.gbifId], staticColumns);
  var sql_update = `update val_species set (${qryColumns.named}) = (${qryColumns.numbered}) where "gbifId"=$1`;

  return new Promise((resolve, reject) => {
    query(sql_update, qryColumns.values)
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

function displayStats() {
  log(`total:${rowCount}|missing:${notCount}|inserted:${insCount}|errors:${errCount}`, logStream, true);
  log(`Log file name: ${dataDir+subDir+logFileName}`, logStream, true);
  log(`Error file name: ${dataDir+subDir+errFileName}`, logStream, true);
}
