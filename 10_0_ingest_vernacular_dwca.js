/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 10_0_ingest_vernacular_dwca.js

  Purpose: Ingest vernacular names included in a separate DwCA file from species taxa.

  Specifics:


*/

const fs = require('fs');
const readline = require('readline');
const Request = require('request');
const moment = require('moment');
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const csvFileTo2DArray = require('./99_parse_csv_to_array').csvFileTo2DArray;
const log = require('./97_utilities').log;

const inpFileDelim = ",";

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding inp data files - INCLUDING TRAILING SLASH
var baseName = paths.baseName; //moved this setting to 00_config.js, as it's used in downstream processing
var fileName = paths.fileName;
if (!fileName) {fileName = baseName + '_vernacularName';}

var dbInsert = 0;
var dbUpdate = 0;

var subDir = baseName + '/';

if (inpFileDelim == ",") {
  inpFileName = fileName + '.csv';
} else if (inpFileDelim == '\t') {
  inpFileName = fileName + '.txt';
}

var logFileName = 'log_' + moment().format('YYYYMMDD-HHmmsss') + '_' + inpFileName;
var errFileName = 'err_' + inpFileName;

var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
var errStream = null;
async function err(text, stream=null, consoul=false) {
  if (!errStream) {
    errStream = await fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
  }
  log(text, errStream, consoul);
}

var update = false;
var headRow = true;
var rowCount = 0; //count records available
var insCount = 0; //count records inserted
var updCount = 0; //count records updated
var notCount = 0; //count records not found
var xstCount = 0; //count val_vernacular name records already exisiting
var dupCount = 0; //count val_species query having more than one result for a taxon, ie. duplicate taxa
var misCount = 0; //count input records missing required fields
var errCount = 0; //count record errors

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

getColumns('val_vernacular')
  .then(col => {
    getInputFile(dataDir+subDir+inpFileName)
      .then(async inp => {
        log(`inp file rowCount:${inp.rowCount} | Header:${inp.header}`);
        rowCount = inp.rows.length;
        for (var i=0; i<inp.rows.length; i++) {
        //for (var i=0; i<1; i++) {
          getValTaxon(inp.rows[i], i)
            .then(res => {
              res.species.forEach((ele) => { //iterate over array of species returned
                insertValVernacular(ele, res.inp, res.idx)
                  .then(res => {})
                  .catch(err => {
                    if (update && err.code == 23505) { //duplicate value constraint
                      updateValVernacular(err.species, err.inp, err.idx)
                        .then(res => {})
                        .catch(err => {});
                    } else {
                      //errors handled within called functions
                    }
                  });
                })
            })
            .catch(err => {
              //missing taxon or ambiguous result or error
              log(err.message, logStream, true);
            });
        } //end for loop
      })
      .catch(err => {
        log(`getInputFile ERROR | ${JSON.stringify(err)}`, logStream, true);
      });
  })
  .catch(err => {
    log(`getColumns ERROR | ${JSON.stringify(err)}`, logStream, true);
  })

function getColumns(tableName) {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns(tableName, staticColumns);
}

/*
Parse the inp file into a 2D array for processing.
*/
async function getInputFile(inpFileName) {
  try {
    return await csvFileTo2DArray(inpFileName, inpFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
*/
function getValTaxon(inp, idx) {
  var sql = null;

  log(`getValTaxon | Input | taxonId:${inp.taxonId} | scientificName:${inp.scientificName}`);

  if (inp.taxonId) {
    sql = `select * from val_species where "taxonId"='${inp.taxonId}'`
  } else if (inp.scientificName) {
    sql = `select * from val_species where "scientificName"='${inp.scientificName}'`
  }

  return new Promise((resolve, reject) => {
    if (!inp.scientificName && !inp.taxonId) {
      reject({"message":`getValTaxon ERROR | scientificName or taxonId REQUIRED`, "inp":inp, "idx":idx, "species":null})
    } else {
      query(sql)
        .then(async res => {
          log(`getValTaxon | scientificName: ${inp.scientificName} | rows: ${res.rows.length} | taxonId: ${res.rows[0].taxonId}`, logStream, true);
          //if (res.rows.length == 1) {
          if (res.rows.length > 0) {
            if (res.rows.length > 1) {dupCount++;} //add this here to keep dupe stats
            resolve({"inp":inp, "idx":idx, "species":res.rows}); //now return an array of species found
          } else if (res.rows.length == 0) {
            notCount++;
            reject({"message":`ERROR: NOT FOUND.`, "inp":inp, "idx":idx, "species":null});
          } else { //this is now 'disabled' by allowing dupes above
            dupCount++;
            await err(`getValTaxon DUPLICATE ${dupCount} | scientificName: ${inp.scientificName} | taxonId: ${inp.taxonId} | vernacularName: ${inp.vernacularName}`, errStream, true);
            await err(`getValTaxon DUPLICATE ${dupCount} | DUPLICATE TAXA:`, errStream, true);
            res.rows.forEach(async (ele,idx,arr) => {
              await err(`getValTaxon DUPLICATE ${dupCount} | ${idx} | ${ele.taxonId} | ${ele.scientificName} | ${ele.taxonRank} | ${ele.taxonomicStatus}`, errStream, true);
            })
            reject({"message":`ERROR: found ${res.rows.length} rows.`, "inp":inp, "idx":idx, "species":null});
          }
        })
        .catch(async err => {
          errCount++;
          err.inp = inp;
          err.idx = idx;
          log(`getValTaxon ERROR | scientificName: ${inp.scientificName} | taxonId: ${inp.taxonId} | ${err.message}`, logStream, true);
          await err(`getValTaxon ERROR ${errCount} | scientificName: ${inp.scientificName} | taxonId: ${inp.taxonId} | vernacularName: ${inp.vernacularName}`, errStream, true);
          await err(`getValTaxon ERROR ${errCount} | code: ${err.code} | message: ${err.message}`, errStream, true);
          reject(err);
        });
    }
  })
}

/*
*/
function insertValVernacular(species, inp, idx) {

  inp.taxonId = species.taxonId;
  inp.source = inp.source ? inp.source : 'VTSR';
  inp.language = inp.language ? inp.language : 'en';

  var qryColumns = pgUtil.parseColumns(inp, 1, [], staticColumns);
  var sql_insert = `insert into val_vernacular (${qryColumns.named}) values (${qryColumns.numbered}) returning *`;

  log(`insertValVernacular | ${sql_insert}`, logStream, true);
  //console.dir(qryColumns)

  return new Promise((resolve, reject) => {
    if (!inp.taxonId || !inp.scientificName || !inp.vernacularName) {
      var msg = 'insertValVernacular ERROR | MISSING from Input:';
      if (!inp.taxonId) msg += ' taxonId';
      if (!inp.scientificName) msg += ' scientificName';
      if (!inp.vernacularName) msg += ' vernacularName';
      misCount++;
      log(msg, logStream, true);
      reject({"message":msg, "inp":inp, "idx":idx, "species":species});
    } else {
      query(sql_insert, qryColumns.values)
        .then(async res => {
          insCount++;
          res.species = species;
          res.inp = inp;
          res.idx = idx;
          log(`insertValVernacular SUCCESS | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, logStream, true);
          resolve(res);
        })
        .catch(async err => {
          if (err.code == 23505) {xstCount++;} //val_vernacular duplicate vernacularName
          else {
            errCount++;
            await err(`insertValVernacular ERROR ${errCount} | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, errStream, true);
            await err(`insertValVernacular ERROR ${errCount} | code: ${err.code} | message: ${err.message} | where: ${err.where}`, errStream, true);
          }
          err.species = species;
          err.inp = inp;
          err.idx = idx;
          //log(`insertValVernacular ERROR | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, logStream, true);
          //log(`insertValVernacular ERROR | code: ${err.code} | message: ${err.message} | where: ${err.where}`, logStream, true);
          reject(err);
        })
    }
  })
}

/*
*/
function updateValVernacular(species, inp, idx) {

  inp.taxonId = species.taxonId;

  var qryColumns = pgUtil.parseColumns(inp, 2, [inp.taxonId], staticColumns);
  var sql_update = `update val_vernacular set (${qryColumns.named}) = (${qryColumns.numbered}) where "taxonId"=$1`;

  return new Promise((resolve, reject) => {
    if (!inp.taxonId || !inp.scientificName || !inp.vernacularName) {
      var msg = 'updateValVernacular ERROR | MISSING from Input:';
      if (!inp.taxonId) msg += ' taxonId';
      if (!inp.scientificName) msg += ' scientificName';
      if (!inp.vernacularName) msg += ' vernacularName';
      misCount++;
      log(msg, logStream, true);
      reject({"message":msg, "inp":inp, "idx":idx, "species":species});
    } else {
      query(sql_update, qryColumns.values)
        .then(async res => {
          updCount++;
          res.species = species;
          res.inp = inp;
          res.idx = idx;
          log(`updateValVernacular SUCCESS | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, logStream, true);
          resolve(res);
        })
        .catch(async err => {
          errCount++;
          await err(`updateValVernacular ERROR ${errCount} | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, errStream, true);
          await err(`updateValVernacular ERROR ${errCount} | code: ${err.code} | message: ${err.message} | where: ${err.where}`, errStream, true);
          err.species = species;
          err.inp = inp;
          err.idx = idx;
          //log(`updateValVernacular ERROR | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, logStream, true);
          //log(`updateValVernacular ERROR | code: ${err.code} | message: ${err.message} | where: ${err.where}`, logStream, true);
          reject(err);
        })
    }
  })
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|taxa-not-found:${notCount}|existing:${xstCount}|species-dupes:${dupCount}|missing-input:${misCount}|errors:${errCount}`, logStream, true);
  err(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|taxa-not-found:${notCount}|existing:${xstCount}|species-dupes:${dupCount}|missing-input:${misCount}|errors:${errCount}`, errStream, true);
}
