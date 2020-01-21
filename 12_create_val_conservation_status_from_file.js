/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 12_create_val_conservation_status_from_file.js

  Purpose: Populate the val_conservation_status table from a file provided by
  VT F & W.

  Specifics:

*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const log = require('./97_utilities').log;
const parseSciName = require('./97_utilities').parseSciName;
const csvFileTo2DArray = require('./99_parse_csv_to_array').csvFileTo2DArray;
var staticColumns = [];
var staticTypes = [];

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var baseName = 'Vermont_Conservation_Status';
//var baseName = 'Vermont_Conservation_Missing';
var subDir = baseName + '/';
var inpFileName = baseName + '.csv';
var outFileName = 'val_' + inpFileName;
var logFileName = 'log_' + moment().format('YYYYMMDD-HHMMSS') + '_' + inpFileName;
var errFileName = 'err_' + inpFileName;

const inputFileDelim = ",";
const outputFileDelim = ",";

var rowCount = 0; //count records available
var outCount = 0; //count records completed so far
var insCount = 0; //count records inserted
var updCount = 0; //count records updated
var errCount = 0; //count record errors

logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});

log(`config paths: ${JSON.stringify(paths)}`, logStream);
log(`output file name ${logFileName}`, logStream);
log(`error file name ${errFileName}`, logStream);

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|output:${outCount}|errors:${errCount}`);
}

getColumns("val_conservation_status")
  .then(async res => {
    await getConservationStatusFile(dataDir+subDir+inpFileName)
      .then(async src => {
        log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
        rowCount = src.rows.length;
        for (var i=0; i<src.rows.length; i++) {
          //await selectValSpecies(src.rows[i])
          selectValSpecies(src.rows[i])
            .then(async val => {
              //await insertValConservation(val.src, val.rows[0])
              insertValConservation(val.src, val.rows[0])
                .then(res => {
                  insCount++;
                  const msg = `SUCCESS: insertValConservation | ${res.val.taxonId} | ${res.val.scientificName} | ${res.src.stateRank} | ${res.src.stateList}`;
                  log(msg, logStream);
                })
                .catch(async err => {
                  const msg = `ERROR: insertValConservation | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.scientificName:undefined} | ${err.val?err.src.stateRank:undefined} | error:${err.message}`;
                  log(msg, logStream);
                  //await updateValConservation(err.src, err.val)
                  updateValConservation(err.src, err.val)
                    .then(res => {
                      updCount++;
                      const msg = `SUCCESS: updateValConservation | ${res.val.taxonId} | ${res.val.scientificName} | ${res.src.stateRank} | ${res.src.stateList}`;
                      log(msg, logStream);
                    })
                    .catch(err => {
                      errCount++;
                      const msg = `ERROR: updateValConservation | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.scientificName:undefined} | ${err.val?err.src.stateRank:undefined} | error:${err.message}`;
                      log(msg, logStream);
                      log(`${err.src.scientificName},${err.src.stateRank},${err.src.stateList}`, errStream, true);
                    });
                })
          })
          .catch(err => {
            const msg = `ERROR: selectValSpecies | ${err.src?err.src.taxonId:undefined} | ${err.src?err.src.scientificName:undefined} | ${err.src?err.src.stateRank:undefined} | error:${err.message}`;
            log(msg, logStream);
            log(`${err.src.scientificName},${err.src.stateRank},${err.src.stateList}`, errStream, true);
          })
        }
      })
      .catch(err => {
        log(`ERROR: getConservationStatusFile | ${err.message}`, logStream);
      });
  })
  .catch(err => {
    log(`ERROR: getColumns | ${err.message}`, logStream);
  })

function getColumns(tableName) {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns(tableName, staticColumns, staticTypes);
}

/*
  Parse the input file into a 2D array for processing.

  NOTE: The input file MUST have a header row whose headings exactly match the column
  names in the table val_conservation_status.

*/
async function getConservationStatusFile(inpFileName) {
  try {
    return await csvFileTo2DArray(inpFileName, inputFileDelim, true, true);
  } catch(err) {
    throw(err);
  }
}

/*
src - object with one row of data from the source file

The incoming scientificName may have variety (var.) or subspecies (ssp.) to
indicate a third identifier.
*/
function selectValSpecies(src) {
    var sciName = parseSciName(src);

    /*
      We do not constrain valid taxon matches to taxonomicStatus='accepted' because
      conservation lists may reference synonyms or other non-canonical values. We
      do have a problem where val_species does not return a definitive result, in
      some cases.
    */
    var text = `
      SELECT "taxonId", "scientificName", "taxonomicStatus" from val_species
      WHERE "scientificName"='${sciName}'
      ORDER BY "taxonomicStatus" ASC
    `;

    return new Promise((resolve, reject) => {
      query(text)
        .then(res => {
          //console.log(`selectValSpecies result: Rows: ${res.rowCount}, First Row:`, res.rows[0]);
          if (res.rowCount == 1) {
            res.src = src;
            resolve(res);
          } else if (res.rowCount == 0) {
            var err = {message:`Species ${src.scientificName} NOT found.`, value:src.scientificName};
            err.src = src;
            reject(err);
          } else { //we now accept the top option as a valid taxon to attach conservation status to...
            //var err = {message:`Wrong number of rows: ${res.rowCount}`, value:res.rowCount};
            res.src = src;
            resolve(res);
          }
        })
        .catch(err => {
          err.src = src;
          reject(err);
        })
    })
}

/*
src - object with data from source file
val - object with data from val_species table for species in question
*/
async function insertValConservation(src, val) {

  log(`ATTEMPT: insertValConservation | VAL taxonId = ${val.taxonId} | scientificName = ${src.scientificName} | stateRank = ${src.stateRank}`, logStream);

  //val.SGCN = src.SGCN?src.SGCN:null;
  val.stateRank = src.stateRank?src.stateRank:null;
  val.globalRank = src.globalRank?src.globalRank:null;
  val.stateList = src.stateList?src.stateList:null;
  val.federalList = src.federalList?src.federalList:null;

  var queryColumns = await pgUtil.parseColumns(val, 1, [], staticColumns, staticTypes);
  var text = `insert into val_conservation_status (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;

  //log(`${text} | ${queryColumns.values}`);

  return new Promise((resolve, reject) => {
    query(text, queryColumns.values)
      .then(res => {
        res.src = src;
        res.val = val;
        resolve(res);
      })
      .catch(err => {
        err.src = src;
        err.val = val;
        reject(err);
      })
  })
}

/*
src - object with data from source file
val - object with data from val_species table for species in question
*/
async function updateValConservation(src, val) {

  log(`ATTEMPT: updateValConservation | VAL taxonId = ${val.taxonId} | scientificName = ${src.scientificName} | stateRank = ${src.stateRank}`, logStream);

  //val.SGCN = src.SGCN?src.SGCN:null;
  val.stateRank = src.stateRank?src.stateRank:null;
  val.globalRank = src.globalRank?src.globalRank:null;
  val.stateList = src.stateList?src.stateList:null;
  val.federalList = src.federalList?src.federalList:null;

  var queryColumns = await pgUtil.parseColumns(val, 2, [val.taxonId], staticColumns, staticTypes);
  var text = `update val_conservation_status set (${queryColumns.named}) = (${queryColumns.numbered}) where "taxonId"=$1`;

  //log(`${text} | ${queryColumns.values}`);

  return new Promise((resolve, reject) => {
    query(text, queryColumns.values)
      .then(res => {
        res.src = src;
        res.val = val;
        resolve(res);
      })
      .catch(err => {
        err.src = src;
        err.val = val;
        reject(err);
      })
  })
}
