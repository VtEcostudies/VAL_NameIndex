/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 12_create_val_conservation_status_from_file.js

  Purpose: Populate the val_conservation_status table from files provided by
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
var baseName = 'Vermont_Conservation_Missing';
var baseName = 'Vermont_Conservation_SGCN';
var subDir = baseName + '/';
var inpFileName = baseName + '.csv';
var outFileName = 'val_' + inpFileName;
var logFileName = 'log_' + moment().format('YYYYMMDD-HHMMSSS') + '_' + inpFileName;
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
log(`scientificName,stateRank,stateList,matchType,taxonomicRank,taxonomicStatus`, errStream);

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
              //NOTE: we are selecting the 1st value from the VAL SELECT... questionnable.
              insertUpdateValConservation(val.src, val.rows[0]);
          })
          .catch(err => {
            const msg = `ERROR: selectValSpecies | ${err.src?err.src.scientificName:undefined} | ${err.src?err.src.stateRank:undefined} | error:${err.message}`;
            log(msg, logStream);
            matchGbifSpecies(err.src) //use GBIF match API to look for a 'corrected' taxon name (A.K.A. Fuzzy Match)
              .then(async gbf => { //handle exact or fuzzy match
                insertUpdateValConservation(gbf.src, gbf);
              })
              .catch(err => { //errors and NOT found handled here
                log(`${err.src.scientificName},${err.src.stateRank},${err.src.stateList},${err.matchType},${err.rank},${err.status}`, errStream, true);
              })
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

async function insertUpdateValConservation(src, val) {
  //await insertValConservation(val.src, val.rows[0])
  insertValConservation(src, val)
    .then(res => {
      insCount++;
      const msg = `SUCCESS: insertValConservation | ${res.val.taxonId} | ${res.val.scientificName} | ${res.src.stateRank} | ${res.src.stateList}`;
      log(msg, logStream);
    })
    .catch(async err => {
      const msg = `ERROR: insertValConservation | ${err.val?err.val.scientificName:undefined} | ${err.val?err.src.stateRank:undefined} | error:${err.message}`;
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
          const msg = `ERROR: updateValConservation | ${err.val?err.val.scientificName:undefined} | ${err.val?err.src.stateRank:undefined} | error:${err.message}`;
          log(msg, logStream);
          log(`${err.src.scientificName},${err.src.stateRank},${err.src.stateList}`, errStream, true);
        });
    })

}

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

  if (src.SGCN) val.SGCN = true;
  if (src.stateRank) val.stateRank = src.stateRank;
  if (src.globalRank) val.globalRank = src.globalRank;
  if (src.stateList) val.stateList = src.stateList;
  if (src.federalList) val.federalList = src.federalList;

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

  //Avoid over-writing existing data: don't add the column to the update if it's not present.
  if (src.SGCN) val.SGCN = true;
  if (src.stateRank) val.stateRank = src.stateRank;
  if (src.globalRank) val.globalRank = src.globalRank;
  if (src.stateList) val.stateList = src.stateList;
  if (src.federalList) val.federalList = src.federalList;

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

async function deleteValConservation(taxonId) {
  var text = `delete from val_conservation_status where "taxonId"=$1`;

  return new Promise((resolve, reject) => {
    query(text, [`'${taxonId}'`])
      .then(res => {
        log(`deleteValConservation ${taxonId} SUCCESS`, logStream, true);
        resolve(res);
      })
      .catch(err => {
        log(`deleteValConservation ${taxonId} ERROR: ${err.message}`, logStream, true);
        reject(err);
      })
  })

}

/*
Search for a species on the GBIF API using the fuzzy match api endpoint.

This API endpoint returns a single match with a confidence metric if a match
is found.

If a species match is not found, it may return the GENUS for the request. This
might indicate an error in the incoming taxa, or it might indicate an unrecognized
synonym, or something else.

Fields returned from this endpoint are different from the raw /species output.

We trasp errors in gbifAcceptedToVal where we compare incoming scientificName to
GBIF canonicalName.
*/
function matchGbifSpecies(src) {
  var name = src.scientificName.trim();

  var parms = {
    url: `http://api.gbif.org/v1/species/match?name=${name}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        log(`matchGbifSpecies | err.code: ${err.code}`, errStream);
        err.src = src;
        reject(err);
      } else {
        log(`matchGbifSpecies(${src.scientificName}) | ${res.statusCode} | ${body.usageKey?1:0} results found | ${body.matchType} | ${body.rank} | ${body.status}`, logStream, true);
        body.src = src; //attach incoming source row-object to returned object for downstream use
        if (!body.usageKey || body.matchType == 'HIGHERRANK' || body.matchType == 'NONE') {
          body.message = `matchGbifSpecies | ${src.scientificName} NOT found.`;
          if (body.matchType == 'HIGHERRANK') {
            //deleteValConservation(body.usageKey);
          }
          reject(body);
        } else {
          resolve(body);
        }
      }
    });
  });
}
