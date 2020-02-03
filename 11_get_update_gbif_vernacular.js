/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 11_get_update_gbif_vernacular.js

  Purpose: Retrieve vernacular names missing language and source from GBIF and
  update val_vernacular with those.

  Specifics:

  Query values in val_vernacular missing language, get their GBIF vernacular info,
  and update val_vernacular.

  Query the GBIF species vernacular API for data.
*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const log = require('./97_utilities').log;
var staticColumns = [];

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var logFileName = 'get_update_vernacular_names_' + moment().format('YYYYMMDD-HHMMSSS') + '.txt';
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;
var offset = 0;
var limit = 25000;

logStream = fs.createWriteStream(`${dataDir}/${logFileName}`, {flags: 'w'});

log(`config paths: ${JSON.stringify(paths)}`, logStream);
log(`output file name ${logFileName}`, logStream);

getColumns("val_vernacular")
  .then(res => {
    getValTaxa()
      .then(async res => {
        log(`${res.rowCount} val_species taxa | First row: ${res.rows[0]}`, logStream);
        for (var i=0; i<res.rowCount; i++) {
          log(`COUNT | ${offset+i}`);
          await getGbifVernacularNames(res.rows[i]) //use taxonId - a column returned from SELECT query
            .then(async res => {
              for (var j=0; j<res.results.length; j++) { //gbif api syntax - 'results' not 'rows'...
                if (res.results[j].language != 'eng') {
                  //log(`${res.val.taxonId} | ${res.val.scientificName} | skipping '${res.results[j].language}' language result`, logStream);
                  continue;
                }
                await updateValVernacular(res.results[j], res.val)
                  .then(res => {
                    insCount++;
                    const msg = `SUCCESS: updateValVernacular | ${res.val.taxonId} | ${res.val.scientificName} | ${res.val.vernacularName}`;
                    log(msg, logStream);
                  })
                  .catch(err => {
                    errCount++;
                    const msg = `ERROR: updateValVernacular | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.scientificName:undefined} | ${err.val?err.val.vernacularName:undefined} | error:${err.message}`;
                    log(msg, logStream);
                  })
              } //end for-loop
            })
            .catch(err => {
              log(`ERROR: getGbifVernacularNames | ${err.val.taxonId} | ${err.val.scientificName} | ${err.message}`, logStream);
            })
        }
      })
      .catch(err => {
        log(`ERROR: getValTaxa | ${err.message}`, logStream);
      });
  })
  .catch(err => {
    log(`ERROR: getColumns | ${err.message}`, logStream);
  })

function getColumns(tableName) {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns(tableName, staticColumns);
}

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValTaxa() {
  var text = '';
  text = `select "taxonId", "scientificName"
          from val_vernacular
          where language is null
          `;

  return await query(text);
}

function getGbifVernacularNames(val) {
  var parms = {
    url: `http://api.gbif.org/v1/species/${val.taxonId}/vernacularNames?limit=1000`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.val = val;
        reject(err);
      } else {
        log(`RESULT: getGbifVernacularNames(${val.taxonId}) | ${val.scientificName} | ${res.statusCode} | count: ${body.results.length}`, logStream);
        body.val = val;
        resolve(body);
      }
    });
  });
}

async function updateValVernacular(gbif, val) {

  log(`ATTEMPT: updateValVernacular | GBIF taxonKey = ${gbif.taxonKey} | scientificName = ${val.scientificName} | vernacularName = ${gbif.vernacularName}`, logStream);

  val.vernacularName = gbif.vernacularName; //translate gbif api values to val columns
  val.source = gbif.source;
  val.language = gbif.language;

  var queryColumns = pgUtil.parseColumns(val, 3, [val.taxonId, val.vernacularName], staticColumns);
  var text = `update val_vernacular set (${queryColumns.named}) = (${queryColumns.numbered}) where  "taxonId"=$1 and "vernacularName"=$2`;
  console.log(text, queryColumns.values);

  return new Promise((resolve, reject) => {
    query(text, queryColumns.values)
      .then(res => {
        res.gbif = gbif;
        res.val = val;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        err.val = val;
        reject(err);
      })
  })
}
