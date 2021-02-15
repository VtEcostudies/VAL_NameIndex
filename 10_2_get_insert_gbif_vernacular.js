/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 10_get_insert_gbif_vernacular.js

  Purpose: Retrieve vernacular names from GBIF and insert into val_vernacular.

  Specifics:

  Query all taxa in val_species, get their GBIF vernacular names, and insert
  into val_vernacular.

  Query the GBIF species vernacular API for a list of values for each taxonId in
  val_species.
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
var logFileName = 'get_insert_vernacular_names_' + moment().format('YYYYMMDD-HHMMSSS') + '.txt';
var debug = true; //flag console output for debugging
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;
var offset = 0;
var limit = 25000;
var where = 'true';//`"createdAt"::date > now()::date - interval '30 day'`;

logStream = fs.createWriteStream(`${dataDir}/${logFileName}`, {flags: 'w'});

log(`config paths: ${JSON.stringify(paths)}`, logStream);
log(`output file name ${logFileName}`, logStream);

getColumns()
  .then(res => {
    getValTaxa()
      .then(async res => {
        log(`${res.rowCount} val_species taxa | First row: ${JSON.stringify(res.rows[0])}`, logStream, true);
        for (var i=0; i<res.rowCount; i++) {
          log(`COUNT | ${offset+i}`, logStream, true);
          await getGbifVernacularNames(res.rows[i]) //use taxonId - a column returned from SELECT query
            .then(async res => {
              for (var j=0; j<res.results.length; j++) { //gbif api syntax - 'results' not 'rows'...
                if (res.results[j].language == 'eng') { //} && res.results[j].preferred) {
                  await insertValVernacular(res.results[j], res.val)
                    .then(res => {
                      insCount++;
                      const msg = `SUCCESS: insertValVernacular | ${res.val.taxonId} | ${res.val.scientificName} | ${res.val.vernacularName}`;
                      log(msg, logStream, true); //just echo successes
                    })
                    .catch(err => {
                      errCount++;
                      const msg = `ERROR: insertValVernacular | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.scientificName:undefined} | ${err.val?err.val.vernacularName:undefined} | error:${err.message}`;
                      log(msg, logStream, debug);
                    })
                }
              } //end for-loop
            })
            .catch(err => {
              log(`ERROR: getGbifVernacularNames | ${err.val.taxonId} | ${err.val.scientificName} | ${err.message}`, logStream, debug);
            })
        }
      })
      .catch(err => {
        log(`ERROR: getValTaxa | ${err.message}`, logStream, debug);
      });
  })
  .catch(err => {
    log(`ERROR: getColumns | ${err.message}`, logStream);
  })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_vernacular", staticColumns);
}

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValTaxa() {
  var text = '';
  text = `select s."taxonId", s."scientificName"
          from val_species s
          where ${where}
          offset ${offset}
          limit ${limit}`;

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
        if (body) {
          log(`RESULT: getGbifVernacularNames(${val.taxonId}) | ${val.scientificName} | ${res.statusCode} | count: ${body?body.results.length:null}`, logStream, debug);
          body.val = val;
          resolve(body);
        } else {
          var err = {message:`${val.taxonId} NOT Found.`, val:val};
          reject(err)
        }
      }
    });
  });
}

/*
Vernacular Names are sometimes a list of values, comma-separated. Parse those here into multiple inserts.

To-do: split multiple vernacular names into separate inserts.

NOTE: this is currently handled after the fact in the DB with function vernacular_split_names().
*/
async function insertValVernacular(gbif, val) {

  try {
    await log(`ATTEMPT: insertValVernacular | GBIF taxonKey = ${gbif.taxonKey} | scientificName = ${val.scientificName} | vernacularName = ${gbif.vernacularName}`, logStream);

    val.vernacularName = gbif.vernacularName; //translate gbif api values to val columns
    val.source = gbif.source;
    val.language = gbif.language;
    val.preferred = gbif.preferred; //newly added on 2020-11-13

    var queryColumns = await pgUtil.parseColumns(val, 1, [], staticColumns);
    var text = `insert into val_vernacular (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
  } catch(err) {
    log(`ERROR WITHIN insertValVernacular | ${err}`);
  }

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
