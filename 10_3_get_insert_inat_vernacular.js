/*
  Project: VAL_Species

  File: 10_3_get_insert_inat_vernacular.js

  Purpose: Retrieve vernacular names from iNaturalist and insert into val_vernacular.

  Specifics:

  Query all taxa in val_species, get their iNat vernacular names, and insert
  into val_vernacular.

  iNat throttles their API, so I had to add a delay between requests. At first a 1-
  second delay was enough. Later, they throttled that, and I changed to 2 seconds.
  That only got us about 300 reqs before dropping, so it looks like their throttling
  has intelligence.

  Now that we've scanned our entire list, in the future if we use iNat at all for updates,
  we should limit requests only to new taxa or those missing data.

  As of September 2021, added getValMissing(), which still is a huge number of taxa.
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

const process = require('child_process');

const logsDir = "../logs_vernacular/";
const logFileName = 'get_insert_inat_vernacular_names_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
const errFileName = 'err_' + logFileName;
const outFileName = 'inat_found_sciName_commonName.csv';
const debug = true; //flag console output for debugging
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;
var offset = 0;
var limit = //25000; OPEN A VPN CONNECTION, AND CHANGE THIS TO A BIG NUMBER!!!!
var delay = 0; //in seconds (on Windows)
var where = 'true';//`"createdAt"::date > now()::date - interval '7 day'`;

logStream = fs.createWriteStream(`${logsDir}/${logFileName}`, {flags: 'w'});
errStream = fs.createWriteStream(`${logsDir}/${errFileName}`, {flags: 'w'});
outStream = fs.createWriteStream(`${logsDir}/${outFileName}`, {flags: 'a'});

log(`config paths: ${JSON.stringify(paths)}`, logStream);
log(`log file: ${logsDir}${logFileName}`, logStream, true);
log(`err file: ${logsDir}${errFileName}`, logStream, true);
log(`out file: ${logsDir}${outFileName}`, logStream, true);

getColumns()
  .then(res => {
    //getValTaxa()
    getValMissing()
      .then(async res => {
        log(`${res.rowCount} val_species taxa | First row: ${JSON.stringify(res.rows[0])}`, logStream, true);
        for (var i=0; i<res.rowCount; i++) {
          log(`COUNT | ${offset+i}`, logStream, true);
          process.execSync(`sleep ${delay}`);
          await getInatVernacularNames(res.rows[i]) //pass entire row...
            .then(async res => {
              for (var j=0; j<res.results.length; j++) { //inat api syntax - 'results' not 'rows'...
                if (res.results[j].name == res.val.scientificName //exact match results only
                  && res.results[j].rank.toLowerCase() == res.val.taxonRank.toLowerCase() //match rank
                  && res.results[j].preferred_common_name) { //non-null vernacular
                  await insertInatValVernacular(res.results[j], res.val)
                    .then(res => {
                      insCount++;
                      const msg = `SUCCESS: insertInatValVernacular | ${res.val.taxonId} | ${res.val.scientificName} | ${res.val.vernacularName}`;
                      log(msg, logStream, true); //just echo successes
                    })
                    .catch(err => {
                      errCount++;
                      const msg = `ERROR: insertInatValVernacular | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.scientificName:undefined} | ${err.val?err.val.vernacularName:undefined} | error:${err.message}`;
                      log(msg, logStream, debug);
                    })
                }
              } //end for-loop
            })
            .catch(err => {
              log(`ERROR: getInatVernacularNames | ${err.val.taxonId} | ${err.val.scientificName} | ${err.message}`, logStream, debug);
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
  text = `select s."taxonId", s."scientificName", s."taxonRank"
          from val_species s
          where ${where}
          offset ${offset}
          limit ${limit}`;

  return await query(text);
}

/*
Get VAL taxa having no vernacular name in val_vernacular.
*/
async function getValMissing() {
  var text = '';
  text = `select s."taxonId", s."scientificName", s."taxonRank"
          from val_species s
          left join val_vernacular v on s."taxonId"=v."taxonId"
          where v."taxonId" is null
          and "taxonRank" IN ('species','subspecies','variety')
          offset ${offset}
          limit ${limit}`;

  return await query(text);
}

function getInatVernacularNames(val) {

  var parms = {
    url: `https://api.inaturalist.org/v1/taxa?q=${val.scientificName}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.val = val;
        reject(err);
      } else {
        if (body) {
          log(`RESULT: getInatVernacularNames(${val.scientificName} | ${res.statusCode} | count: ${body.results?body.results.length:null}`, logStream, debug);
          if (body.results) {
            body.results.forEach((obj, idx) => {
              if (obj.preferred_common_name) {
                log(`FOUND matched_term: ${obj.matched_term} WITH name: ${obj.name} AND preferred_common_name: ${obj.preferred_common_name}`, logStream, true);
                log(`${val.taxonId},${val.scientificName},|,${obj.matched_term},${obj.name},${obj.rank},${obj.preferred_common_name}`,outStream,true);
              }
            });
          }
          //console.log(body);
          body.val = val;
          resolve(body);
        } else {
          var err = {message:`${val?val.taxonId:''} NOT Found.`, val:val};
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
function insertInatValVernacular(inat, val) {
  return new Promise((resolve, reject) => {
    try {
      log(`ATTEMPT: insertInatValVernacular | iNat taxonKey = ${inat.taxon_id} | scientificName = ${val.scientificName} | vernacularName = ${inat.preferred_common_name}`, logStream, true);

      val.vernacularName = inat.preferred_common_name; //translate inat api values to val columns
      val.source = "iNaturalist taxa API";
      val.language = 'en';
      val.preferred = 't'; //iNat only provides preferred common name

      var queryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
      var text = `insert into val_vernacular (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
    } catch(err) {
      log(`ERROR WITHIN insertInatValVernacular | ${err}`);
      reject({err:err, inat:inat, val:val})
    }

    query(text, queryColumns.values)
      .then(res => {
        res.inat = inat;
        res.val = val;
        resolve(res);
      })
      .catch(err => {
        err.inat = inat;
        err.val = val;
        reject(err);
      })
  })
}
