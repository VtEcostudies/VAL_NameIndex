/*
  Project: VAL_Species

  File: 10_3_get_insert_Gbif_vernacular.js

  Purpose: Retrieve vernacular names from GBIF and insert into ${vernacularTable}.

  Specifics:

  Query taxa in ${speciesTable}, for ranks in ${ranksToQuery}, get their GBIF vernacular 
  names from the GBIF API, and insert into ${vernacularTable}.

  Once we've scanned our entire list, we should limit requests to new taxa or those 
  missing data.

  As of September 2021, added getValMissing(), which still is a huge number of taxa.
*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const dbConfig = require('./db_config.json');
const db = require('./VAL_Utilities/db_postgres');
const pgUtil = require('./VAL_Utilities/db_pg_util');
const log = require('./VAL_Utilities/97_utilities').log;

const process = require('child_process');

const logsDir = "../logs_vernacular/";
const logFileName = 'get_insert_Gbif_vernacular_names_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
const errFileName = 'err_' + logFileName;
const outFileName = 'Gbif_found_sciName_commonName.csv';
const debug = true; //flag console output for debugging
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;
var offset = 0;
var limit = 10; //25000;
var where = 'true';//`"createdAt"::date > now()::date - interval '7 day'`;

logStream = fs.createWriteStream(`${logsDir}/${logFileName}`, {flags: 'w'});
errStream = fs.createWriteStream(`${logsDir}/${errFileName}`, {flags: 'w'});
outStream = fs.createWriteStream(`${logsDir}/${outFileName}`, {flags: 'a'});

const speciesTable = 'new_species';
const vernacularTable = 'new_vernacular';
const ranksToQuery = `'species','subspecies','variety'`;

log(`config paths: ${JSON.stringify(paths)}`, logStream);
log(`log file: ${logsDir}${logFileName}`, logStream, true);
log(`err file: ${logsDir}${errFileName}`, logStream, true);
log(`out file: ${logsDir}${outFileName}`, logStream, true);

db.connect(dbConfig.pg) //this produces an error message on failure
  .then(msg => {
    setColumns()
      .then(res => {
        //getValTaxaAll()
        getValMissing()
          .then(async res => {
            log(`${res.rowCount} ${speciesTable} taxa | First row: ${JSON.stringify(res.rows[0])}`, logStream, true);
            for (var i=0; i<res.rowCount; i++) {
              log(`COUNT | ${offset+i}`, logStream, true);
              await getGbifVernacularNames(res.rows[i]) //use taxonId - a column returned from SELECT query
                .then(async res => {
                  for (var j=0; j<res.results.length; j++) { //gbif api syntax - 'results' not 'rows'...
                    if (res.results[j].language == 'eng') { // && res.results[j].preferred)
                      await insertGbifVernacular(res.results[j], res.val)
                          .then(res => {
                              insCount++;
                              const msg = `\t SUCCESS(${j+1}): insertGbifValVernacular | ${res.val.taxonId} | ${res.val.canonicalName} | ${res.val.vernacularName}`;
                              log(msg, logStream, true); //just echo successes
                            })
                            .catch(err => {
                              errCount++;
                              const msg = `\t ERROR(${j+1}): insertGbifValVernacular | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.canonicalName:undefined} | ${err.val?err.val.vernacularName:undefined} | error:${err.message}`;
                              log(msg, logStream, debug);
                            })
                        } else {
                          var msg = `\t ERROR(${j+1}): insertGbifValVernacular | no english verncuularNames found.`;
                          log(msg, logStream, true);
                        }
                  } //end 'j' for-loop over N vernacular names for taxon
                })
                .catch(err => {
                  log(`ERROR: getGbifVernacularNames | ${err.val.taxonId} | ${err.val.canonicalName} | ${err.message}`, logStream, debug);
                })
            } //end 'i' for-loop over N VAL taxa
          })
          .catch(err => {
            log(`ERROR: getValMissing | ${err.message}`, logStream, debug);
          });
      })
      .catch(err => {
        log(`ERROR: setColumns | ${err.message}`, logStream);
      })
    }) //end connect - no need to catch error, the call handles that

  function setColumns() {
    return pgUtil.setColumns(vernacularTable) //new method stores table column arrays in db_pg_util by tableName
      .then(ret => {
        pgUtil.setColumns(speciesTable);
      })
  }

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValTaxaAll() {
  var text = '';
  text = `select s."taxonId", s."canonicalName", s."taxonRank"
          from ${speciesTable} s
          where ${where}
          offset ${offset}
          limit ${limit}`;

  return await db.query(text);
}

/*
Get VAL taxa having no vernacular name in ${vernacularTable}.
*/
async function getValMissing() {
  var text = '';
  text = `select s."taxonId", s."canonicalName", s."taxonRank"
          from ${speciesTable} s
          left join ${vernacularTable} v on s."taxonId"=v."taxonId"
          where v."taxonId" is null
          and LOWER("taxonRank") IN ${ranksToQuery}
          offset ${offset}
          limit ${limit}`;

  return await db.query(text);
}

function getGbifVernacularNames(val, idx) {

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
          log(`RESULT(${idx}): getGbifVernacularNames(${val.taxonId}) | ${val.scientificName} | ${res.statusCode} | count: ${body?body.results.length:0}`, logStream, debug);
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
function insertGbifValVernacular(Gbif, val) {
  return new Promise((resolve, reject) => {
    try {
      log(`ATTEMPT: insertGbifValVernacular | Gbif taxonKey = ${Gbif.taxon_id} | canonicalName = ${val.canonicalName} | vernacularName = ${Gbif.preferred_common_name}`, logStream, true);

      val.vernacularName = Gbif.preferred_common_name; //translate Gbif api values to val columns
      val.source = "Gbifuralist taxa API";
      val.language = 'en';
      val.preferred = 't'; //Gbif only provides preferred common name

      var qryColumns = pgUtil.parseColumns(val, 1, [], [], [], vernacularTable);
      var text = `insert into ${vernacularTable} (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;
    } catch(err) {
      log(`ERROR WITHIN insertGbifValVernacular | ${err}`);
      reject({err:err, Gbif:Gbif, val:val})
    }

    db.query(text, qryColumns.values)
      .then(res => {
        res.Gbif = Gbif;
        res.val = val;
        resolve(res);
      })
      .catch(err => {
        err.Gbif = Gbif;
        err.val = val;
        reject(err);
      })
  })
}
