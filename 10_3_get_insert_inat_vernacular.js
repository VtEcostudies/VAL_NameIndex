/*
  Project: VAL_Species

  File: 10_3_get_insert_inat_vernacular.js

  Purpose: Retrieve vernacular names from iNaturalist and insert into ${vernacularTable}.

  Specifics:

  Query taxa in ${speciesTable}, for ranks in ${ranksToQuery}, get their GBIF vernacular 
  names from the iNat API, and insert into ${vernacularTable}.

  iNat throttles their API, so I had to add a delay between requests. At first a 1-
  second delay was enough. Later, they throttled that, and I changed to 2 seconds.
  That only got us about 300 reqs before dropping, so it looks like their throttling
  has intelligence.

  However - we learned that we can get blacklisted by iNat. Use a VPN and throttling is
  not an issue.

  Now that we've scanned our entire list, in the future if we use iNat at all for updates,
  we should limit requests only to new taxa or those missing data.

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
const logFileName = 'get_insert_inat_vernacular_names_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
const errFileName = 'err_' + logFileName;
const outFileName = 'inat_found_sciName_commonName.csv';
const debug = true; //flag console output for debugging
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;
var offset = 0;
var limit = 10; //25000; OPEN A VPN CONNECTION, AND CHANGE THIS TO A BIG NUMBER!!!!
var delay = 0; //in seconds (on Windows)
var where = 'true';//`"createdAt"::date > now()::date - interval '7 day'`;

logStream = fs.createWriteStream(`${logsDir}/${logFileName}`, {flags: 'w'});
errStream = fs.createWriteStream(`${logsDir}/${errFileName}`, {flags: 'w'});
outStream = fs.createWriteStream(`${logsDir}/${outFileName}`, {flags: 'a'});

const speciesTable = 'new_species';
const vernacularTable = 'new_vernacular';

log(`config paths: ${JSON.stringify(paths)}`, logStream);
log(`log file: ${logsDir}${logFileName}`, logStream, true);
log(`err file: ${logsDir}${errFileName}`, logStream, true);
log(`out file: ${logsDir}${outFileName}`, logStream, true);

db.connect(dbConfig.pg) //this produces an error message on failure
  .then(msg => {
  setColumns()
    .then(res => {
      //getValTaxa()
      getValMissing()
        .then(async res => {
          log(`${res.rowCount} ${speciesTable} taxa | First row: ${JSON.stringify(res.rows[0])}`, logStream, true);
          for (var i=0; i<res.rowCount; i++) {
            log(`COUNT | ${offset+i}`, logStream, true);
            process.execSync(`sleep ${delay}`);
            await getInatVernacularNames(res.rows[i], offset+i) //pass entire row...
              .then(async res => {
                for (var j=0; j<res.results.length; j++) { //inat api syntax - 'results' not 'rows'...
                  if (res.results[j].name == res.val.canonicalName //exact match results only
                    && res.results[j].rank.toLowerCase() == res.val.taxonRank.toLowerCase() //match rank
                    && res.results[j].preferred_common_name) { //non-null vernacular
                    await insertInatValVernacular(res.results[j], res.val)
                      .then(res => {
                        insCount++;
                        const msg = `\t SUCCESS(${j+1}): insertInatValVernacular | ${res.val.taxonId} | ${res.val.canonicalName} | ${res.val.vernacularName}`;
                        log(msg, logStream, true); //just echo successes
                      })
                      .catch(err => {
                        errCount++;
                        const msg = `\t ERROR(${j+1}): insertInatValVernacular | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.canonicalName:undefined} | ${err.val?err.val.vernacularName:undefined} | error:${err.message}`;
                        log(msg, logStream, debug);
                      })
                  } else {
                    var msg = `\t ERROR(${j+1}): insertInatValVernacular`;
                    if (res.results[j].name != res.val.canonicalName) {
                      msg += `\n\t | iNat name (${res.results[j].name}) != VAL name (${res.val.canonicalName})`;
                    }
                    if (res.results[j].rank.toLowerCase() != res.val.taxonRank.toLowerCase()) {
                      msg += `\n\t | iNat rank (${res.results[j].rank.toUpperCase()}) != VAL rank (${res.val.taxonRank})`;
                    }
                    if (!res.results[j].preferred_common_name) {
                      msg += `\n\t | preferred_common_name is empty`;
                    }
                    log(msg, logStream, true);
                  }
                } //end for-loop
              })
              .catch(err => {
                log(`ERROR: getInatVernacularNames | ${err.val.taxonId} | ${err.val.canonicalName} | ${err.message}`, logStream, debug);
              })
          }
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
async function getValTaxa() {
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
          and LOWER("taxonRank") IN ('species','subspecies','variety')
          offset ${offset}
          limit ${limit}`;

  return await db.query(text);
}

function getInatVernacularNames(val, idx) {

  var parms = {
    url: `https://api.inaturalist.org/v1/taxa?q=${val.canonicalName}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.val = val;
        reject(err);
      } else {
        if (body) {
          log(`\t RESULT(${idx}): getInatVernacularNames(${val.canonicalName}) | ${res.statusCode} | count: ${body.results?body.results.length:null} | ${parms.url}`, logStream, debug);
          if (body.results) {
            body.results.forEach((obj, jdx) => {
              if (obj.preferred_common_name) {
                log(`\t FOUND(${idx}): getInatVernacularNames(${val.canonicalName}|matched_term:${obj.matched_term}|name:${obj.name}|preferred_common_name:${obj.preferred_common_name}|iconic_taxon_name:${obj.iconic_taxon_name}`, logStream, true);
                log(`${val.taxonId},${val.canonicalName}|matched_term:${obj.matched_term},name:${obj.name},rank:${obj.rank},preferred_common_name${obj.preferred_common_name}`,outStream,false);
              } else {
                //log(`NOT FOUND(${idx}): getInatVernacularNames(${val.canonicalName}|matched_term:${obj.matched_term}|name:${obj.name}|iconic_taxon_name:${obj.iconic_taxon_name}`, logStream, true);
                //console.dir(obj);
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
      log(`ATTEMPT: insertInatValVernacular | iNat taxonKey = ${inat.taxon_id} | canonicalName = ${val.canonicalName} | vernacularName = ${inat.preferred_common_name}`, logStream, true);

      val.vernacularName = inat.preferred_common_name; //translate inat api values to val columns
      val.source = "iNaturalist taxa API";
      val.language = 'en';
      val.preferred = 't'; //iNat only provides preferred common name

      var qryColumns = pgUtil.parseColumns(val, 1, [], [], [], vernacularTable);
      var text = `insert into ${vernacularTable} (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;
    } catch(err) {
      log(`ERROR WITHIN insertInatValVernacular | ${err}`);
      reject({err:err, inat:inat, val:val})
    }

    db.query(text, qryColumns.values)
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
