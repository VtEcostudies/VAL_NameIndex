/*
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
const dbConfig = require('./db_config').dbConfig;
const db = require('./VAL_Utilities/db_postgres');
const pgUtil = require('./VAL_Utilities/db_pg_util');
const log = require('./VAL_Utilities/97_utilities').log;

var debug = true; //flag console output for debugging
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;
var offset = 0;
var limit = 35000;
var where = 'true';//`"createdAt"::date > now()::date - interval '30 day'`;

const dataDir = paths.dataDir; //path to directory holding inp data files - INCLUDING TRAILING SLASH
const subDir = '00_vernacular_names/';
const logsDir = "../logs_vernacular/";
const logFileName = 'get_insert_gbif_vernacular_names_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
const errFileName = 'err_' + logFileName;
const logStream = fs.createWriteStream(`${logsDir}${logFileName}`);
const errStream = fs.createWriteStream(`${logsDir}${errFileName}`);

const sourceTable = 'new_vernacular'; //template table to create new table from
const targetTable = 'new_vernacular'; //table to create
const speciesTable = 'new_species'; //new_species; //species table name

log(`log file: ${logsDir}${logFileName}`, logStream, true);
log(`err file: ${logsDir}${errFileName}`, logStream, true);

db.connect(dbConfig.pg) //this produces an error message on failure
  .then(msg => {
    createVernacularTable(sourceTable, targetTable, speciesTable)
      .finally(res => {
        setColumns()
        .then(res => {
          getValTaxa()
          //getValMissing()
            .then(async res => {
              log(`${res.rowCount} val_species taxa | First row: ${JSON.stringify(res.rows[0])}`, logStream, true);
              for (var i=0; i<res.rowCount; i++) {
                log(`COUNT | ${offset+i}/${res.rowCount}`, logStream, true);
                await getGbifVernacularNames(res.rows[i]) //use taxonId - a column returned from SELECT query
                  .then(async res => {
                    for (var j=0; j<res.results.length; j++) { //gbif api syntax - 'results' not 'rows'...
                      if (res.results[j].language == 'eng') { //} && res.results[j].preferred) {
                        await insertGbifVernacular(res.results[j], res.val)
                          .then(res => {
                            insCount++;
                            const msg = `SUCCESS: insertGbifVernacular | ${res.val.taxonId} | ${res.val.scientificName} | ${res.val.vernacularName}`;
                            log(msg, logStream, true); //just echo successes
                          })
                          .catch(err => { //error on insertGbifVernacular
                            errCount++;
                            const msg = `ERROR: insertGbifVernacular | ${err.val?err.val.taxonId:undefined} | ${err.val?err.val.scientificName:undefined} | ${err.val?err.val.vernacularName:undefined} | error:${err.message}`;
                            log(msg, logStream, debug);
                            log(`${err.val.taxonId}|${err.val.scientificName}\n`, errStream);
                          })
                      }
                    }
                  })
                  .catch(err => { //error on getGbifVernacularNames
                    log(`ERROR: getGbifVernacularNames | ${err.val.taxonId} | ${err.val.scientificName} | ${err.message}`, logStream, debug);
                    log(`${err.val.taxonId}|${err.val.scientificName}\n`, errStream);
                  })
              } //end for-loop
            })
            .catch(err => {
              log(`ERROR: getValTaxa | ${err.message}`, logStream, debug);
            });
        })
        .catch(err => {
          log(`ERROR: setColumns | ${err.message}`, logStream);
        })
    })
    .catch(err => {
      log(`createVernacularTable ERROR | ${err.message} | ${err.code}`, logStream, true);
    })
}) //end connect - no need to catch error, the call handles that

function setColumns() {
  return pgUtil.setColumns(targetTable) //new method stores table column arrays in db_pg_util by tableName
}

async function createVernacularTable(sourceName, targetName, speciesName) {

  const text = `
  CREATE TABLE IF NOT EXISTS ${targetName} AS SELECT * FROM ${sourceName} LIMIT 0;

  DROP TRIGGER IF EXISTS trigger_updated_at ON ${targetName};

  CREATE TRIGGER trigger_updated_at 
  BEFORE UPDATE
  ON ${targetName}
  FOR EACH ROW
  EXECUTE PROCEDURE set_updated_at();

  ALTER TABLE ${targetName} ALTER column "updatedAt" SET default now();
  ALTER TABLE ${targetName} ALTER column "createdAt" SET default now();
  ALTER TABLE ${targetName} ADD CONSTRAINT unique_taxonid_vernacularname_${targetName} UNIQUE("taxonId", "vernacularName");
  --ALTER TABLE ${targetName} ADD CONSTRAINT fk_taxon_id FOREIGN KEY ("taxonId") REFERENCES ${speciesName} ("taxonId");
  
  --DROP FUNCTION IF EXISTS vernacular_case();
  CREATE OR REPLACE FUNCTION vernacular_case()
      RETURNS trigger
      LANGUAGE 'plpgsql'
  AS $BODY$
  BEGIN
     NEW."vernacularName" = INITCAP(NEW."vernacularName");
     RETURN NEW;
  END;
  $BODY$;
  
  DROP TRIGGER IF EXISTS trigger_insert_vernacular_name ON ${targetName};
  CREATE TRIGGER trigger_insert_vernacular_name
      BEFORE INSERT 
      ON ${targetName}
      FOR EACH ROW
      EXECUTE FUNCTION vernacular_case();
  
  DROP TRIGGER IF EXISTS trigger_update_vernacular_name ON ${targetName};
  CREATE TRIGGER trigger_update_vernacular_name
      BEFORE UPDATE
      ON ${targetName}
      FOR EACH ROW
      EXECUTE FUNCTION vernacular_case();
  `;

  return await db.query(text);

}

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValTaxa() {
  var text = '';
  text = `select s."taxonId", s."scientificName", s."taxonRank"
          from ${speciesTable} s
          where ${where}
          offset ${offset}
          limit ${limit}`;

  return await db.query(text);
}

/*
Get VAL taxa having no vernacular name in val_vernacular.
*/
async function getValMissing() {
  var text = '';
  text = `select s."taxonId", s."scientificName", s."taxonRank"
          from ${speciesTable} s
          left join ${targetTable} v on s."taxonId"=v."taxonId"
          where v."taxonId" is null
          offset ${offset}
          limit ${limit}`;

  return await db.query(text);
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
          log(`RESULT: getGbifVernacularNames(${val.taxonId}) | ${val.scientificName} | ${res.statusCode} | count: ${body.results?body.results.length:0}`, logStream, debug);
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
async function insertGbifVernacular(gbif, val) {
  return new Promise((resolve, reject) => {
    try {
    log(`insertGbifVernacular | GBIF taxonKey = ${gbif.taxonKey} | scientificName = ${val.scientificName} | vernacularName = ${gbif.vernacularName}`, logStream, debug);

    val.vernacularName = gbif.vernacularName; //translate gbif api values to val columns
    val.source = gbif.source;
    val.language = gbif.language;
    val.preferred = gbif.preferred; //newly added on 2020-11-13

    var queryColumns = pgUtil.parseColumns(val, 1, [], [], [], targetTable);
    var text = `insert into ${targetTable} (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;

    db.query(text, queryColumns.values)
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
    } catch(err) {
      console.log('insertGbifVernacular try/catch ERROR', err);
      err.gbif = gbif; err.val = val; err.idx = idx;
      reject(err);
    }
  }) //end promise
}
