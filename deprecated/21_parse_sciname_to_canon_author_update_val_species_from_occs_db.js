/*
  Project: VAL_Species

  File: 20_find_missing_gbif_key_update_val_species_from_occs_db.js

  Purpose: Fix broken taxonomy tree in ${table_name} table.

  Specifics:

  Query missing secondary taxonKeys in table ${table_name} by querying them.
  in the database. Update rows with missing keys from GBIF species API.

  The query finds missing secondary taxonKeys for:

  acceptedTaxonKey
  parentKey
  kingdomKey
  phylumKey
  classKey
  orderKey
  familyKey
  genusKey
  speciesKey

  Query the GBIF species API for complete record data for each missing taxonKey.
*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const db_conf = require('./db_config.json');
const connect = require('./VAL_Utilities/db_postgres').connect;
const query = require('./VAL_Utilities/db_postgres').query;
const pgUtil = require('./VAL_Utilities/db_pg_util');
const parseCanonAuthorFromScientificRank = require('./VAL_Utilities/98_gbif_to_val_columns').parseCanonAuthorFromScientificRank;
const log = require('./VAL_Utilities/97_utilities').log;
var staticColumns = [];

console.log(query);
console.log(pgUtil);
console.log(parseCanonAuthorFromScientificRank);

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var baseName = '00_Missing_Gbif_Species_From_Occs';
var subDir = `${baseName}/`; //put output into a sub-dir so we can easily find it
var logFileName = 'update_missing_taxa_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
var errFileName = 'err_' + baseName + '.txt';
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'a'});
var updFileName = 'upd_' + baseName + '.txt';
var updStream = fs.createWriteStream(`${dataDir}${subDir}${updFileName}`, {flags: 'a'});
var rowCount = 0;
var updCount = 0; //updates
var errCount = 0;

var table_name = 'val_species_from_occs';

process.on('exit', function(code) {
  log(`--------------------------------------------------------------------------------`, logStream, true);
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

log(`config paths: ${JSON.stringify(paths)}`, logStream, true);
log(`log file path: ${dataDir}${subDir}${logFileName}`, logStream, true);
log(`ERROR file path: ${dataDir}${subDir}${errFileName}`, logStream, true);
log(`UPDATE file path: ${dataDir}${subDir}${updFileName}`, logStream, true);

connect(db_conf.pg).then(res => {
  getColumns()
    .then(res => {
      getValMissing()
        .then(async res => {
          rowCount = res.rows.length;
          log(`${res.rowCount} missing canonicalName.`, logStream, true);
          for (var i=0; i<res.rowCount; i++) {
          //for (var i=0; i<1; i++) {
            log(`--------------------------------------------------------------------------------`, logStream, true);
            log(`row ${i+1}: ${JSON.stringify(res.rows[i])}`, logStream, true);
            await updateSpeciesFromOccCanonAuthor(res.rows[i])
              .then(res => {
                updCount++;
                log(`updateSpeciesFromOccCanonAuthor SUCCESS | taxonKey:${res.gbif.taxonKey}`, logStream, true);
                log(JSON.stringify(res.gbif), updStream);
              })
              .catch(err => {
                errCount++;
                log(`updateSpeciesFromOccCanonAuthor ERROR ${errCount} | gbifKey:${err.gbif.taxonKey} | error:${err.message}`, logStream, true);
                log(JSON.stringify(err), errStream, true);
              });
            }
        })
        .catch(err => {
          log(`getValMissing ERROR | ${err.message}`, logStream, true);
        });
    })
    .catch(err => {
      log(`getColumns ERROR | ${err.message}`, logStream, true);
    })
}).catch(err => {
  log(`db connect ERROR | ${err}`, logStream, true)
})

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns(table_name, staticColumns);
}

/*
Query missing canonicalName in species-from-occurrences table.
*/
async function getValMissing() {
  var text = `
  SELECT * FROM ${table_name} WHERE "canonicalName" IS NULL
  --AND "taxonRank" = 'FORM'
`;

  return await query(text);
}

/*
  Update rows without canonicalName using our parsing tool to split
  scientificName into canon and author.
*/
function updateSpeciesFromOccCanonAuthor(gbif) {
  log(`updateSpeciesFromOccCanonAuthor | taxonKey = ${gbif.taxonKey} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName} | authorship = ${gbif.authorship}`, logStream, true);

  var parsed = parseCanonAuthorFromScientificRank(gbif.scientificName, gbif.taxonRank)

  var sql = `update ${table_name} set ("canonicalName", "authorship") = ($2,$3) where "taxonKey"=$1`;
  var arg = [gbif.taxonKey, parsed.canon, parsed.author];

  log(`updateSpeciesFromOccCanonAuthor Query | ${sql} | ${arg}`, logStream, true);

  return new Promise((resolve, reject) => {
    query(sql,arg)
      .then(res => {
        res.gbif = gbif;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|updated:${updCount}|errors:${errCount}`, logStream, true);
  log(`total:${rowCount}|updated:${updCount}|errors:${errCount}`, updStream);
}
