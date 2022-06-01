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
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const gbifToValDirect = require('./98_gbif_to_val_columns').gbifToValDirect;
const log = require('./97_utilities').log;
var staticColumns = [];

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
var nubCount = 0; //inserts having taxonKey == nubkey
var notCount = 0; //inserts having taxonKey != nubkey
var nonCount = 0; //inserts without nubkey
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
log(`INSERT file path: ${dataDir}${subDir}${updFileName}`, logStream, true);

getColumns()
  .then(res => {
    getValMissing()
      .then(async res => {
        rowCount = res.rows.length;
        log(`${res.rowCount} missing gbif Keys.`, logStream, true);
        for (var i=0; i<res.rowCount; i++) {
        //for (var i=0; i<10; i++) {
          log(`--------------------------------------------------------------------------------`, logStream, true);
          log(`row ${i+1}: ${JSON.stringify(res.rows[i])}`, logStream, true);
          await getGbifSpecies(res.rows[i]) //2 columns returned from getValMissing: taxonKey, column
            .then(async res => {
              if (!res.gbif) { //missingKey not found in GBIF
                console.log('GBIF RESULT NOT FOUND');
                log(JSON.stringify(res), errStream, true);
              } else if (res.gbif.key == res.gbif.nubKey) {
                console.log('GBIF RESULT FOUND AND KEY == NUBKEY');
                await updateSpeciesFromOccMissing(res.gbif)
                  .then(res => {
                    nubCount++;
                    log(`updateSpeciesFromOccMissing SUCCESS | taxonKey:${res.gbif.taxonKey}`, logStream, true);
                    log(JSON.stringify(res.gbif), updStream);
                  })
                  .catch(err => {
                    errCount++;
                    log(`updateSpeciesFromOccMissing ERROR ${errCount} | gbifKey:${err.gbif.key} | error:${err.message}`, logStream, true);
                    log(JSON.stringify(err), errStream, true);
                  });
              }
              else if (res.gbif.nubkey) { //we got a gbif result with a nubkey, but gbif.key != gbif.nubkey
                console.log('GBIF RESULT FOUND AND KEY != NUBKEY');
                //log(JSON.stringify(res), errStream, true);
                await updateSpeciesFromOccMissing(res.gbif)
                  .then(res => {
                    notCount++;
                    log(`updateSpeciesFromOccMissing SUCCESS | taxonKey:${res.gbif.taxonKey}`, logStream, true);
                    log(JSON.stringify(res.gbif), updStream);
                  })
                  .catch(err => {
                    errCount++;
                    log(`updateSpeciesFromOccMissing ERROR ${errCount} | gbifKey:${err.gbif.key} | error:${err.message}`, logStream, true);
                    log(JSON.stringify(err), errStream, true);
                  });
              }
              else {  //we got a gbif result, but there is no gbif.nubkey
                console.log('GBIF RESULT FOUND BUT NO NUBKEY');
                //log(JSON.stringify(res), errStream, true);
                await updateSpeciesFromOccMissing(res.gbif)
                  .then(res2 => {
                    nonCount++;
                    log(`updateSpeciesFromOccMissing SUCCESS | taxonKey:${res2.gbif.taxonKey}`, logStream, true);
                    log(JSON.stringify(res2.gbif), updStream);
                  })
                  .catch(err2 => {
                    errCount++;
                    log(`updateSpeciesFromOccMissing ERROR ${errCount} | gbifKey:${err2.gbif.key} | error:${err2.message}`, logStream, true);
                    log(JSON.stringify(err2), errStream, true);
                  });
              }
            })
            .catch(err => {
              errCount++;
              log(`getGbifSpecies (for missingKey) ERROR ${errCount} | gbifKey:${err.key} | ${err.message}`, logStream, true);
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

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns(table_name, staticColumns);
}

/*
Query missing secondary keys in species-from-occurrences table.
*/
async function getValMissing() {
  var text = '';

  text = `
  SELECT DISTINCT ON ("taxonKey") --1253 -> 1144
  "missingColumn", "taxonKey"
  FROM (
  	SELECT "taxonKey", 'parentKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "parentKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM')
  UNION
  	SELECT "taxonKey", 'acceptedTaxonKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "acceptedTaxonKey" IS NULL
  		AND "taxonKey" != 0
  UNION
  	SELECT "taxonKey", 'kingdomKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "kingdomKey" IS NULL
  UNION
  	SELECT "taxonKey", 'phylumKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "phylumKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM')
  UNION
  	SELECT "taxonKey", 'classKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "classKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM')
  UNION
  	SELECT "taxonKey", 'orderKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "orderKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS')
  UNION
  	SELECT "taxonKey", 'familyKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "familyKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER')
  UNION
  	SELECT "taxonKey", 'genusKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "genusKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER', 'FAMILY')
  UNION
  	SELECT "taxonKey", 'speciesKey' AS "missingColumn" FROM val_species_from_occs
  		WHERE "speciesKey" IS NULL
  		AND UPPER("taxonRank") NOT IN ('KINGDOM', 'PHYLUM', 'CLASS', 'ORDER', 'FAMILY', 'GENUS')
  ) agg
`;

  return await query(text);
}

/*
  Get GBIF species for taxonKey.
  Return
  - results or error
*/
async function getGbifSpecies(fix) {
  const key = fix.taxonKey;
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  console.log('getGbifSpecies |', key);

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        reject({"gbif":null, "fix":fix, "err":err});
      } else {
        if (body && body.key) {
          log(`getGbifSpecies(${key}) | ${res.statusCode} | gbif_key: ${body.key} | gbif_nubKey: ${body.nubKey}`, logStream, true);
          resolve({"gbif":body, "fix":fix});
        } else {
          var err = {message:`${key} NOT Found. | sourceKey: http://api.gbif.org/v1/species/${key}`};
          log(`getGbifSpecies ERROR ${errCount} | ${err.message}`, logStream, true);
          log(`${key} | ${err.message}`, errStream, true);
          resolve({"gbif":null, "fix":fix, "err":err});
        }
      }
    });
  });
}

function gbifToDbColums(gbif) {
  if (gbif.rank) gbif.taxonRank = gbif.rank;
  gbif.taxonRank =   gbif.taxonRank.toUpperCase();
  gbif.taxonKey = gbif.key;
  gbif.acceptedScientificName = gbif.accepted;
  gbif.acceptedTaxonKey = gbif.acceptedKey;
  if ('ACCEPTED' == gbif.taxonomicStatus.toUpperCase()) {
    gbif.acceptedScientificName = gbif.scientificName;
    gbif.acceptedTaxonKey = gbif.taxonKey;
  }
  return gbif;
}

/*
  update a row of gbif species data with values from GBIF species API:

    http://api.gbif.org/v1/species/${key}

*/
function updateSpeciesFromOccMissing(gbif) {
  log(`updateSpeciesFromOccMissing | taxonKey = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  //translate gbif api values to gbif db colums
  //our column names here derive from the GBIF occurrence download, which has some
  //different names than their own species API columns names...
  gbif = gbifToDbColums(gbif);

  var q_c = pgUtil.parseColumns(gbif, 2, [gbif.taxonKey], staticColumns);
  var sql = `update ${table_name} set (${q_c.named}) = (${q_c.numbered}) where "taxonKey"=$1`;
  var arg = q_c.values;

  log(`updateSpeciesFromOccMissing Query | ${sql} | ${arg}`, logStream, true);

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
  log(`total:${rowCount}|updated == nubkey:${nubCount}|updated != nubkey:${notCount}|updated NO nubkey:${nonCount}|errors:${errCount}`, logStream, true);
  log(`total:${rowCount}|updated == nubkey:${nubCount}|updated != nubkey:${notCount}|updated NO nubkey:${nonCount}|errors:${errCount}`, updStream);
}
