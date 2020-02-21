/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 06_find_missing_gbif_id_add_to_val_db.js

  Purpose: Fix broken taxonomy tree in val_species table.

  Specifics:

  Query missing primary taxonIds in table val_species by querying all secondary
  and tertiary taxonIds in the database. Add missing taxa to the val_species table.

  The query find missing primary taxonIds for:

  kingdomId
  phylumId
  classId
  orderId
  familyId
  genusId
  speciesId
  acceptedNameUsage

  Query the GBIF species API for complete record data for each missing taxonId.
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
var baseName = 'Add_Missing_VAL_Taxa';
var subDir = `${baseName}/`; //put output into a sub-dir so we can easily find it
var logFileName = 'insert_missing_taxa_' + moment().format('YYYYMMDD-HHMMSSS') + '.txt';
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
var errFileName = 'err_' + baseName + '.txt';
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
var rowCount = 0;
var insCount = 0;
var errCount = 0;

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

log(`config paths: ${JSON.stringify(paths)}`, logStream, true);
log(`log file name: ${logFileName}`, logStream, true);
log(`err file name: ${errFileName}`, logStream, true);

getColumns()
  .then(res => {
    getValMissing()
      .then(res => {
        rowCount = res.rows.length;
        log(`${res.rowCount} missing gbif Ids. | First row: ${JSON.stringify(res.rows[0])}`, logStream, true);
        for (var i=0; i<res.rowCount; i++) {
          getGbifSpecies(res.rows[i].taxonId) //use taxonId - only column returned from SELECT query
            .then(res => {
              insertValTaxon(res)
                .then(res => {
                  insCount++;
                  log(`insertValTaxon SUCCESS | taxonId:${res.val.taxonId}`, logStream);
                })
                .catch(err => {
                  errCount++;
                  log(`insertValTaxon ERROR ${errCount} | gbifId:${err.gbif.key} | error:${err.message}`, logStream, true);
                  log(`${err.gbif.key} | ${err.message}`, errStream, true);
                })
            })
            .catch(err => {
              errCount++;
              log(`getGbifSpecies ERROR ${errCount} | gbifId:${err.key} | ${err.message}`, logStream, true);
              log(`${err.key} | ${err.message}`, errStream, true);
            })
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
  return pgUtil.getColumns("val_species", staticColumns);
}

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValMissing() {
  var text = '';

  text = `--retrieve a list of acceptedNameUsageId which lack a primary definition (no taxonId)
select cast(va."acceptedNameUsageId" as text) as "taxonId"
from val_species vs
right join val_species va
on vs."taxonId" = va."acceptedNameUsageId"
where vs."taxonId" is null and va."acceptedNameUsageId" != ''
union
--retrieve a list of parentNameUsageId which lack a primary definition (no taxonId)
select cast(va."parentNameUsageId" as text) as "taxonId"
from val_species vs
right join val_species va
on vs."taxonId" = va."parentNameUsageId"
where vs."taxonId" is null and va."parentNameUsageId" != ''
union
--retrieve a list of kingdomId which lack a primary definition (no taxonId)
select b."kingdomId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."kingdomId"
where a."taxonId" is null and b."kingdomId" is not null
union
--retrieve a list of phylumId which lack a primary definition (no taxonId)
select b."phylumId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."phylumId"
where a."taxonId" is null and b."phylumId" is not null
union
--retrieve a list of classId which lack a primary definition (no taxonId)
select b."classId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."classId"
where a."taxonId" is null and b."classId" is not null
union
--retrieve a list of orderId which lack a primary definition (no taxonId)
select b."orderId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."orderId"
where a."taxonId" is null and b."orderId" is not null
union
--retrieve a list of familyId which lack a primary definition (no taxonId)
select b."familyId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."familyId"
where a."taxonId" is null and b."familyId" is not null
union
--retrieve a list of genusId which lack a primary definition (no taxonId)
select b."genusId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."genusId"
where a."taxonId" is null and b."genusId" is not null
union
--retrieve a list of speciesId which lack a primary definition (no taxonId)
select b."speciesId" as "taxonId"
from val_species a
right join val_species b
on a."taxonId" = b."speciesId"
where a."taxonId" is null and b."speciesId" is not null
  `;

  return await query(text);
}

function getGbifSpecies(key) {
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.key = key;
        reject(err);
      } else {
        if (body && body.key) {
          log(`getGbifSpecies(${key}) | ${res.statusCode} | gbifId: ${body.key}`);
          body.key = key;
          resolve(body);
        } else {
          var err = {message:`${key} NOT Found`};
          err.key = key;
          reject(err);
        }
      }
    });
  });
}

async function insertValTaxon(gbif) {
  //translate gbif api values to val columns
  log(`insertValTaxon | taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  var val = gbifToValDirect(gbif);

  var queryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
  const text = `insert into val_species (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
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

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|errors:${errCount}`, logStream, true);
}
