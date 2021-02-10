/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 06_find_missing_gbif_id_add_to_val_db.js

  Purpose: Fix broken taxonomy tree in val_species table.

  Specifics:

  Query missing primary taxonIds in table val_species by querying all secondary
  and tertiary taxonIds in the database. Add missing taxa to the val_species table.

  The query finds missing primary taxonIds for:

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
//var logStream = null; //debugging
var errFileName = 'err_' + baseName + '.txt';
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
//var errStream = null; //debugging
var rowCount = 0;
var insCount = 0;
var updCount = 0;
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
      .then(async res => {
        rowCount = res.rows.length;
        log(`${res.rowCount} missing gbif Ids.`, logStream, true);
        for (var i=0; i<res.rowCount; i++) {
        //for (var i=0; i<1; i++) {
          log(`row ${i+1}: ${JSON.stringify(res.rows[i])}`, logStream, true);
          await getGbifSpecies(res.rows[i], res.rows[i].missingId) //3 columns returned from getValMissing: missingId, sourceId, column
            .then(async res => {
              //if key != nubKey in GBIF, we don't have a definitive value. This cannot be resolved
              //without altering the source record's taxon key to the nubKey. To do that, getValMissing
              //must be altered to return actual taxonId of the source record. The lookup ID will now
              //be called missingId in the query.
              if (!res.gbif.nubKey || res.gbif.key == res.gbif.nubKey) {
                await insertValMissing(res.gbif)
                  .then(res => {
                    insCount++;
                    log(`insertValMissing SUCCESS | taxonId:${res.val.taxonId}`, logStream);
                  })
                  .catch(err => {
                    errCount++;
                    log(`insertValMissing ERROR ${errCount} | gbifId:${err.gbif.key} | error:${err.message}`, logStream, true);
                    log(`${err.gbif.key} | ${err.message}`, errStream, true);
                  });
                } else {
                  await getGbifSpecies(res.fix, res.gbif.nubKey)
                    .then(async res => {
                      await updateValSource(res.gbif, res.fix)
                        .then(res => {
                          updCount++;
                          log(`updateValSource SUCCESS | taxonId:${res.gbif.key}`, logStream);
                        })
                        .catch(err => {
                          errCount++;
                          log(`updateValSource ERROR ${errCount} | gbifId:${err.gbif.key} | error:${err.message}`, logStream, true);
                          log(`${err.gbif.key} | ${err.message}`, errStream, true);
                        });
                    })
                    .catch(err => {
                      errCount++;
                      log(`getGbifSpecies (for nubKey) ERROR ${errCount} | gbifId:${err.fix.missingId} | ${err.message}`, logStream, true);
                      log(`${err.fix.missingId} | ${err.message}`, errStream, true);
                    });
                }
            })
            .catch(err => {
              errCount++;
              log(`getGbifSpecies (for missingId) ERROR ${errCount} | gbifId:${err.val.missingId} | ${err.message}`, logStream, true);
              log(`${err.val.missingId} | ${err.message}`, errStream, true);
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
  return pgUtil.getColumns("val_species", staticColumns);
}

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValMissing() {
  var text = '';

  text = `
  --retrieve a list of acceptedNameUsageId which lack a primary definition (no taxonId)
  select cast(va."acceptedNameUsageId" as text) as "missingId", va."taxonId" as "sourceId", 'acceptedNameUsage' as column
  from val_species vs
  right join val_species va
  on vs."taxonId" = va."acceptedNameUsageId"
  where vs."taxonId" is null and va."acceptedNameUsageId" != '' and va."acceptedNameUsageId" != '0'
  union
  --retrieve a list of parentNameUsageId which lack a primary definition (no taxonId)
  select cast(va."parentNameUsageId" as text) as "missingId", va."taxonId" as "sourceId", 'parentNameUsage' as column
  from val_species vs
  right join val_species va
  on vs."taxonId" = va."parentNameUsageId"
  where vs."taxonId" is null and va."parentNameUsageId" != '' and va."parentNameUsageId" != '0'
  union
  --retrieve a list of kingdomId which lack a primary definition (no taxonId)
  select b."kingdomId" as "missingId",  b."taxonId" as "sourceId", 'kingdom' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."kingdomId"
  where a."taxonId" is null and b."kingdomId" is not null and b."kingdomId" != '0'
  union
  --retrieve a list of phylumId which lack a primary definition (no taxonId)
  select b."phylumId" as "missingId",  b."taxonId" as "sourceId", 'phylum' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."phylumId"
  where a."taxonId" is null and b."phylumId" is not null and b."phylumId" != '0'
  union
  --retrieve a list of classId which lack a primary definition (no taxonId)
  select b."classId" as "missingId",  b."taxonId" as "sourceId", 'class' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."classId"
  where a."taxonId" is null and b."classId" is not null and b."classId" != '0'
  union
  --retrieve a list of orderId which lack a primary definition (no taxonId)
  select b."orderId" as "missingId",  b."taxonId" as "sourceId", 'order' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."orderId"
  where a."taxonId" is null and b."orderId" is not null and b."orderId" != '0'
  union
  --retrieve a list of familyId which lack a primary definition (no taxonId)
  select b."familyId" as "missingId",  b."taxonId" as "sourceId", 'familiy' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."familyId"
  where a."taxonId" is null and b."familyId" is not null and b."familyId" != '0'
  union
  --retrieve a list of genusId which lack a primary definition (no taxonId)
  select b."genusId" as "missingId",  b."taxonId" as "sourceId", 'genus' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."genusId"
  where a."taxonId" is null and b."genusId" is not null and b."genusId" != '0'
  union
  --retrieve a list of speciesId which lack a primary definition (no taxonId)
  select b."speciesId" as "missingId",  b."taxonId" as "sourceId", 'species' as column
  from val_species a
  right join val_species b
  on a."taxonId" = b."speciesId"
  where a."taxonId" is null and b."speciesId" is not null and b."speciesId" != '0'
  `;

  return await query(text);
}

/*
  Get GBIF species for key.
  Return
  - results or error
  - incoming val object from getValMissing
*/
async function getGbifSpecies(fix, key=null) {
  if (!key) {key = fix.missingId;}
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.fix = fix;
        reject(err);
      } else {
        if (body && body.key) {
          log(`getGbifSpecies(${key}) | ${res.statusCode} | gbif_key: ${body.key} | gbif_nubKey: ${body.nubKey}`, logStream, true);
          resolve({"gbif":body, "fix":fix});
        } else {
          var err = {message:`${key} NOT Found`};
          err.fix = fix; //return our incoming getValMissing object
          reject(err);
        }
      }
    });
  });
}

async function insertValMissing(gbif) {
  log(`insertValMissing | taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  //translate gbif api values to val columns
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

async function updateValSource(gbif, fix) {
  log(`updateValSource | taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  //translate gbif api values to val columns - but we only use two of them for this missingId update
  var val = gbifToValDirect(gbif);

  var queryColumns = pgUtil.parseColumns(val, 2, [val.gbifId], staticColumns);

  if ('parentNameUsage' == fix.column) {
    var sql = `update val_species set ("taxonId","parentNameUsageId") = ($2,$3) where "gbifId"=$1`;
    var arg = [fix.sourceId,fix.sourceId,val.parentNameUsageId];
  } else {
    var sql = `update val_species set ("${fix.column}","${fix.column}Id") = ($2,$3) where "gbifId"=$1`;
    var arg = [fix.sourceId,val[fix.column],val[`${fix.column}Id`]];
  }

  log(`updateValSource Query | ${sql} | ${arg}`, logStream, true);

  return new Promise((resolve, reject) => {
    query(sql,arg)
      .then(res => {
        res.gbif = gbif;
        res.val = val;
        res.fix = fix;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        err.val = val;
        err.misssing = fix;
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|errors:${errCount}`, logStream, true);
}
