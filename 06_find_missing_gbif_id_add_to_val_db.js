/*
  Project: VAL_Species

  File: 06_find_missing_gbif_id_add_to_val_db.js

  Purpose: Fix broken taxonomy tree in ${speciesTable} table.

  Specifics:

  Query missing primary taxonIds in table ${speciesTable} by querying all secondary
  and tertiary taxonIds in the database. Add missing taxa to the ${speciesTable} table.

  The query finds missing primary taxonIds for:

  parentNameUsageId
  acceptedNameUsageId

  kingdomId
  phylumId
  classId
  orderId
  familyId
  genusId
  speciesId

  Query the GBIF species API for complete record data for each missing taxonId.
*/

//https://nodejs.org/api/readline.html
const fs = require('fs');
const get = require('request').get;
const moment = require('moment');
const paths = require('./00_config').paths;
const dbConfig = require('./db_config').dbConfig;
const connect = require('./VAL_Utilities/db_postgres').connect;
const query = require('./VAL_Utilities/db_postgres').query;
const pgUtil = require('./VAL_Utilities/db_pg_util');
const gbifToValSpecies = require('./VAL_Utilities/98_gbif_to_val_columns').gbifToValSpecies;
const log = require('./VAL_Utilities/97_utilities').log;

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var baseName = '00_Add_Missing_VAL_Taxa';
var subDir = `${baseName}/`; //put output into a sub-dir so we can easily find it
var logFileName = 'insert_missing_taxa_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
//var logStream = null; //debugging
var errFileName = 'err_' + baseName + '.txt';
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
//var errStream = null; //debugging
var rowCount = 0;
var insCount = 0;
var updCount = 0;
var delCount = 0;
var errCount = 0;
const debug = true; //turn on/off extra console logging, etc.

const sourceTable = 'new_species'; //template table to create new table from
const speciesTable = 'mval_species'; //new_species; //new table name
const errorTable = 'species_err';

process.on('exit', function(code) {
  log(`--------------------------------------------------------------------------------`, logStream, true);
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

log(`config paths: ${JSON.stringify(paths)}`, logStream, true);
log(`log file name: ${logFileName}`, logStream, true);
log(`err file name: ${errFileName}`, logStream, true);

connect(dbConfig.pg) //this produces an error message on failure
  .then(msg => {
    pgUtil.copyTableEmpty(sourceTable, speciesTable)
      .then(res => {
        setColumns()
          .then(res => {
            getValMissing()
              .then(async res => {
                rowCount = res.rows.length;
                log(`${res.rowCount} missing gbif Ids.`, logStream, true);
                for (var i=0; i<res.rowCount; i++) {
                //for (var i=0; i<1; i++) {
                  log(`--------------------------------------------------------------------------------`, logStream, true);
                  log(`row ${i+1}/${rowCount}: ${JSON.stringify(res.rows[i])}`, logStream, true);
                  await getGbifSpecies(res.rows[i], res.rows[i].missingId) //3 columns returned from getValMissing: missingId, sourceId, columnName
                    .then(async res => {
                      /*
                        GBIF missingId taxon is no longer found. Search for the sourceId. If that's not found delete ours,
                        UNLESS the taxonId is VTSR*.
                      */
                      if (!res.gbif) { //missingId not found in GBIF
                        await getGbifSpecies(res.fix, res.fix.sourceId) //look for sourceId in GBIF
                          .then(async res => {
                            if (res.gbif) { //found GBIF result for sourceId
                              await updateValSource(res.gbif, res.fix) //update the sourceId record in case it has fixed values for missingId
                                .then(res => {
                                  updCount++;
                                  log(`updateValSource SUCCESS | taxonId:${res.gbif.key}`, logStream);
                                })
                                .catch(err => {
                                  errCount++;
                                  log(`updateValSource ERROR ${errCount} | gbifId:${err.gbif.key} | error:${err.message}`, logStream, true);
                                  log(`${err.gbif.key} | ${err.message}`, errStream, true);
                                });
                            } else if (res.fix.missingId.includes('VTSR') || res.fix.sourceId.includes('VTSR')){
                              errCount++;
                              var err = {message:'CANNOT DELETE VTSR* taxonIds!!!!!!!!!!!!!!!!!!!!!'};
                              log(`getGbifSpecies ERROR ${errCount} | taxonId:${res.fix.missingId} | error:${err.message}`, logStream, true);
                              log(`${res.fix.missingId} | ${err.message}`, errStream, true);
                            } else {
                              await deleteValSource(res.fix.sourceId)
                              .then(res => {
                                delCount++;
                                log(`deleteValSource SUCCESS | taxonId:${res.sourceId}`, logStream);
                              })
                              .catch(err => {
                                errCount++;
                                log(`deleteValSource ERROR ${errCount} | taxonId:${err.sourceId} | error:${err.message}`, logStream, true);
                                log(`${err.taxonId} | ${err.message}`, errStream, true);
                              });
                            }
                          })
                          .catch(err => {
                            errCount++;
                            log(`getGbifSpecies (for nubKey) ERROR ${errCount} | gbifId:${err.fix.missingId} | ${err.message}`, logStream, true);
                            log(`${err.fix.missingId} | ${err.message}`, errStream, true);
                          });
                      } else if (res.gbif.nubKey && res.gbif.key == res.gbif.nubKey) { //we got a gbif result, and gbif.key is good
                        await insertValMissing(res.gbif)
                          .then(res => {
                            insCount++;
                            log(`insertValMissing SUCCESS | taxonId:${res.val.taxonId}`, logStream, debug);
                          })
                          .catch(err => {
                            errCount++;
                            log(`insertValMissing ERROR ${errCount} | gbifId:${err.gbif.key} | error:${err.message}`, logStream, true);
                            log(`${err.gbif.key} | ${err.message}`, errStream, true);
                          });
                      } else if (res.gbif.nubKey && res.gbif.key != res.gbif.nubKey) { //we got a gbif missingId result with a nubKey, but its gbif.key != gbif.nubkey
                        /*
                          If key != nubKey in the found missingId child GBIF record, we didn't have a nubKey missingId in the sourceId record.
                          This cannot be resolved without altering the sourceId record's missingId key to be the nubKey.

                          Here we update the sourceId's record only and do not attempt to insert the missingId record. It may already exist, and it will
                          get caught on the next iteration.

                          This looked wrong to me. The intent had been to update the sourceId record with a good missingId nubKey. To do that, we need
                          to get the sourceId's GBIF record, but here we call getGbifSpecies with missingId's nubKey. We need to use sourceId.
                        */
                        errCount++;
                        log(`getGbifSpecies ERROR ${errCount} key != nubKey in missingId results | missingId: http://api.gbif.org/v1/species/${res.fix.missingId} | sourceId: http://api.gbif.org/v1/species/${res.fix.sourceId}`, logStream, true);
                        //await getGbifSpecies(res.fix, res.gbif.nubKey)
                        /*
                        await getGbifSpecies(res.fix, res.fix.sourceId) //I think this is correct
                          .then(async res => {
                            await updateValSource(res.gbif, res.fix)
                              .then(res => {
                                updCount++;
                                log(`updateValSource SUCCESS | VAL taxonId:${res.val.taxonId}`, logStream, debug);
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
                        */
                      } else if (res.gbif.key && !res.gbif.nubKey) { //we got a missingId result without a nubKey. Insert it anwyay - if we got to this point, there's no better option
                        log(`getGbifSpecies RESULT -  nubKey is EMPTY in missingId results | missingId: http://api.gbif.org/v1/species/${res.fix.missingId} | sourceId: http://api.gbif.org/v1/species/${res.fix.sourceId}`, logStream, true);
                        await insertValMissing(res.gbif)
                          .then(res => {
                            insCount++;
                            log(`insertValMissing SUCCESS | taxonId:${res.val.taxonId}`, logStream, debug);
                          })
                          .catch(err => {
                            errCount++;
                            log(`insertValMissing ERROR ${errCount} | gbifId:${err.gbif.key} | error:${err.message}`, logStream, true);
                            log(`${err.gbif.key} | ${err.message}`, errStream, true);
                          });
                      } else { //we got a missingId result with unknown problems.
                        errCount++;
                        log(`getGbifSpecies ERROR ${errCount} unknown problem in missingId results | missingId: http://api.gbif.org/v1/species/${res.fix.missingId} | sourceId: http://api.gbif.org/v1/species/${res.fix.sourceId}`, logStream, true);
                      }
                    })
                    .catch(err => {
                      errCount++;
                      log(`getGbifSpecies (for missingId) ERROR ${errCount} | gbifId:${err.fix.missingId} | ${err.message}`, logStream, true);
                      log(`${err.fix.missingId} | ${err.message}`, errStream, true);
                    });
                }
              })
              .catch(err => {
                log(`getValMissing ERROR | ${err.message}`, logStream, true);
              });
          })
          .catch(err => {
            log(`setColumns ERROR | ${JSON.stringify(err)}`, logStream, true);
          })
      })
      .catch(err => {
        log(`copyTableEmpty ERROR | ${err.message} | ${err.code}`, logStream, true);
      })
  }) //end connect - no need to catch error, the call handles that
      
function setColumns() {
  return pgUtil.setColumns(speciesTable) //new method stores table column arrays in db_pg_util by tableName
    .then(ret => {
      pgUtil.setColumns(errorTable);
    })
}

/*
NOTE: taxonId and acceptedNameUsageId are text, while gbifId is integer.
*/
async function getValMissing() {

const kingdom = `
--retrieve a list of kingdomId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."kingdomId" as "missingId", b."taxonRank", 'kingdom' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."kingdomId"
where a."taxonId" is null and b."kingdomId" is not null and b."kingdomId" != '0'`;
const phylym = `
--retrieve a list of phylumId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."phylumId" as "missingId", b."taxonRank", 'phylum' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."phylumId"
where a."taxonId" is null and b."phylumId" is not null and b."phylumId" != '0'`;
const classT = `
--retrieve a list of classId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."classId" as "missingId", b."taxonRank", 'class' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."classId"
where a."taxonId" is null and b."classId" is not null and b."classId" != '0'`;
const order = `
--retrieve a list of orderId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."orderId" as "missingId", b."taxonRank", 'order' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."orderId"
where a."taxonId" is null and b."orderId" is not null and b."orderId" != '0'`;
const family = `
--retrieve a list of familyId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."familyId" as "missingId", b."taxonRank", 'family' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."familyId"
where a."taxonId" is null and b."familyId" is not null and b."familyId" != '0'`;
const genus = `
--retrieve a list of genusId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."genusId" as "missingId", b."taxonRank", 'genus' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."genusId"
where a."taxonId" is null and b."genusId" is not null and b."genusId" != '0'`;
const species = `
--retrieve a list of speciesId which lack a primary definition (no taxonId)
select b."taxonId" as "sourceId", b."speciesId" as "missingId", b."taxonRank", 'species' as column
from ${speciesTable} a
right join ${speciesTable} b
on a."taxonId" = b."speciesId"
where a."taxonId" is null and b."speciesId" is not null and b."speciesId" != '0'`;
//For normalized species db, we just need parent. For complete dataset with synonomy, we also need accepted.
const parent = `
select distinct on (va."parentNameUsageId")
va."taxonId" as "sourceId", va."canonicalName" as "sourceName", 'parentNameUsage' as column, cast(va."parentNameUsageId" as text) as "missingId", va."parentNameUsage", va."taxonRank"
from ${speciesTable} vs
right join ${speciesTable} va
on vs."taxonId" = va."parentNameUsageId"
where vs."taxonId" is null and va."parentNameUsageId" != '' and va."parentNameUsageId" != '0'`;
const accept = `
select distinct on (va."acceptedNameUsageId")
va."taxonId" as "sourceId", va."canonicalName" as "sourceName", 'acceptedNameUsage' as column, cast(va."acceptedNameUsageId" as text) as "missingId", va."acceptedNameUsage", va."taxonRank"
from ${speciesTable} vs
right join ${speciesTable} va
on vs."taxonId" = va."acceptedNameUsageId"
where vs."taxonId" is null and va."acceptedNameUsageId" != '' and va."acceptedNameUsageId" != '0'`;

return await query(`${parent} union ${accept}`);
}

/*
  Get GBIF species for key. Key in this case is one of:
  - fix.missingId
  - gbif.nubkey from missingId results for a dead-end taxon
  - fix.sourceId
  Return
  - results or error
  - incoming fix object from getValMissing for downstream processing
  In Other Words, if there's a problem getting missingId, which we try to get on
  the first pass, we back up and try to reload the sourceId - which is the taxon
  in our DB that's pointing to missingId. The assumption, on such an error, is
  that sourceId itself is incorrect, and we need to update it (or DELETE it.)
*/
async function getGbifSpecies(fix, key) {
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };
  
  return new Promise((resolve, reject) => {
    get(parms, (err, res, body) => {
      if (err) {
        err.fix = fix;
        reject(err);
      } else {
        if (body && body.key) {
          log(`getGbifSpecies(${key}) | ${res.statusCode} | gbif_key: ${body.key} | gbif_nubKey: ${body.nubKey}`, logStream, true);
          resolve({"gbif":body, "fix":fix});
        } else {
          var err = {message:`${key} NOT Found. | missingId: http://api.gbif.org/v1/species/${fix.missingId} | sourceId: http://api.gbif.org/v1/species/${fix.sourceId}`};
          errCount++;
          log(`getGbifSpecies ERROR ${errCount} | ${err.message}`, logStream, true);
          log(`${key} | ${err.message}`, errStream, true);
          resolve({"gbif":null, "fix":fix, "err":err});
          //err.fix = fix; //return our incoming getValMissing object
          //reject(err);
        }
      }
    });
  });
}

async function insertValMissing(gbif, idx) {
  return new Promise((resolve, reject) => {
    try {

    log(`insertValMissing | taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

    //translate gbif api values to val columns
    var val = gbifToValSpecies(gbif);

    //console.log('insertValMissing', val);

    var queryColumns = pgUtil.parseColumns(val, 1, [], [], [], speciesTable);

    const text = `insert into ${speciesTable} (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
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
    } catch(err) {
      console.log('insertValMissing try/catch ERROR', err);
      err.gbif = gbif; err.val = val; err.idx = idx;
      reject(err);
    }
  }) //end promise
}

/*
  Here, we've determined that the sourceId had a bad value for missingId. Using a good value, update our sourceId
  record.
  
  NOTE that we set 2 values: fix.column (ie. taxonName) and fix.columnId (ie. taxonId) (except originally in the case of 
  parent, where we just set it's primary taxonId to itself. That was done because the original val_species did not have 
  a parentNameUsage column.) We have to set 2 columns to avoid a postgres error caused by using array-brackets around a 
  single value, like ['1234'].
*/
function updateValSource(gbif, fix) {
  log(`updateValSource | taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  //translate gbif api values to val columns even through we only use two of them for this missingId update
  var val = gbifToValSpecies(gbif);

  //if ('parentNameUsage' == fix.column) { //we don't need this anymore with 'new_species' table, with has a parentNameUsage column
  if (0) {
    var sql = `update ${speciesTable} set ("taxonId","parentNameUsageId") = ($2,$3) where "taxonId"=$1`;
    var arg = [fix.sourceId,fix.sourceId,val.parentNameUsageId];
  } else {
    var sql = `update ${speciesTable} set ("${fix.column}","${fix.column}Id") = ($2,$3) where "taxonId"=$1`;
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

/*
  ...can't delete from ${speciesTable} if val_vernacular has pointer. need to alter foreign key relation
  to 'ON DELETE CASCADE'.
*/
function deleteValSource(taxonId) {
  var sql = `delete from ${speciesTable} where "taxonId"=$1;`;
  var arg = [taxonId];

  log(`deleteValSource Query | ${sql} | ${arg}`, logStream, true);

  return new Promise((resolve, reject) => {
    query(sql,arg)
      .then(res => {
        res.taxonId = taxonId;
        resolve(res);
      })
      .catch(err => {
        err.taxonId = taxonId;
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|deleted:${delCount}|errors:${errCount}`, logStream, true);
}
