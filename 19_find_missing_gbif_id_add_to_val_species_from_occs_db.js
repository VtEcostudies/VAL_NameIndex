/*
  Project: VAL_Species

  File: 06_find_missing_gbif_id_add_to_val_db.js

  Purpose: Fix broken taxonomy tree in ${table_name} table.

  Specifics:

  Query missing primary taxonKeys in table ${table_name} by querying all secondary
  and tertiary taxonKeys in the database. Add missing taxa to the ${table_name} table.

  The query finds missing primary taxonKeys for:

  kingdomKey
  phylumKey
  classKey
  orderKey
  familyKey
  genusKey
  speciesKey
  acceptedNameUsageKey
  parentNameUsageKey

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
var baseName = '00_Add_Missing_VAL_Taxa';
var subDir = `${baseName}/`; //put output into a sub-dir so we can easily find it
var logFileName = 'insert_missing_taxa_' + moment().format('YYYYMMDD-HHmmsss') + '.txt';
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
var errFileName = 'err_' + baseName + '.txt';
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'a'});
var insFileName = 'ins_' + baseName + '.txt';
var insStream = fs.createWriteStream(`${dataDir}${subDir}${insFileName}`, {flags: 'a'});
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
log(`INSERT file path: ${dataDir}${subDir}${insFileName}`, logStream, true);

getColumns()
  .then(res => {
    getValMissing()
      .then(async res => {
        rowCount = res.rows.length;
        log(`${res.rowCount} missing gbif Keys.`, logStream, true);
        for (var i=0; i<res.rowCount; i++) {
        //for (var i=0; i<20; i++) {
          log(`--------------------------------------------------------------------------------`, logStream, true);
          log(`row ${i+1}: ${JSON.stringify(res.rows[i])}`, logStream, true);
          await getGbifSpecies(res.rows[i], res.rows[i].missingKey) //3 columns returned from getValMissing: missingKey, sourceKey, column
            .then(async res => {
              if (!res.gbif) { //missingKey not found in GBIF
                console.log('GBIF RESULT NOT FOUND');
                log(JSON.stringify(res), errStream, true);
              } else if (res.gbif.key == res.gbif.nubKey) {
                console.log('GBIF RESULT FOUND AND KEY == NUBKEY');
                await insertValMissing(res.gbif)
                  .then(res => {
                    nubCount++;
                    log(`insertValMissing SUCCESS | taxonKey:${res.gbif.taxonKey}`, logStream);
                    log(JSON.stringify(res.gbif), insStream);
                  })
                  .catch(err => {
                    errCount++;
                    log(`insertValMissing ERROR ${errCount} | gbifKey:${err.gbif.key} | error:${err.message}`, logStream, true);
                    log(JSON.stringify(err), errStream, true);
                  });
              }
              else if (res.gbif.nubkey) { //we got a gbif result with a nubkey, but gbif.key != gbif.nubkey
                console.log('GBIF RESULT FOUND AND KEY != NUBKEY');
                //log(JSON.stringify(res), errStream, true);
                await insertValMissing(res.gbif)
                  .then(res => {
                    notCount++;
                    log(`insertValMissing SUCCESS | taxonKey:${res.gbif.taxonKey}`, logStream);
                    log(JSON.stringify(res.gbif), insStream);
                  })
                  .catch(err => {
                    errCount++;
                    log(`insertValMissing ERROR ${errCount} | gbifKey:${err.gbif.key} | error:${err.message}`, logStream, true);
                    log(JSON.stringify(err), errStream, true);
                  });
              }
              else {  //we got a gbif result, but there is no gbif.nubkey
                console.log('GBIF RESULT FOUND BUT NO NUBKEY');
                //log(JSON.stringify(res), errStream, true);
                await insertValMissing(res.gbif)
                  .then(res2 => {
                    nonCount++;
                    log(`insertValMissing SUCCESS | taxonKey:${res2.gbif.taxonKey}`, logStream);
                    log(JSON.stringify(res2.gbif), insStream);
                  })
                  .catch(err2 => {
                    errCount++;
                    log(`insertValMissing ERROR ${errCount} | gbifKey:${err2.gbif.key} | error:${err2.message}`, logStream, true);
                    log(JSON.stringify(err2), errStream, true);
                  });
              }
            })
            .catch(err => {
              errCount++;
              log(`getGbifSpecies (for missingKey) ERROR ${errCount} | gbifKey:${err.fix.missingKey} | ${err.message}`, logStream, true);
              log(JSON.stringify(res), errStream, true);
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
NOTE: taxonKey and acceptedNameUsageKey are type integer.
*/
async function getValMissing() {
  var text = '';

  text = `
  SELECT DISTINCT ON ("missingKey")
  "sourceKey", "missingKey", "taxonRank", "column"
  FROM (
    select distinct ON (b."acceptedTaxonKey")
    b."taxonKey" as "sourceKey", b."acceptedTaxonKey" as "missingKey", b."taxonRank", 'acceptedTaxonKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."acceptedTaxonKey"
    where (a."taxonKey" IS NULL) and b."acceptedTaxonKey" IS NOT NULL and b."acceptedTaxonKey" != 0
    union
    --retrieve a list of parentKey which lack a primary definition (no taxonKey)
    select distinct ON (b."parentKey")
    b."taxonKey" as "sourceKey", b."parentKey" as "missingKey", b."taxonRank", 'parentKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."parentKey"
    where (a."taxonKey" IS NULL) and b."parentKey" IS NOT NULL and b."parentKey" != 0
    union
    --retrieve a list of kingdomKey which lack a primary definition (no taxonKey)
    select distinct ON (b."kingdomKey")
    b."taxonKey" as "sourceKey", b."kingdomKey" as "missingKey", b."taxonRank", 'kingdomKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."kingdomKey"
    where (a."taxonKey" IS NULL) and b."kingdomKey" IS NOT NULL and b."kingdomKey" != 0
    union
    --retrieve a list of phylumKey which lack a primary definition (no taxonKey)
    select distinct ON (b."phylumKey")
    b."taxonKey" as "sourceKey", b."phylumKey" as "missingKey", b."taxonRank", 'phylumKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."phylumKey"
    where (a."taxonKey" IS NULL) and b."phylumKey" IS NOT NULL and b."phylumKey" != 0
    union
    --retrieve a list of classKey which lack a primary definition (no taxonKey)
    select distinct ON (b."classKey")
    b."taxonKey" as "sourceKey", b."classKey" as "missingKey", b."taxonRank", 'classKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."classKey"
    where (a."taxonKey" IS NULL) and b."classKey" IS NOT NULL and b."classKey" != 0
    union
    --retrieve a list of orderKey which lack a primary definition (no taxonKey)
    select distinct ON (b."orderKey")
    b."taxonKey" as "sourceKey", b."orderKey" as "missingKey", b."taxonRank", 'orderKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."orderKey"
    where (a."taxonKey" IS NULL) and b."orderKey" IS NOT NULL and b."orderKey" != 0
    union
    --retrieve a list of familyKey which lack a primary definition (no taxonKey)
    select distinct ON (b."familyKey")
    b."taxonKey" as "sourceKey", b."familyKey" as "missingKey", b."taxonRank", 'familyKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."familyKey"
    where (a."taxonKey" IS NULL) and b."familyKey" IS NOT NULL and b."familyKey" != 0
    union
    --retrieve a list of genusKey which lack a primary definition (no taxonKey)
    select distinct ON (b."genusKey")
    b."taxonKey" as "sourceKey", b."genusKey" as "missingKey", b."taxonRank", 'genusKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."genusKey"
    where (a."taxonKey" IS NULL) and b."genusKey" IS NOT NULL and b."genusKey" != 0
    union
    --retrieve a list of speciesKey which lack a primary definition (no taxonKey)
    select distinct ON (b."speciesKey")
    b."taxonKey" as "sourceKey", b."speciesKey" as "missingKey", b."taxonRank", 'speciesKey' as column
    from val_species_from_occs a
    right join val_species_from_occs b
    on a."taxonKey" = b."speciesKey"
    where (a."taxonKey" IS NULL) and b."speciesKey" IS NOT NULL and b."speciesKey" != 0
  ) agg
  `;

  return await query(text);
}

/*
  Get GBIF species for key. Key in this case is one of:
  - fix.missingKey
  - gbif.nubkey from missingKey results for a dead-end taxon
  - fix.sourceKey
  Return
  - results or error
  - incoming fix object from getValMissing for downstream processing
  In Other Words, if there's a problem getting missingKey, which we try to get on
  the first pass, we back up and try to reload the sourceKey - which is the taxon
  in our DB that's pointing to missingKey. The assumption, on such an error, is
  that sourceKey itself is incorrect, and we need to UPDATE it (or DELETE it.)
*/
async function getGbifSpecies(fix, key) {
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  console.log('getGbifSpecies |', fix, key);

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        reject({"gbif":null, "fix":fix, "err":err});
        //err.fix = fix;
        //reject(err);
      } else {
        if (body && body.key) {
          log(`getGbifSpecies(${key}) | ${res.statusCode} | gbif_key: ${body.key} | gbif_nubKey: ${body.nubKey}`, logStream, true);
          resolve({"gbif":body, "fix":fix});
        } else {
            var err = {message:`${key} NOT Found. | missingKey: http://api.gbif.org/v1/species/${fix.missingKey} | sourceKey: http://api.gbif.org/v1/species/${fix.sourceKey}`};
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

async function insertValMissing(gbif) {
  log(`insertValMissing | taxonKey = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  //translate gbif api values to val columns
  //no, don't do that for this setup. columns are named from GBIF field names here.
  var val = {};
  //val = gbifToValDirect(gbif);
  //however, our column names here derive from the GBIF occurrence download, which uses some
  //different names than their own species API columns names...
  if (gbif.rank) gbif.taxonRank = gbif.rank;
  gbif.taxonRank =   gbif.taxonRank.toUpperCase();
  gbif.taxonKey = gbif.key;
  gbif.acceptedScientificName = gbif.accepted;
  gbif.acceptedTaxonKey = gbif.acceptedKey;
  if ('ACCEPTED' == gbif.taxonomicStatus.toUpperCase()) {
    gbif.acceptedScientificName = gbif.scientificName;
    gbif.acceptedTaxonKey = gbif.taxonKey;
  }

  //console.log('insertValMissing |', gbif);

  var queryColumns = pgUtil.parseColumns(gbif, 1, [], staticColumns);
  const text = `insert into ${table_name} (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonKey"`;
  return new Promise((resolve, reject) => {
    query(text, queryColumns.values)
      .then(res => {
        res.gbif = gbif;
        //res.val = val;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        //err.val = val;
        reject(err);
      })
  })
}

/*
  Here, we've determined that the sourceKey had a bad value for missingKey. Using a good value, update our sourceKey
  record.
*/
function updateValSource(gbif, fix) {
  log(`updateValSource | taxonKey = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`, logStream, true);

  //translate gbif api values to val columns - but we only use two of them for this missingKey update
  var val = gbifToValDirect(gbif);

  var queryColumns = pgUtil.parseColumns(val, 2, [val.gbifKey], staticColumns);

  if ('parentNameUsage' == fix.column) {
    var sql = `update ${table_name} set ("taxonKey","parentNameUsageKey") = ($2,$3) where "gbifKey"=$1`;
    var arg = [fix.sourceKey,fix.sourceKey,val.parentNameUsageKey];
  } else {
    var sql = `update ${table_name} set ("${fix.column}","${fix.column}Key") = ($2,$3) where "gbifKey"=$1`;
    var arg = [fix.sourceKey,val[fix.column],val[`${fix.column}Key`]];
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
  ...can't delete from ${table_name} if val_vernacular has pointer. need to alter foreign key relation
  to 'ON DELETE CASCADE'.
*/
function deleteValSource(taxonKey) {
  var sql = `delete from ${table_name} where "taxonKey"=$1;`;
  var arg = [taxonKey];

  log(`deleteValSource Query | ${sql} | ${arg}`, logStream, true);

  return new Promise((resolve, reject) => {
    query(sql,arg)
      .then(res => {
        res.taxonKey = taxonKey;
        resolve(res);
      })
      .catch(err => {
        err.taxonKey = taxonKey;
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|inserted == nubkey:${nubCount}|inserted != nubkey:${notCount}|inserted NO nubkey:${nonCount}|errors:${errCount}`, logStream, true);
  log(`total:${rowCount}|inserted == nubkey:${nubCount}|inserted != nubkey:${notCount}|inserted NO nubkey:${nonCount}|errors:${errCount}`, insStream);
}
