/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 07_fix_errors_val_db.js

  Purpose: Find and fix known errors in the val_species table.

  Specifics:

  - write an SQL statement to find the errors
  - extract gbifId from each row
  - query the GBIF species API for corrected values
  - update val_species with the corrected data

  Add new queries here as needed.

  To use a new query, add a new query string to the function getValErrors() as a
  new array element, and set the value of the global varialble err_id to the new
  array index.

*/

//https://nodejs.org/api/readline.html
const fs = require('fs');
const Request = require("request");
const moment = require('moment');
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const gbifToValDirect = require('./VAL_Utilities/98_gbif_to_val_columns').gbifToValDirect;
const log = require('./VAL_Utilities/97_utilities').log;
var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dwcaDir = paths.dwcaDir; //path to directory holding extracted GBIF DwCA species files
var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var baseName = '00_Fix_VAL_Taxa_Errors';
var subDir = `${baseName}/`; //put output into a sub-dir so we can easily find it
var logFileName = 'fix_taxa_errors' + moment().format('YYYYMMDD-HHMMSSS') + '.txt';
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
var errFileName = 'err_' + baseName + '.txt';
//var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});

const err_id = 11; //we 'fix' a variety of 'errors' here. the index of the one to fix.
const update = 1; //flag whether to update or just compare val vs gbif
var errCount = 0;

getColumns()
  .then(res => {
    getValErrors(err_id)
      .then(async res => {
        console.log('Error row count:', res.rowCount, 'First row:', res.rows[0]);
        for (var i=0; i<res.rowCount; i++) {
          await getGbifSpecies(i, res.rows[i])
            .then(async res => {
              if (update) {
                await compareRecords(res.idx, res.val, res.gbif);
                await updateValTaxon(res.idx, res.val, res.gbif, err_id)
                  .then(res => {
                    log(`${res.idx} | updateValTaxon SUCCESS | gbifId:${res.new.gbifId} | `, logStream, true);
                  })
                  .catch(err => {
                    log(`${res.idx} | updateValTaxon ERROR | gbifId:${err.new?err.new.taxonId:'err.new is empty'} | ${err.message}`, logStream, true);
                  })
              } else {
                await compareRecords(res.idx, res.val, res.gbif);
              }
            })
            .catch(err => {
              console.log('getGbifSpecies ERROR |', err.message);
            })
        }
      })
      .catch(err => {
        console.log('getValErrors ERROR |', err.message);
      });
  })
  .catch(err => {
    console.log('getColumns ERROR |', err.message);
  })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_species", staticColumns);
}

async function getValErrors(fix_id=1) {
  var select_err = [];
  select_err[0] = `select "gbifId" from val_species where "scientificName" is null or "scientificName" = ''`;
  select_err[1] = `select "gbifId" from val_species where "taxonRank" = 'phylum'`;
  select_err[2] = `select "gbifId" from val_species where "taxonRank" = 'class' and "scientificName" != "acceptedNameUsage"`;
  select_err[3] = `select "gbifId" from val_species where "taxonRank" = 'order' and "scientificName" != "acceptedNameUsage"`;
  select_err[4] = `select "gbifId" from val_species where "taxonRank" = 'family' and "scientificName" != "acceptedNameUsage"`;
  select_err[5] = `select "gbifId" from val_species where "taxonRank" = 'genus' and "scientificName" like '%' || "scientificNameAuthorship" and "scientificNameAuthorship" != ''`;
  select_err[6] = `select "gbifId" from val_species where "scientificName" like '%' || "scientificNameAuthorship" and "scientificNameAuthorship" != ''`;
  select_err[7] = `select "gbifId" from val_species where split_part("scientificName", ' ', 4) != '';`;
  select_err[8] = `select "gbifId" from val_species where split_part("scientificName", ' ', 3) != '' and "taxonRank" NOT IN ('subspecies','variety','form')`;
  select_err[9] = `select "gbifId" from val_species where split_part("scientificName", '?', 2) != '';`;
  select_err[10] = `select * from val_species where "taxonId" != "acceptedNameUsageId" and "taxonomicStatus" = 'accepted';`;
  select_err[11] = `select * from val_species where "taxonRank" IN ('species', 'subspecies', 'variety');`;
  return await query(select_err[fix_id]);
}

function compareRecords(i, val, gbif) {
  /*
  if (res.val.taxonomicStatus.toLowerCase() == res.gbif.taxonomicStatus.toLowerCase()) {
    log(`===================>>>${res.idx}|MATCHING taxonomicStatus|${res.val.scientificName}`, logStream, true);
  }*/
  //console.log(gbif);
  var msg = 0;
  for (var key in val) {
    if (gbif[key]) {
      //console.log(key);
      var v = val[key]; v = v?v:'';
      var g = gbif[key]; g = g?g:'';
      if (key != 'scientificName' && v.toLowerCase() != g.toLowerCase()) {
        log(`${i} | ${key} | VAL:${val[key]} | GBIF:${gbif[key]}`, logStream, true); msg++;
      }
    }
  }
  if (val.scientificName != gbif.canonicalName) {
    log(`${i} | canonicalName | VAL:${val.scientificName} | GBIF:${gbif.canonicalName}`, logStream, true); msg++;
  }
  if (gbif.acceptedId && val.acceptedNameUsageId != gbif.acceptedId) {
    log(`${i} | acceptedNameUsageId | VAL:${val.acceptedNameUsageId} | GBIF:${gbif.acceptedId}`, logStream, true); msg++;
  }
  if (gbif.accepted && val.acceptedNameUsage != gbif.accepted) {
    log(`${i} | acceptedNameUsage | VAL:${val.acceptedNameUsage} | GBIF:${gbif.accepted}`, logStream, true); msg++;
  }
  if (gbif.parentKey && val.parentNameUsageId != gbif.parentKey) {
    log(`${i} | parentNameUsageId | VAL:${val.parentNameUsageId} | GBIF:${gbif.parentKey}`, logStream, true); msg++;
  }
  if (msg) {
    errCount++;
    log(`${errCount} | End compareRecords | gbifId:${val.gbifId} | rank:${val.taxonRank} | name:${val.scientificName}`, logStream, true);
    log(`-------------------------------------------------------------------------------------`, logStream, true);
  }
}

function getGbifSpecies(idx, val) {
  var key = val.gbifId;
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.val = val;
        err.idx = idx;
        reject(err);
      } else if (res.statusCode > 299) {
        //console.log(`getGbifSpecies(${key}) | ${res.statusCode}`);
        res.val = val;
        res.idx = idx;
        reject(res);
      } else {
        var ret = {};
        //console.log(`getGbifSpecies(${key}) | ${res.statusCode}`);
        ret.val = val;
        ret.idx = idx;
        ret.gbif = body;
        resolve(ret);
      }
    });
  });
}

/*
  Update val_species in specific ways for specific fix_ids.
*/
async function updateValTaxon(idx, old, gbif, fix_id=1) {
  //translate gbif api values to val columns
  console.log(`taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`);
  var qryColumns = [];
  var sql_update = [];
  var speciessub = [];

  var val = gbifToValDirect(gbif);

  if (fix_id == 5 || fix_id == 6 || fix_id == 7) { //for genus & species, oddly the opposite occurs: acceptedNameUsage has author. leave that alone, since it doesn't break nameindexer.
    delete val.acceptedNameUsageId;
    delete val.acceptedNameUsage;
  }

  qryColumns[0] = {};
  qryColumns[0].values = [gbif.scientificName, gbif.key];
  sql_update[0] = `update val_species set "scientificName"=$1 where "gbifId"=$2 and "scientificName"='';`;
  if (fix_id > 0) {
    qryColumns[fix_id] = pgUtil.parseColumns(val, 2, [val.gbifId], staticColumns);
    sql_update[fix_id] = `update val_species set (${qryColumns[fix_id].named}) = (${qryColumns[fix_id].numbered}) where "gbifId"=$1 returning "taxonId"`;
  }
  log(JSON.stringify(sql_update[fix_id]) + JSON.stringify(qryColumns[fix_id].values), logStream, true);
  return new Promise((resolve, reject) => {
    query(sql_update[fix_id], qryColumns[fix_id].values)
      .then(res => {
        res.gbif = gbif;
        res.old = old;
        res.new = val;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        err.old = old;
        err.new = val;
        err.idx = idx;
        reject(err);
      })
  })
}
