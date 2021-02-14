/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 16_find_and_remove_duplicate_taxa.js

  Purpose: Find duplicate taxa, find canonical IDs, replace duplicates with
  canonical values, delete duplicates.

  Specifics:

  1) get dupes for a taxonRank, eg. phylum
  2) iterate over the list of results
  3) for each taxon, find the canonical taxonId, defined as the GBIF taxon where key==nubKey
  4) set all taxonRankIds to the nubKey value
  5) set all parentNameUsageIds to the nubKey value
  6) if taxonRank<=species, figure out whether to apply this to acceptedNameUsageId

  Notes:

  1) This worked up to family, at which point there were many GENUS taxa having more than one
  nubKey.
*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const log = require('./97_utilities').log;

var testMode = true; //select or update/delete?
var delkey = false; //call delete function?

log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var baseName = '00_Fix_Duplicate_VAL_Taxa';
var subDir = `${baseName}/`; //put output into a sub-dir so we can easily find it
var logFileName = 'fix_duplicate_taxa_' + moment().format('YYYYMMDD-HHMMSSS') + '.txt';
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});
var errFileName = 'err_' + baseName + '.txt';
var errStream = null;
function err(text, stream, consoul) {
  if (!errStream) {
    errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
  }
  log(text, errStream, console);
}
var errCount = 0;

process.on('exit', function(code) {
  //displayStats();
  return log(`About to exit with code ${code}`, logStream, true);
});

var ranks = ['genus'];

ranks.forEach((rank, index, array) => {
  getValDupes(rank)
    .then(async res => {
      log(`TaxonRank: ${res.rows[0]?res.rows[0].taxonRank:'(Empty)'} | Duplicates: ${res.rowCount} | ${testMode?'TESTMODE':null}`, logStream, true);
      for (var i=0; i<res.rowCount; i++) {
        log(`------------------------------------------------------------------------------------------`, logStream, true);
        await getGbifnubKey(res.rows[i].scientificName, rank)
          .then(async res => {
            await fixTaxonRankKeys(res.canonicalName, res.rank.toLowerCase(), res.nubKey);
            await fixParentUsageKeys(res.canonicalName, res.rank.toLowerCase(), res.nubKey);
            await fixAcceptedUsageKeys(res.canonicalName, res.rank.toLowerCase(), res.nubKey);
            if (delkey) {await deleteUnusedKeys(res.canonicalName, res.rank.toLowerCase(), res.nubKey);}
          })
          .catch(err => {
            errCount++;
            log(`getGbifnubKey ERROR ${errCount} | ${err.message}`, logStream, true);
            err(`getGbifnubKey ERROR ${errCount} | ${err.message}`, errStream, true);
          })
      }
    })
    .catch(err => {
      log(`getValDupes ERROR | ${err}`, logStream, true);
    });
});

async function getValDupes(taxonRank='class') {
  var sql = `SELECT
    "scientificName", "taxonRank", COUNT(*) as dupes
      FROM
          val_species
      GROUP BY
          "scientificName", "taxonRank"
      HAVING
          COUNT(*) > 1
          AND "taxonRank" = '${taxonRank}'
      ORDER BY
          "dupes" desc, "taxonRank";`
  return await query(sql);
}

function getGbifnubKey(taxon, rank) {
  var parms = {
    url: `http://api.gbif.org/v1/species?name=${taxon}&rank=${rank}`,
    json: true
  };

  log(`getGbifnubKey | ${parms.url}`, logStream, true);

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        reject(err);
      } else if (res.statusCode > 299) {
        log(`getGbifnubKey(${taxon}) | http ${res.statusCode}`, logStream, true);
        reject(res);
      } else {
        log(`getGbifnubKey(${taxon}) | http ${res.statusCode}`, logStream, true);
        var ret = [];
        body.results.forEach((obj,idx,arr) => {
          if (obj.key == obj.nubKey) {
            log(`getGbifnubKey | FOUND canonical nubKey: ${obj.key}`, logStream, true);
            ret.push(obj);
          }
        })
        if (ret.length == 1) {resolve(ret[0]);}
        else if (ret.length > 1) {
          //resolve(ret[0]);
          reject({"message":`${ret.length} nubKeys FOUND.`});
        }
        else {reject({"message":"nubKey NOT FOUND."});}
      }
    });
  });
}

/*
  Update all taxonRank keys to nubKey for canonicalName. In val_species, we have columns for each
  major taxonRank: kingdom, phylum, class, order, etc. Those column names are the taxonRank itself,
  so it's easy to convert those to our column names for update.
*/
function fixTaxonRankKeys(canonicalName, taxonRank, nubKey) {

  log(`fixTaxonRankKeys VALUES | taxonRank = ${taxonRank} | scientificName = ${canonicalName} | nubKey = ${nubKey}`, logStream, true);

  var select = `select * from val_species where "${taxonRank}Id"!='${nubKey}' and "${taxonRank}"='${canonicalName}';`;
  var update = `update val_species
  set "${taxonRank}Id"='${nubKey}'
  where "${taxonRank}Id"!='${nubKey}' and "${taxonRank}"='${canonicalName}';`

  var sql = testMode ? select : update;

  log(`fixTaxonRankKeys QUERY | ${sql}`);

  return new Promise((resolve, reject) => {
    query(sql)
      .then(res => {
        if (testMode) {
          log(`fixTaxonRankKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | rows: ${res.rows.length}`, logStream, true);
          //console.dir(res.rows[0]);
        } else {
           log(`fixTaxonRankKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | command: ${res.command} | success: ${res.rowCount}`, logStream, true);
        }
        resolve(res);
      })
      .catch(err => {
        reject(err);
      })
  })
}

/*
  Update all parentNameUsage keys from key to nubKey for one rank level lower than taxonRank.
*/
function fixParentUsageKeys(canonicalName, taxonRank, nubKey) {

  log(`fixParentUsageKeys VALUES | taxonRank = ${taxonRank} | scientificName = ${canonicalName} | nubKey = ${nubKey}`, logStream, true);

  var select = `select * from val_species where "parentNameUsageId" in (
    	select "taxonId" from val_species
    	where "scientificName"='${canonicalName}' and "taxonRank"='${taxonRank}' and "taxonId" != '${nubKey}')`;

  var update = `update val_species set "parentNameUsageId" = '${nubKey}' where "parentNameUsageId" in (
    	select "taxonId" from val_species
    	where "scientificName"='${canonicalName}' and "taxonRank"='${taxonRank}' and "taxonId" != '${nubKey}')`;

  var sql = testMode ? select : update;

  log(`fixParentUsageKeys QUERY | ${sql}`);

  return new Promise((resolve, reject) => {
    query(sql)
      .then(res => {
        if (testMode) {
          log(`fixParentUsageKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | rows: ${res.rows.length}`, logStream, true);
          //console.dir(res.rows[0]);
        } else {
           log(`fixParentUsageKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | command: ${res.command} | success: ${res.rowCount}`, logStream, true);
        }
        resolve(res);
      })
      .catch(err => {
        reject(err);
      })
  })
}

/*
  Update all acceptedNameUsage keys from key to nubKey for one rank level lower than taxonRank.
*/
function fixAcceptedUsageKeys(canonicalName, taxonRank, nubKey) {

  log(`fixAcceptedUsageKeys VALUES | taxonRank = ${taxonRank} | scientificName = ${canonicalName} | nubKey = ${nubKey}`, logStream, true);

  var select = `select * from val_species where "acceptedNameUsageId" in (
    	select "taxonId" from val_species
    	where "scientificName"='${canonicalName}' and "taxonRank"='${taxonRank}' and "taxonId" != '${nubKey}')`;

  var update = `update val_species set "acceptedNameUsageId" = '${nubKey}' where "acceptedNameUsageId" in (
    	select "taxonId" from val_species
    	where "scientificName"='${canonicalName}' and "taxonRank"='${taxonRank}' and "taxonId" != '${nubKey}')`;

  var sql = testMode ? select : update;

  log(`fixAcceptedUsageKeys QUERY | ${sql}`);

  return new Promise((resolve, reject) => {
    query(sql)
      .then(res => {
        if (testMode) {
          log(`fixAcceptedUsageKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | rows: ${res.rows.length}`, logStream, true);
          //console.dir(res.rows[0]);
        } else {
           log(`fixAcceptedUsageKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | command: ${res.command} | success: ${res.rowCount}`, logStream, true);
        }
        resolve(res);
      })
      .catch(err => {
        reject(err);
      })
  })
}

/*
  Delete non-canonical taxa for taxonRank and scientificName.
*/
function deleteUnusedKeys(canonicalName, taxonRank, nubKey) {

  log(`deleteUnusedKeys VALUES | taxonRank = ${taxonRank} | scientificName = ${canonicalName} | nubKey = ${nubKey}`, logStream, true);

  var select = `select "taxonId","scientificName","taxonRank" from val_species
  where "taxonRank"='${taxonRank}' and "scientificName"='${canonicalName}' and "taxonId"!='${nubKey}';`;

  var delqry = `delete from val_species
  where "taxonRank"='${taxonRank}' and "scientificName"='${canonicalName}' and "taxonId"!='${nubKey}';`;

  var sql = testMode ? select : delqry;

  log(`deleteUnusedKeys QUERY | ${sql}`);

  return new Promise((resolve, reject) => {
    query(sql)
      .then(res => {
        if (testMode) {
          log(`deleteUnusedKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | rows: ${res.rows.length}`, logStream, true);
          //console.dir(res.rows[0]);
        } else {
           log(`deleteUnusedKeys RESULTS | taxon:${canonicalName} | rank:${taxonRank} | command: ${res.command} | success: ${res.rowCount}`, logStream, true);
           //console.dir(res);
        }
        resolve(res);
      })
      .catch(err => {
        reject(err);
      })
  })
}
