/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 10_1_update_species_to_vernacular.js

  Purpose: Move/copy vernacular names in val_species to val_vernacular.

  Specifics:

*/

const fs = require('fs');
const Request = require('request');
const moment = require('moment');
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const log = require('./97_utilities').log;

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding inp data files - INCLUDING TRAILING SLASH
var subDir = '00_vernacular_names/';
var fileName = 'update_species_to_vernacular.txt';

var logFileName = 'log_' + moment().format('YYYYMMDD-HHmmsss') + '_' + fileName;
var errFileName = 'err_' + fileName;

var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
var errStream = null;
async function err(text, stream=null, consoul=false) {
  if (!errStream) {
    errStream = await fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
  }
  log(text, errStream, consoul);
}

var rowCount = 0; //count records available
var insCount = 0; //count records inserted
var xstCount = 0; //count val_vernacular name records already exisiting
var errCount = 0; //count record errors

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

getColumns('val_vernacular')
  .then(col => {
    getValSpeciesVernacular()
      .then(rows => {
        rowCount = rows.length;
        for (var i=0; i<rows.length; i++) { //iterate over array of species returned
        //for (var i=0; i<1; i++) { //test iterate 1 times over array of species returned
          insertValVernacular(rows[i], i)
            .then(res => {})
            .catch(err => {});
          }
      })
      .catch(err => {
        log(err.message, logStream, true);
      });
  })
  .catch(err => {
    log(`getColumns ERROR | ${JSON.stringify(err)}`, logStream, true);
  })

function getColumns(tableName) {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns(tableName, staticColumns);
}

/*
*/
function getValSpeciesVernacular() {
  var sql = `select * from val_species where "vernacularName" is not null and "vernacularName" != ''`;
  sql = `select s."scientificName",s."taxonId",s."vernacularName",v."vernacularName" as v_vernacular
        from val_species s
        left join val_vernacular v on LOWER(s."vernacularName")=LOWER(v."vernacularName")
        where s."vernacularName" is not null and v."vernacularName" is null`;

  return new Promise((resolve, reject) => {
    query(sql)
      .then(res => {
        log(`getValSpeciesVernacular | rows:${res.rows.length}`, logStream, true);
        resolve(res.rows);
      })
      .catch(err => {
        reject(err);
      });
  })
}

/*
*/
function insertValVernacular(species, idx) {
  var inp = {};

  inp.scientificName = species.scientificName;
  inp.taxonId = species.taxonId;
  inp.vernacularName = species.vernacularName;
  inp.source = 'VTSR';
  inp.language = 'en';
  inp.preferred = 't'; //if it's coming from val_species and it's misssing from val_vernacular, it's preferred

  var qryColumns = pgUtil.parseColumns(inp, 1, [], staticColumns);
  var sql_insert = `insert into val_vernacular (${qryColumns.named}) values (${qryColumns.numbered}) returning *`;

  log(`insertValVernacular | ${sql_insert}`, logStream, true);
  log(qryColumns.values, logStream, true);

  return new Promise((resolve, reject) => {
    query(sql_insert, qryColumns.values)
      .then(async res => {
        insCount++;
        res.species = species;
        res.idx = idx;
        log(`insertValVernacular SUCCESS | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, logStream, true);
        resolve(res);
      })
      .catch(async err => {
        if (err.code == 23505) {xstCount++;} //val_vernacular duplicate vernacularName
        else {
          errCount++;
          await err(`insertValVernacular ERROR ${errCount} | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, errStream, true);
          await err(`insertValVernacular ERROR ${errCount} | code: ${err.code} | message: ${err.message} | where: ${err.where}`, errStream, true);
        }
        err.species = species;
        err.idx = idx;
        //log(`insertValVernacular ERROR | taxonId:${inp.taxonId} | scientificName: ${inp.scientificName} | vernacularName: ${inp.vernacularName}`, logStream, true);
        //log(`insertValVernacular ERROR | code: ${err.code} | message: ${err.message} | where: ${err.where}`, logStream, true);
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|existing:${xstCount}|errors:${errCount}`, logStream, true);
  if(errStream) {err(`total:${rowCount}|inserted:${insCount}|existing:${xstCount}|errors:${errCount}`, errStream, true);}
}
