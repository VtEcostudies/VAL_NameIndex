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
var fs = require('fs');
var Request = require("request");
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dDir = paths.dwcaDir; //path to directory holding extracted GBIF DwCA species files
var wStream = []; //array of write streams

const err_id = 9; //we 'fix' a variety of 'errors' here. the index of the one to fix.

getColumns()
  .then(res => {
    getValErrors(err_id)
      .then(res => {
        console.log('Error row count:', res.rowCount, 'First row:', res.rows[0]);
        for (var i=0; i<res.rowCount; i++) {
          getGbifSpecies(res.rows[i].gbifId)
            .then(res => {
              updateValTaxon(res, err_id)
                .then(res => {})
                .catch(err => {
                  console.log('updateValTaxon ERROR | gbifId:', err.val?err.val.taxonId:'err.val is empty', '|', err.message);
                })
            })
            .catch(err => {
              console.log('getGbifSpecies ERROR |', err);
            })
        }
      })
      .catch(err => {
        console.log('getValErrors ERROR |', err);
      });
  })
  .catch(err => {
    console.log('getColumns ERROR |', err);
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
  return await query(select_err[fix_id]);
}

function getGbifSpecies(key) {
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        reject(err);
      } else if (res.statusCode > 299) {
        console.log(`getGbifSpecies(${key}) | ${res.statusCode}`);
        reject(res);
      } else {
        console.log(`getGbifSpecies(${key}) | ${res.statusCode}`);
        resolve(body);
      }
    });
  });
}

async function updateValTaxon(gbif, fix_id=1) {
  //translate gbif api values to val columns
  console.log(`taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`);
  var val = {};
  var qryColumns = [];
  var sql_update = [];
  var speciessub = [];
  if (gbif.canonicalName) {
    speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=gbif.rank.toLowerCase()=='species'?speciessub[1]:'';
    val.infraspecificEpithet=gbif.rank.toLowerCase()=='subspecies'?speciessub[2]:'';
    val.infraspecificEpithet=gbif.rank.toLowerCase()=='variety'?speciessub[2]:'';
  }

  val.gbifId=gbif.key;
  val.taxonId=gbif.key;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:gbif.scientificName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedKey?gbif.acceptedKey:gbif.key;
  val.acceptedNameUsage=gbif.accepted?gbif.accepted:gbif.scientificName;
  val.taxonRank=gbif.rank.toLowerCase();
  val.taxonomicStatus=gbif.taxonomicStatus.toLowerCase()
  val.parentNameUsageId=gbif.parentKey || 0;
  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=gbif.authorship;
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:'';
  val.taxonRemarks=gbif.remarks;
  val.kingdom=gbif.kingdom?gbif.kingdom:null;
  val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:null;;
  val.phylum=gbif.phylum?gbif.phylum:null;
  val.phylumId=gbif.phylumKey?gbif.phylumKey:null;
  val.class=gbif.class?gbif.class:null;
	val.classId=gbif.classKey?gbif.classKey:null;
  val.order=gbif.order?gbif.order:null;
  val.orderId=gbif.orderKey?gbif.orderKey:null;
  val.family=gbif.family?gbif.family:null;
  val.familyId=gbif.familyKey?gbif.familyKey:null;
  val.genus=gbif.genus?gbif.genus:null;
  val.genusId=gbif.genusKey?gbif.genusKey:null;
  val.species=gbif.species?gbif.species:null;
  val.speciesId=gbif.speciesKey?gbif.speciesKey:null;

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
  console.log(sql_update[fix_id], qryColumns[fix_id].values);
  return new Promise((resolve, reject) => {
    query(sql_update[fix_id], qryColumns[fix_id].values)
      .then(res => {
        resolve(res);
      })
      .catch(err => {
        err.gbif = gbif;
        err.val = val;
        reject(err);
      })
  })
}
