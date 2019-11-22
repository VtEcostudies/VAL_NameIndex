/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 06_find_missing_gbif_id_add_to_val_db.js

  Purpose:
  
  Specifics:

*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var logFileName = 'insert_missing_taxa_' + moment().format('YYYYMMDD-HH:MM:SS')
var wStream = []; //array of write streams
var insCount = 0;
var errCount = 0;

console.log('output file name', logFileName);

getColumns()
  .then(res => {
    getValMissing()
      .then(res => {
        console.log(res.rowCount, 'missing gbif Ids.', 'First row:', res.rows[0]);
        for (var i=0; i<res.rowCount; i++) {
          wStream[0] = fs.createWriteStream(`${dataDir}/${logFileName}`, {flags: 'w'});
          getGbifSpecies(res.rows[i].gbifId)
            .then(res => {
              insertValTaxon(res)
                .then(res => {
                  insCount++;
                  const msg = `insertValTaxon SUCCESS | gbifId:${res.val.taxonId}`;
                  console.log(msg);
                  write[0].write(`${msg}\n`);
                })
                .catch(err => {
                  errCount++;
                  const msg = `insertValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`;
                  write[0].write(`${msg}\n`);
                })
            })
            .catch(err => {
              console.log('getGbifSpecies ERROR |', err);
            })
        }
      })
      .catch(err => {
        console.log('getValMissing ERROR |', err);
      });
  })
  .catch(err => {
    console.log('getColumns ERROR |', err);
  })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_species", staticColumns);
}

async function getValMissing() {
  const text = `select vg."gbifId"
                from val_species vs
                right join val_gbif_taxon_id vg
                on vs."gbifId" = vg."gbifId"
                where vs."gbifId" is null;`;
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

async function insertValTaxon(gbif) {
  //translate gbif api values to val columns
  console.log(`taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`);
  var val = {};
  var speciessub = [];
  if (gbif.canonicalName) {
    speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=gbif.rank=='SPECIES'?speciessub[1]:'';
    val.infraspecificEpithet=gbif.rank=='SUBSPECIES'?speciessub[2]:'';
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

  var queryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
  const text = `insert into val_species (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
  return new Promise((resolve, reject) => {
    query(text, queryColumns.values)
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
