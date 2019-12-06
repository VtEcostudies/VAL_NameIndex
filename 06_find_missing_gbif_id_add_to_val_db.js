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
  acceptedNameUsageId

  Query the GBIF species API for complete record data for each missing taxonId.
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
var logFileName = 'insert_missing_taxa_' + moment().format('YYYYMMDD-HHMMSS') + '.txt';
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
          getGbifSpecies(res.rows[i].taxonId) //use taxonId - only column returned from SELECT query
            .then(res => {
              insertValTaxon(res)
                .then(res => {
                  insCount++;
                  const msg = `insertValTaxon SUCCESS | taxonId:${res.val.taxonId}`;
                  console.log(msg);
                  wStream[0].write(`${msg}\n`);
                })
                .catch(err => {
                  errCount++;
                  const msg = `insertValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`;
                  console.log(msg);
                  wStream[0].write(`${msg}\n`);
                })
            })
            .catch(err => {
              console.log('getGbifSpecies ERROR |', err.message);
            })
        }
      })
      .catch(err => {
        console.log('getValMissing ERROR |', err.message);
      });
  })
  .catch(err => {
    console.log('getColumns ERROR |', err.message);
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
  text = `select vg."taxonId"
          from val_species vs
          right join val_gbif_taxon_id vg
          on vs."gbifId" = vg."gbifId"
          where vs."gbifId" is null;`;

  text = `--retrieve a list of acceptedNameUsageId which lack a primary definition (no taxonId)
        select cast(va."acceptedNameUsageId" as int8) as "taxonId"
        from val_species vs
        right join val_species va
        on vs."taxonId" = va."acceptedNameUsageId"
        where vs."taxonId" is null
        union
        --retrieve a list of kingdomId which lack a primary definition (no taxonId)
        select b."kingdomId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."kingdomId"
        where a."gbifId" is null and b."kingdomId" is not null
        union
        --retrieve a list of phylumId which lack a primary definition (no taxonId)
        select b."phylumId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."phylumId"
        where a."gbifId" is null and b."phylumId" is not null
        union
        --retrieve a list of classId which lack a primary definition (no taxonId)
        select b."classId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."classId"
        where a."gbifId" is null and b."classId" is not null
        union
        --retrieve a list of orderId which lack a primary definition (no taxonId)
        select b."orderId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."orderId"
        where a."gbifId" is null and b."orderId" is not null
        union
        --retrieve a list of familyId which lack a primary definition (no taxonId)
        select b."familyId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."familyId"
        where a."gbifId" is null and b."familyId" is not null
        union
        --retrieve a list of genusId which lack a primary definition (no taxonId)
        select b."genusId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."genusId"
        where a."gbifId" is null and b."genusId" is not null
        union
        --retrieve a list of speciesId which lack a primary definition (no taxonId)
        select b."speciesId" as "taxonId"
        from val_species a
        right join val_species b
        on a."gbifId" = b."speciesId"
        where a."gbifId" is null and b."speciesId" is not null
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
        reject(err);
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

  if (gbif.canonicalName) {
    var rank = gbif.rank?gbif.rank.toLowerCase():undefined;
    var speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=rank=='species'?speciessub[1]:null;
    val.infraspecificEpithet=rank=='subspecies'?speciessub[2]:null;
    val.infraspecificEpithet=rank=='variety'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
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
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:null;
  val.taxonRemarks=gbif.remarks?gbif.remarks:null;
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
