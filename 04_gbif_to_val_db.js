/*
  Author: Jason Loomis

  Project: GBIF_Species_to_VAL_Species

  Parse GBIF species download DWcA into a VAL species list DwCA that can be
  used by the VAL ALA nameindexer.

  As of the fall of 2019, the GBIF API does not provide a species download that
  includes checklists. Instead, they provide an occurrence download that
  enumerates species.

  File: 04_gbif_to_val_db.js

  Notes:

  Specifics:

  index GBIF name
  1  taxonKey
  2  scientificName
  3  acceptedTaxonKey
  4  acceptedScientificName
  5  taxonRank
  6  taxonomicStatus
  7  kingdom
	8  kingdomKey
  9  phylum
	10 phylumKey
  11 class
	12 classKey
  13 order
  14 orderKey
  15 family
  16 familyKey
  17 genus
  18 genusKey
  19 species
  20 speciesKey

  - The database ingests all columns, but renames them to DwCA-compliant names,
  which are used by VAL.

  - To derive parentNameUsageId, we find the second-to-last non-zero value of
  GBIF kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, or
  speciesKey. Special cases:

      - If kingdomKey is the last key, we assign parentNameUsageId to itself.
      - If acceptedScientificName contains subsp. or var., we assign speciesKey
      to parentNameUsageId.

  - nomenclaturalCode, will be assigned the static value 'GBIF' for
  Catalogue Of Life, the source nomenclatural index of the GBIF backbone, which
  is where this initial dataset originated.

  - scientificNameAuthorship, will be derived from the parsed ending of GBIF
  acceptedScientificName. We find authorship by removing the leading 1 or 2
  tokens of scientificName.

*/

//https://nodejs.org/api/readline.html
var readline = require('readline');
var fs = require('fs');
var paths = require('./00_config').paths;
var delim = require('./00_config').delim;
var csvLineTo1DArray = require('./99_parse_csv_to_array').csvLineTo1DArray;

const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
var staticColumns = [];
var valKingdoms = ['animalia','archaea','chromista','fungi','plantae','protozoa']; //kingdoms to include in val_species

const id = delim.infield;
const od = delim.outfield;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dDir = paths.dwcaDir; //path to directory holding extracted GBIF DWcA species files

var idx = 0; //file row index
var out = '';
var log = '';
var taxonRank = ''
var accAuth = []; //array of tokens parsed from acceptedNameUsage to find authorship
var tk_idx = 0; //acceptedNameUsage token index
var author = '';
var subsps = ''; //sub-species
var variety = ''; //variety is to botanists what sub-species is for others

getColumns().then(res => {

  var fRead = readline.createInterface({
    input: fs.createReadStream(`${dDir}/gbif_species.txt`)
  });

  //read occurrence.txt
  fRead.on('line', async function (row) {
      var gbf = csvLineTo1DArray(row, id);
      var val = {};

      val.gbifId = gbf[0];  //gbifId ~ key
      val.taxonId = gbf[0];  //taxonID ~ taxonKey
      val.scientificName = gbf[1];  //scientificName
      val.acceptedNameUsageId = gbf[2];  //acceptedNameUsageID ~ acceptedTaxonKey
      val.acceptedNameUsage = gbf[3];  //acceptedNameUsage ~ acceptedScientificName
      val.taxonRank = gbf[4].toLowerCase();  //taxonRank is often missing. see below
      val.taxonomicStatus = gbf[5].toLowerCase();;  //taxonomicStatus
      val.parentNameUsageId = 0;      //parentNameUsageId
      val.nomenclaturalCode = 'GBIF';   //nomenclaturalCode
      val.scientificNameAuthorship = null;      //scientificNameAuthorship
      val.specificEpithet = null;      //specificEpithet (==species)
      val.infraspecificEpithet = null;     //infraspecificEpithet (==sub-species or variety)
      val.vernacularName = null;     //vernacularName
      val.taxonRemarks = null;     //taxonRemarks
      val.datasetName = null;     //datasetName
      val.datasetId = null;     //datasetId

      val.kingdom = gbf[6];
      val.phylum = gbf[8];
      val.class = gbf[10];
      val.order = gbf[12];
      val.family = gbf[14];
      val.genus = gbf[16];
      val.species = gbf[18];

      val.kingdomId = gbf[7] ? gbf[7] : 0;
      val.phylumId = gbf[9] ? gbf[9] : 0;
      val.classId = gbf[11] ? gbf[11] : 0;
      val.orderId = gbf[13] ? gbf[13] : 0;
      val.familyId = gbf[15] ? gbf[15] : 0;
      val.genusId = gbf[17] ? gbf[17] : 0;
      val.speciesId = gbf[19] ? gbf[19] : 0;

      //fix taxonomicStatus: if there's an acceptedNameUsageId, it's 'accepted'
      val.taxonomicStatus = gbf[2] ? 'accepted' : null;

      //parentNameUsageId and taxonRank
      for (var i=7; i<=19; i+=2) {
        if (gbf[i]) { //test for non-null key
          if (i == 7) { //in the special case of kingdom, parentNameUsageId is self-referential
            val.parentNameUsageId = gbf[7];
          } else {
            val.parentNameUsageId = gbf[i-2];
          }
          switch(i) {
            case  7: taxonRank = 'kingdom'; break;
            case  9: taxonRank = 'phylum'; break;
            case 11: taxonRank = 'class'; break;
            case 13: taxonRank = 'order'; break;
            case 15: taxonRank = 'family'; break;
            case 17: taxonRank = 'genus'; break;
            case 19: taxonRank = 'species'; break;
          }
          val.taxonRank = taxonRank;
        }
      }

      //scientificNameAuthorship, specificEpithet, infraspecificEpithet
      /*
      Since this GBIF download has no sub-species, and since acceptedNameUsage has
      author(s), if they're present, we can just strip the 2 leading parts of the
      scientificName. Whatever remains is scientificNameAuthorship.

      Note: there are cases when scientificName has authors, but acceptedNameUsage
      does not. When this happens we do not try to go back and determine the authors
      from abandoned naming.

      Also: remove double-quotes ("), then enclose the result in double-quotes.

      Further: the 3rd key in acceptedScientificName can also be 'subsp.' or 'var.'
      */
      accAuth = gbf[3].split(" ").slice(); //break acceptedNameUsage into tokens by spaces
      author = '';
      subsps = '';
      variety = '';
      tk_idx = 2; //default token index is 2 - in most cases, scientificName is 2 tokens. not always.


      if (accAuth[1]) { //if there is a 2nd token...
        if (taxonRank.toLowerCase() == 'species') { //if rank is species, 2nd token is species
          val.specificEpithet = accAuth[1]; //specificEpithet is a single token naming a species
        } else if (taxonRank.toLowerCase() == 'genus') {
          tk_idx = 1; //scientificName for genus has only one token. scientificNameAuthorship would begin at 2nd token.
        }
      }

      if (accAuth[tk_idx]) { //if there's a token after the scientificName...
        if (accAuth[tk_idx].substring(0,6).toLowerCase() == 'subsp.') {
          //parse sub-species from name and move token count forward to author
          val.taxonRank = 'subspecies'; //taxonRank
          val.parentNameUsageId = gbf[19]; //parentNameUsageId is species
          val.infraspecificEpithet = accAuth[3]; //infraspecificEpithet
          tk_idx = 4; //this is where any author's name will be
        } else if (accAuth[tk_idx].substring(0,4).toLowerCase() == 'var.') {
          //parse variety from name and move token count forward to author
          val.taxonRank = 'variety'; //taxonRank
          val.parentNameUsageId = gbf[19]; //parentNameUsageId is species
          val.infraspecificEpithet = accAuth[3]; //infraspecificEpithet
          tk_idx = 4; //this is where any author's name will be
        }
        if (accAuth[tk_idx]) { //check again for a token at a possibly new offset
          author = accAuth[tk_idx];
          for (var i=tk_idx+1; i<accAuth.length; i++) {
            author += " " + accAuth[i];
          }
          author = author.replace('"','');
        }
      }
      if (author) {val.scientificNameAuthorship = author;}

      if (valKingdoms.includes(val.kingdom.toLowerCase())) {
        insertValTaxon(idx, val).then(res => {}).catch(err => {
          insertValReject(err.idx, err.val, err.message).catch(err =>{
            //console.log(err);
          });
        });
      } else { //Don't include these. Bacteria, Viruses, ...
        insertValReject(idx, val, 'Excluded Kingdom').catch(err =>{
          //console.log(err);
        });
      }

      idx++;
  });

  fRead.on('close', function() {
    console.log('Read file closed.');
  });

}).catch(err => { //getColumns catch
  console.log(err);
}); //end of getColumns call

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return new Promise((resolve, reject) => {
    pgUtil.getColumns("val_species", staticColumns) //run it once on init: to create the array here. also diplays on console.
      .then(res => {
          staticColumns.push(`rejectReason`); //Add this so both tables can insert all values
          //console.log('val_species columns:', staticColumns);
          resolve(res);
      })
      .catch(err => {
          console.log(`04_gbif_to_val_db.getColumns | error: `, err.message);
          reject(err);
      });
  });
}

async function insertValTaxon(idx, val={}) {
  var queryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
  text = `insert into val_species (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
  //console.log(idx, text, queryColumns.values);
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text, queryColumns.values);
      resolve(res);
    } catch (err) {
      console.log(idx , 'insertValTaxon', err.message, err.detail);
      err.val = Object.assign({}, val); //need to return incoming object to caller - attach to err object
      err.idx = idx; //need to return incoming index to caller - attach to err object
      reject(err);
    }
  });
}

async function insertValReject(idx, val={}, reason='missing') {
  val.rejectReason = reason;
  var queryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
  text = `insert into val_reject (${queryColumns.named}) values (${queryColumns.numbered}) returning "taxonId"`;
  return new Promise(async (resolve, reject) => {
    try {
      var res = await query(text, queryColumns.values);
      resolve(res);
    } catch (err) {
      console.log(idx, 'insertValReject', err.message, err.detail);
      err.val = Object.assign({}, val); //need to return incoming object to caller - attach to err object
      err.idx = idx; //need to return incoming index to caller - attach to err object
      reject(err);
    }
  });
}

function logArrayToConsole(idx, arr, txt='VAL') {
  var log = '';
  for (var i=0; i<arr.length; i++) {
    log += `${i}:${arr[i]}|`;
  }
  console.log(idx, txt, log);
  return log;
}
