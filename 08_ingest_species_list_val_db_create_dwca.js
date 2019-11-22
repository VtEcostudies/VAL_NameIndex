/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 08_ingest_species_list_val_db_create_dwca.js

  Purpose: Incorporate new VT Species Registry checklist files into the val_species
  database, create a corrected taxon file for a future DwCA checklist to be
  published on our IPT, fill-in any missing higher-order taxa needed to support
  these new species in the ALA nameindexer, and create log files to keep an
  account of what was done.

  Specifics:

  Kent has a mosquito species file, and some more files. He wants to:

  1. include these species in val_species table of val_db with proper taxonIds, etc.
  2. produce a DwCA taxon.txt file with proper taxonIds, etc. for publishing on our IPT

  To do that we will:

  1. open the file and parse rows into object having key:value pairs for all data
  2. using scientificName w/o author lookup/find that taxon on GBIF API:
    a. http://api.gbif.org/v1/species/match?name=Aedes%20cinereus
    b. http://api.gbif.org/v1/species?name=Aedes%20cinereus
  3. (a) looks better. if result has matchType=='EXACT', then we use it. if matchType=='FUZZY',
  not sure what to do.
  4. We use the /match API's 'usageKey' as gbifId and taxonId. Missing from this API result is authorship. Not sure why.
  5. Keep track of taxonId values for higher-order taxa and output to file for processing later.

*/

//https://nodejs.org/api/readline.html
var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const csvFileTo2DArray = require('./99_parse_csv_to_array').csvFileTo2DArray;

const inputFileDelim = ",";
const outputFileDelim = ",";

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var baseName = 'spidersVTlist';
baseName = 'WhirligigBeetles';
baseName = 'BombusChecklist';
var subDir = 'dwca-checklist-crickets_katydids-vt-v1.4/'; // - INCLUDING TRAILING SLASH
subDir = 'dwca-checklist_vermont_mammals-v1.2/';
subDir = baseName + '/';
var inputFileName = 'taxon.txt'; //this for incoming dwca
inputFileName = baseName + '.csv';
var outputFileName = 'val_' + inputFileName;
var logFileName = 'log_' + moment().format('YYYYMMDD-HHMMSS') + '_' + inputFileName;
var headRow = true;
var rowCount = 0; //count records available
var outCount = 0; //count records completed so far
var insCount = 0; //count records inserted
var updCount = 0; //count records updated
var wStream = []; //array of write streams

var dbInsert = 0;
var dbUpdate = 0;

var taxonIdObj = {}; //an object with keys for all taxonIds referenced here

getColumns()
  .then(col => {
    getSpeciesFile(dataDir+subDir+inputFileName)
      .then(src => {
        console.log('Input file rowCount:', src.rowCount, 'Header:', src.header);
        rowCount = src.rows.length;
        for (var i=0; i<src.rows.length; i++) {
          matchGbifSpecies(src.rows[i], i)
            .then((gbf) => {
              //we receive the source row back from matchGbifSpecies.then(), attached as a sub-object to gbf
              var val = convertGbifToVal(gbf, gbf.src);
              writeResultToFile(val);
              buildTaxonIdArr(val, gbf.idx);
              if (dbInsert) {
                insertValTaxon(val, gbf.idx)
                  .then(ins => {
                    insCount++;
                    log(`${ins.idx} | insertValTaxon SUCCESS | gbifId:${ins.val.taxonId} | Inserted:${insCount}`);
                  })
                  .catch(err => {
                    //console.log(err.idx, 'insertValTaxon ERROR | gbifId:', err.val?err.val.taxonId:'err.val is empty', '|', err.message);
                    log(`${err.idx} | insertValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`);
                    if (dbUpdate) {
                      updateValTaxon(err.val, err.idx)
                        .then(upd => {
                          updCount++;
                          log(`${upd.idx} | updateValTaxon SUCCESS | gbifId:${upd.val.taxonId} | Updated:${updCount}`);
                        })
                        .catch(err => {
                          log(`${err.idx} | updateValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`);
                        });
                    } //end if (dbUpdate)
                  });
              } //end if (dbInsert)
            })
            .catch(err => {
              log(`matchGbifSpecies ERROR | ${err}`);
            })
        } //end for loop
      })
      .catch(err => {
        console.log(`getSpeciesFile ERROR | ${err}`);
      });
  })
  .catch(err => {
    console.log(`getColumns ERROR | ${err}`);
  })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_species", staticColumns);
}

async function getSpeciesFile(inputFileName) {
  try {
    return await csvFileTo2DArray(inputFileName, inputFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
Search for a species on the GBIF API using the fuzzy match api endpoint.

This API endpoint returns a single match with a confidence metric if a match
is found.

If a species match is not found, it may return the GENUS for the request. This
probably indicates an error in the incoming taxa, so we flag these events as
errors.

Fields returned from this endpoint are different from the raw /species output.

We trasp errors in convertGbifToVal where we compare incoming scientificName to
GBIF canonicalName.
*/
function matchGbifSpecies(src, idx) {
  var name = src.scientificName.trim();

  //console.log('matchGbifSpecies | cleaned: ', name);

  var parms = {
    url: `http://api.gbif.org/v1/species/match?name=${name}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        reject(err);
      } else {
        log(`matchGbifSpecies(${src.scientificName}) | ${res.statusCode}`);
        body.src = src; //attach incoming source row-object to returned object for downstream use
        body.idx = idx; //attach incoming row index to returned object for downstream use
        resolve(body);
      }
    });
  });
}

/*
Insert the fixed-up val database object. On error, return the val object for
downstream processing.
*/
async function insertValTaxon(val, idx) {

  var qryColumns = pgUtil.parseColumns(val, 1, [], staticColumns);
  var sql_insert = `insert into val_species (${qryColumns.named}) values (${qryColumns.numbered}) returning "taxonId"`;
  //console.log(sql_insert, qryColumns.values);
  return new Promise((resolve, reject) => {
    query(sql_insert, qryColumns.values)
      .then(res => {
        res.val = val;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        err.val = val;
        err.idx = idx;
        reject(err);
      })
  })
}

/*
Update the fixed-up val database object. On error, return the val object for
downstream processing.
*/
async function updateValTaxon(val, idx) {

  var qryColumns = pgUtil.parseColumns(val, 2, [val.gbifId], staticColumns);
  var sql_update = `update val_species set (${qryColumns.named}) = (${qryColumns.numbered}) where "gbifId"=$1`;
  //console.log(sql_update, qryColumns.values);
  return new Promise((resolve, reject) => {
    query(sql_update, qryColumns.values)
      .then(res => {
        res.val = val;
        res.idx = idx;
        resolve(res);
      })
      .catch(err => {
        err.val = val;
        err.idx = idx;
        reject(err);
      })
  })
}

/*
Translate GBIF taxon data to VAL taxon data for insert/update into database and
output to file.

The format of the incoming data depends upon which GBIF API endpoint was used.
We attempt to handle at least 2 here.

inputs:

gbif - object returned from GBIF species query
scr - object from source input row
*/
function convertGbifToVal(gbif, src) {
  //begin by copying the incoming file's values to our resulting values
  var val = {}; //Object.assign({}, src);
  if (src.id) {val.id=src.id;} //spit this back out for incoming DwCA that use it to map val_taxon.txt to other incoming dwca extensions

  //translate gbif api values to val columns
  gbif.key=gbif.key?gbif.key:gbif.usageKey;
  src.canonicalName = parseCanonicalName(src.scientificName, src.taxonRank.trim() || undefined);

  //console.log(`taxonId = ${gbif.key} | scientificName = ${gbif.scientificName} | canonicalName = ${gbif.canonicalName}`);

  val.gbifId=gbif.key;
  val.taxonId=gbif.key;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:src.canonicalName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedKey?gbif.acceptedKey:gbif.key;
  val.acceptedNameUsage=gbif.accepted?gbif.accepted:gbif.scientificName;
  val.taxonRank=gbif.rank?gbif.rank.toLowerCase():null;
  val.parentNameUsageId=gbif.parentKey || getParentKeyFromTreeKeys(gbif);

  if (!gbif.authorship && gbif.canonicalName) {
    var authorsub = gbif.scientificName.split(gbif.canonicalName);
    console.log('Split Author from GBIF scientificName:', authorsub);
    val.scientificNameAuthorship = authorsub[1]?authorsub[1]:null;
    val.scientificNameAuthorship = val.scientificNameAuthorship.trim();
  }

  if (gbif.canonicalName) {
    var speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=gbif.rank.toLowerCase()=='species'?speciessub[1]:'';
    val.infraspecificEpithet=gbif.rank.toLowerCase()=='subspecies'?speciessub[2]:'';
    val.infraspecificEpithet=gbif.rank.toLowerCase()=='variety'?speciessub[2]:'';
  }

  //compare the incoming scientificName to the matched scientificName
  //if they're different, then gbif resolved it to a different name
  //T0-DO: also check gbif's taxonomicStatus. the incoming scientificName could
  //have matched to a synonym, or something else...
  if (src.canonicalName == gbif.canonicalName) {
    val.taxonomicStatus = 'accepted';
  } else {
    val.taxonomicStatus = 'invalid';
    log(`INVALID INVALID INVALID | gbifId:${gbif.key} | incoming sciName:${src.scientificName}`);
  }

  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=val.scientificNameAuthorship?val.scientificNameAuthorship:null;
  val.vernacularName=gbif.vernacularName || src.vernacularName || null;
  val.taxonRemarks=gbif.remarks || src.taxonRemarks;
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

  val.datasetName=src.datasetName || null;
  val.datasetId=src.datasetId || null;
  val.bibliographicCitation=src.bibliographicCitation || null;
  val.references=src.references || null;
  val.institutionCode=src.institutionCode || null;
  val.collectionCode=src.collectionCode || null;

  return val;
}

function parseCanonicalName(name, rank='species') {
  var tokens = name.split(" ").slice(); //break name into tokens by spaces
  var endidx = 2; //default last index is 2 - in most cases, scientificName is 2 tokens. not always.
  var canon = '';

  if (tokens[1]) { //if there is a 2nd token...
    if (rank.toLowerCase() == 'species') { //if rank is species, 2nd token is species
      endidx = 2;
    } else if (rank.toLowerCase() == 'subspecies' || rank.toLowerCase() == 'subspecies') {
      endidx = 3;
    } else { //scientificName for genus and higher have only one token.
      endidx = 1;
    }
  }

  for (var i=0; i<endidx; i++) {
    canon += tokens[i] + ' ';
  }

  log(`parseCanonicalName | rank:${rank} | result:${canon}`);

  return canon.trim();
}

function getParentKeyFromTreeKeys(gbif) {
  var parentId = 0;

  //parentNameUsageID is key of next higher rank (except for kingdom, which is itself)
  switch(gbif.rank.toLowerCase()) {
    case 'kingdom': parentId = gbif.kingdomKey; break;
    case 'phylum': parentId = gbif.kingdomKey; break;
    case 'class': parentId = gbif.phylumKey; break;
    case 'order': parentId = gbif.classKey; break;
    case 'family': parentId = gbif.orderKey; break;
    case 'genus': parentId = gbif.familyKey; break;
    case 'species': parentId = gbif.genusKey; break;
    case 'subspecies': parentId = gbif.speciesKey; break;
    case 'variety': parentId = gbif.speciesKey; break;
  }

  return parentId;
}

/*
get array of object keys from val object and write to file header
*/
function writeHeaderToFile(val) {
  var out = '';
  var arr = Object.keys(val); //result is array of keys

  for (i=0; i<arr.length; i++) {
    out += arr[i];
    if (i < (arr.length-1)) out += outputFileDelim;
  }

  wStream[0].write(`${out}\n`);
}

/*
This assumes that the incoming object is one line of data that was parsed into
named fields that are DwCA compliant.
*/
function writeResultToFile(val) {
  var out = '';
  var fld = '';

  if (!wStream[0]) {
    wStream[0] = fs.createWriteStream(`${dataDir}${subDir}${outputFileName}`, {flags: 'w'});
    writeHeaderToFile(val);
  }

  //loop through values. add double quotes if not there
  for (const key in val) {
    fld = val[key] || '';
    if (isNaN(fld)) { //check for null, numeric
      //check for leading and trailing double-quotes
      if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"`) {
        fld = `"${fld}"`;
      }
    }
    out += fld + outputFileDelim;
  }
  //write output to file
  out = out.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter
  log(`writeResultToFile | ${out}`);
  wStream[0].write(`${out}\n`);

}

/*
Process one row of the incoming file. Extract all taxonId keys listed and add
them to a local object that lists all higher-order keys or other refernced
taxonIds.

Create a complete list of gbif taxonIds NOT in the incoming dataset that fill
out the higher order taxonomic tree for those incoming data.
*/
function buildTaxonIdArr(val, idx) {
  outCount++;
  //console.log('buildTaxonIdArr | ', rowCount, outCount, idx);
  log(`${idx} | buildTaxonIdArr | gbifId:${val.taxonId} | rowCount:${rowCount} | outCount:${outCount}`);
  try {
    if (val.taxonId != val.acceptedNameUsageID) {
      taxonIdObj[val.acceptedNameUsageId] = 1;}
    taxonIdObj[val.kingdomId] = 1;
    taxonIdObj[val.phylumId] = 1;
    taxonIdObj[val.classId] = 1;
    taxonIdObj[val.orderId] = 1;
    taxonIdObj[val.familyId] = 1;
    taxonIdObj[val.genusId] = 1;
    if (val.taxonId != val.speciesId) {
      taxonIdObj[val.speciesId] = 1;}
  } catch (err) {
    log(`buildTaxonIdArr | ERROR:${err}`);
  }

  if (outCount >= (rowCount)) { //this is how we detect an end of processing
    writeTaxonIdFile();
    setTimeout(displayStats, 5000); //delay the call to attempt to display it last...
  }
}

/*
Iterate over local taxonIdObj and write it to a file in the same location as the
incoming taxon file.

This file is used later to populate the PostGRES table val_gbif_taxon_id, which
is the list of all taxonIds referenced anywhere within taxon rows.

Subsequent to that step, 06_find_missing_gbif_id_add_to_val_db.js joins two
tables to find missing taxon rows in the database, then attempts to get them
from GBIF and insert them.
*/
function writeTaxonIdFile() {
  if (!wStream[1]) {
    wStream[1] = fs.createWriteStream(`${dataDir}${subDir}missing_taxonIds_${inputFileName}`, {flags: 'w'});
  }

  for (var key in taxonIdObj) {
    wStream[1].write(`${key}\n`);
  }
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|output:${outCount}`);
}

function log(out) {
  console.log(out);
  if (!wStream[2]) {
    wStream[2]=fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
  }
  wStream[2].write(`${out}\n`);
}
