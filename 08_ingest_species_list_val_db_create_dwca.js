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
  5. (Removed) Keep track of taxonId values for higher-order taxa and output to file for processing later.
  (Step 5 was moved to a separate file.)

  To-do 01-16-2020:

  - use logErr to put errors into separate file for post-processing
  - if that works, remove code to read processed data (the other attempt to deal with errors)

*/

const logHand = require('why-is-node-running');
const fs = require('fs');
const readline = require('readline');
const Request = require('request');
const moment = require('moment');
const paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const csvFileTo2DArray = require('./99_parse_csv_to_array').csvFileTo2DArray;
const gbifToValIngest = require('./98_gbif_to_val_columns').gbifToValIngest;

const inputFileDelim = ",";
const outputFileDelim = ",";

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var baseName = '';
//baseName = 'spidersVTlist';
//baseName = 'WhirligigBeetles';
//baseName = 'BombusChecklist';
//baseName= 'CicadaVermont';
//baseName= 'Orthoptera_Vermont';
//baseName= 'Ticks_Vermont';
//baseName= 'Spiders_Vermont';
//baseName= 'Amphibian_Reptile_Vermont';
//baseName = 'Robber_Flies_Vermont';
//baseName = 'Butterflies_Vermont';
//baseName = 'Crayfish_Vermont';
//baseName = 'Dragonflies_Damselflies_Vermont';
//baseName = 'Fish_Vermont';
//baseName = 'Freshwater_Mussels_Vermont';
//baseName = 'Plants_Vermont';
//baseName = 'Syrphids_Vermont';
//baseName = 'Error_Corrections';
//baseName = 'Springtails_VT';
//baseName = 'Bryophytes_VT';
baseName = 'Vermont_Conservation_Missing'; //the not-found taxa from adding Vermont_Conservation_Status: 1300 rows!

//var subDir = 'dwca-checklist-crickets_katydids-vt-v1.4/'; // - INCLUDING TRAILING SLASH
//subDir = 'dwca-checklist_vermont_mammals-v1.2/';
subDir = baseName + '/';
//var inputFileName = 'taxon.txt'; //this for incoming dwca that was submitted via IPT
if (inputFileDelim == ",") {
  inputFileName = baseName + '.csv';
} else if (inputFileDelim == '\t') {
  inputFileName = baseName + '.txt';
}
var outputFileName = 'val_' + inputFileName;
var logFileName = 'log_' + moment().format('YYYYMMDD-HHMMSS') + '_' + inputFileName;
var processedFileName = 'proc_' + inputFileName;
var errFileName = 'err_' + inputFileName;
var headRow = true;
var rowCount = 0; //count records available
var outCount = 0; //count records completed so far
var insCount = 0; //count records inserted
var updCount = 0; //count records updated
var errCount = 0; //count record errors
var wStream = {}; //array of write streams

var dbInsert = 1;
var dbUpdate = 1;

var taxonIdObj = {}; //an object with keys for all taxonIds referenced here
var rowsToSkip = {}; //an object of source rows already processed - read from the processed file

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

async function getRowsToSkip() {
  return await csvFileTo2DArray(dataDir+subDir+processedFileName);
}

function assignRowsToSkip(index, row) {rowsToSkip[row] = index;}

fs.access(dataDir+subDir+processedFileName, fs.constants.F_OK, (err) => {
  if (err) {
      console.log(`getRowsToSkip - ${dataDir+subDir+processedFileName} does not exist`)
      return;
  }

  getRowsToSkip().then(async (res) => {
    for (var i=0; i<res.rows.length; i++) {
      await assignRowsToSkip(i, res.rows[i]); //this fucking works.
    }
    console.log('getRowsToSkip:', rowsToSkip);
    //process.exit(99);
  }).catch((err) => {console.log('getRowsToSkip ERROR', err.message);});

})

getColumns()
  .then(col => {
    getSpeciesFile(dataDir+subDir+inputFileName)
      .then(async src => {
        log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
        rowCount = src.rows.length;
        for (var i=0; i<src.rows.length; i++) {
          //this can't work yet - have to change result file write mode to 'append' when we are picking-up
          //from a dropped processing session.
          //if (rowsToSkip[src.rows[i]]) {console.log(`(would if we could) SKIPPING source row ${i}`);} //continue;}
          await matchGbifSpecies(src.rows[i], i)
          //matchGbifSpecies(src.rows[i], i)
            .then(async (gbf) => {
              //rankmatch is true if they match, or if the incoming dataSet did not specify...
              var rankMatch = gbf.src.taxonRank?(gbf.rank.toLowerCase()==gbf.src.taxonRank.toLowerCase()):true;
              //A successful match returns a single object or empty. if empty, we do not get an error.
              //if the match returned an object, check that the taxonRank is the same. If it is not,
              //then the match was not successful - proceed to findGbifSpecies.
              if (gbf.usageKey && rankMatch) { //found a match - EXACT or FUZZY?
                await getGbifSpecies(gbf, gbf.src, gbf.idx)
                //getGbifSpecies(gbf, gbf.src, gbf.idx)
                  .then((res) => {processResults(res, res.src);})
                  .catch((err) => {
                    log(`getGbifSpecies ERROR | ${err.src.scientificName} | ${err}`);
                    logErr(err.src.scientificName);
                  });
              } else { //empty or incorrect result - try to find the source object another way
                await findGbifSpecies(gbf.src, gbf.idx)
                //findGbifSpecies(gbf.src, gbf.idx)
                  .then((res) => {
                      processResults(res.self, res.src);
                  })
                  .catch(err => {
                    log(`findGbifSpecies ERROR | ${err.src.scientificName} | ${err}`);
                    logErr(err.src.scientificName)
                  })
                } //end else
              })
            .catch((err) => {
              log(`matchGbifSpecies ERROR | ${err.src.scientificName} | ${err}`);
              logErr(err.src.scientificName)
            });
        } //end for loop
      })
      .catch(err => {
        log(`getSpeciesFile ERROR | ${err}`);
      });
  })
  .catch(err => {
    log(`getColumns ERROR | ${err}`);
  })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_species", staticColumns);
}

/*
Parse the input file into a 2D array for processing.
*/
async function getSpeciesFile(inputFileName) {
  try {
    return await csvFileTo2DArray(inputFileName, inputFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
If a match was made, or a species was found, produce the output.
*/
function processResults(gbf, src) {
  try {

    log(`gbifToValIngest |
      gbifId:${gbf.key} | GBIF scientificName:${gbf.scientificName} | GBIF canonicalName:${gbf.canonicalName} | GBIF rank:${gbf.rank}`);

    var val = gbifToValIngest(gbf, src);
    writeResultToFile(val);
    writeProcessedToFile(src); //make a file of source rows processed
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
    } //end if (dbUpdate && dbInsert)
    if (dbUpdate && !dbInsert) {
      updateValTaxon(val, gbf.idx)
        .then(upd => {
          updCount++;
          log(`${upd.idx} | updateValTaxon SUCCESS | gbifId:${upd.val.taxonId} | Updated:${updCount}`);
        })
        .catch(err => {
          log(`${err.idx} | updateValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`);
        });
    } //end if (dbUpdate && !dbInsert)
  } catch (err) {
    log(`processResults ERROR | ${err}`);
  } //end try(processResults)
}

/*
Search for a species on the GBIF API using the fuzzy match api endpoint.

This API endpoint returns a single match with a confidence metric if a match
is found.

If a species match is not found, it may return the GENUS for the request. This
might indicate an error in the incoming taxa, or it might indicate an unrecognized
synonym, or something else.

Fields returned from this endpoint are different from the raw /species output.

We trasp errors in gbifAcceptedToVal where we compare incoming scientificName to
GBIF canonicalName.
*/
function matchGbifSpecies(src, idx) {
  var name = src.scientificName.trim();

  var parms = {
    url: `http://api.gbif.org/v1/species/match?name=${name}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        console.log('matchGbifSpecies|err.code:', err.code);
        src.err = {"code":err.code, "func":"matchGbifSpecies"};
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        log(`matchGbifSpecies(${src.scientificName}) | ${res.statusCode} | ${body.usageKey?1:0} results found.`);
        /*
        log(`matchGbifSpecies RESULT:
          GBIF RANK:${body.rank.toLowerCase()}
          SOURCE RANK:${src.taxonRank.toLowerCase()}
          COMPARISON:${body.rank.toLowerCase()==src.taxonRank.toLowerCase()}
          `);
        */
        body.src = src; //attach incoming source row-object to returned object for downstream use
        body.idx = idx; //attach incoming row index to returned object for downstream use
        resolve(body);
      }
    });
  });
}

/*
This endpoint returns an array of matches. We traverse that array, looking for
a best match, initially defined as:

ret.gbif:
  - the first array element having taxonomicStatus==ACCEPTED, if one exists
  overriding that:
  - the first array element having taxonomicStatus==ACCEPTED and defined rank, if one exists

ret.self:
  - the first array element where GBIF:scientificName==SOURCE:scientificName, if one exists
  overriding that:
  - the first array element where GBIF:scientificName==SOURCE:scientificName and defined rank, if one exists

*/
function findGbifSpecies(src, idx) {
  var name = src.scientificName.trim();

  var parms = {
    url: `http://api.gbif.org/v1/species?name=${name}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        var res = body.results?body.results:[];
        if (res.length == 0) {
          var err = {"message":"No matching entries found."};
          err.src = src;
          err.idx = idx;
          reject(err);
        }

        log(`findGbifSpecies(${src.scientificName}) | ${res.statusCode} | ${res.length} Results. Searching for best match...`);

        var acpt=-1, self=-1;
        for (var i=0; i<res.length; i++) {
          if (res[i].taxonomicStatus == 'ACCEPTED') {
            if (acpt<0) {acpt=i;}
            if (res[i].rank && !res[acpt].rank) {acpt=i;}
          }
          //select the gbif raw match to supplied taxon as 'self' - 1) sciName is equal, 2) there is a rank listed
          //NOTE: this DOES NOT WORK YET for subspecies! ...because sciName contains var. variety, subsp. subspecies, ...
          //FURTHER NOTE: I don't see how this works with sciName containing author?!
          if (res[i].scientificName == name) {
            if (self<0) self=i; //this keeps the earliest valid result (prefer res[1] over res[2], all things being equal)
            if (res[i].rank && !res[self].rank) {self=i;} //preferentially swap-in entries with defined RANK
          }
        }
        //if an accepted name or self-match was found, return the first, most valid instance
        //if no accepted name or self-match was found, return undefined for gbif or self
        var ret = {};
        ret.gbif = acpt<0?undefined:res[acpt];
        ret.self = self<0?undefined:res[self];
        log(`findGbifSpecies(${src.scientificName}) |
          ACCEPTED:${ret.gbif?ret.gbif.key:undefined} - ${ret.gbif?ret.gbif.scientificName:undefined} |
          Self-status:${ret.self?ret.self.taxonomicStatus:undefined}`);
        ret.src = src; //attach incoming source row-object to returned object for downstream use
        ret.idx = idx; //attach incoming row index to returned object for downstream use
        resolve(ret);
      }
    });
  });
}

function getGbifSpecies(gbf, src, idx) {
  var key = gbf.usageKey?gbf.usageKey:gbf.key;

  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        console.log('getGbifSpecies|err.code:', err.code);
        src.err = {"code":err.code, "func":"getGbifSpecies"};
        err.gbf = gbf;
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        log(`getGbifSpecies(${gbf.scientificName}) | ${res.statusCode} | gbifKey:${body.key?key:undefined}`);
        body.gbf = gbf;
        body.src = src; //attach incoming gbif row-object to returned object for downstream use
        body.idx = idx; //attach incoming row index to returned object for downstream use
        resolve(body);
      }
    });
  });
}


/*
Attempt to parse canonicalName from incoming name. If we don't have rank, this is
not feasible. In that case, return trimmed name.
*/
function parseCanonicalName(name, rank=undefined) {

  if (!rank) return name.trim();

  var tokens = name.split(" ").slice(); //break name into tokens by spaces
  var endidx = 2; //default last index is 2 - in most cases, scientificName is 2 tokens. not always.
  var canon = '';

  if (tokens[1]) { //if there is a 2nd token...
    if (rank.toLowerCase() == 'species') { //if rank is species, 2nd token is species
      endidx = 2;
    } else if (rank.toLowerCase() == 'subspecies' || rank.toLowerCase() == 'variety') {
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
get array of object keys from val object and write to file header
*/
function writeHeaderToFile(val) {
  var out = '';
  var arr = Object.keys(val); //result is array of keys

  for (i=0; i<arr.length; i++) {
    out += arr[i];
    if (i < (arr.length-1)) out += outputFileDelim;
  }

  wStream['result'].write(`${out}\n`);
}

/*
This assumes that the incoming object is one line of data that was parsed into
named fields that are DwCA compliant.
*/
function writeResultToFile(val) {
  var out = '';
  var fld = '';

  if (!wStream['result']) {
    wStream['result'] = fs.createWriteStream(`${dataDir}${subDir}${outputFileName}`, {flags: 'w', encoding: 'utf8'});
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
  wStream['result'].write(`${out}\n`);
  outCount++;
}

function writeProcessedToFile(src) {
  var out = '';
  var fld = '';

  if (!wStream['processed']) {
    wStream['processed'] = fs.createWriteStream(`${dataDir}${subDir}${processedFileName}`, {flags: 'w', encoding: 'utf8'});
  }

  for (const key in src) {
    fld = src[key] || '';
    if (isNaN(fld)) { //check for null, numeric
      //check for leading and trailing double-quotes
      if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"`) {
        fld = `"${fld}"`;
      }
    }
    out += fld + outputFileDelim;
  }
  out = out.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter
  wStream['processed'].write(`${out}\n`);
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|output:${outCount}|errors:${errCount}`);
}

function log(out) {
  console.log(out);
  if (!wStream['log']) {
    wStream['log']=fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
  }
  wStream['log'].write(`${out}\n`);
}

async function logErr(txt) {
  errCount++;
  try {
    console.log(`Added to Error File: ${txt}`);
    if (!wStream['err']) {
      wStream['err'] = await fs.createWriteStream(`${dataDir}${subDir}${errFileName}`);
    }
    wStream['err'].write(txt + '\n');
  } catch(error) {
    throw error;
  }
}
