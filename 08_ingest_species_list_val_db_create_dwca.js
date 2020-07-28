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

  To-do 01-27-2020:

  - handle ingestion of 'errors' - defined as species not matched, found or recognized by GBIF at the taxonomicRank
  indicated in the incoming source file. To do this, we are 'forcing' the taxonomic interpretation on the targeted
  user / database / publication; we are claiming we know better than GBIF. To accomplish that in this context, an
  easy method is to flag a specific set of records to manually override the default behavior. To keep track of those
  records in the database, and flag them in the output files, we need a flag. Maybe the easiest is to use nomen-
  claturalCode: if we're imposing the opinion of another system, ITIS for example, we can apply that value. If the
  opinion is ours alone, we might be able to invent our own - it's not clear whether that's allowed.

  Duh - the simplest way to flag NEW records is to set gbifId = 0, which is necessary anyway.
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
const addCanonicalName = require('./97_utilities').addCanonicalName;
const addTaxonRank = require('./97_utilities').addTaxonRank;
const log = require('./97_utilities').log;
const jsonToString = require('./97_utilities').jsonToString;

const inpFileDelim = ",";
const outFileDelim = ",";

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var baseName = paths.baseName; //moved this setting to 00_config.js, as it's used in downstream processing
baseName = 'Moths_Vermont';

var dbInsert = 0;
var dbUpdate = 0;

var subDir = baseName + '/';

if (inpFileDelim == ",") {
  inpFileName = baseName + '.csv';
} else if (inpFileDelim == '\t') {
  inpFileName = baseName + '.txt';
}
//inpFileName = 'fix_' + inpFileName; //use this to handle small update files in the same directory
var outFileName = 'val_' + inpFileName;
var logFileName = 'log_' + moment().format('YYYYMMDD-HHMMSSS') + '_' + inpFileName;
var errFileName = 'err_' + inpFileName;

//Don't create outStream here. An empty outStream var flags the writing header to file below.
var outStream = null;
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`);

var headRow = true;
var rowCount = 0; //count records available
var outCount = 0; //count records completed so far
var insCount = 0; //count records inserted
var updCount = 0; //count records updated
var notCount = 0; //count records NOT found
var errCount = 0; //count record errors

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

getColumns()
  .then(col => {
    getSpeciesFile(dataDir+subDir+inpFileName)
      .then(async src => {
        log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
        rowCount = src.rows.length;
        for (var i=0; i<src.rows.length; i++) {
          await addCanonicalName(src.rows[i], logStream); //parse scientificName into canonicalName and add to src object
          await matchGbifSpecies(src.rows[i], i)
          //matchGbifSpecies(src.rows[i], i)
            .then(async (gbf) => {
              var rankMatch = false;
              if (gbf.results) {
                /*
                  Hmm. we MUST ignore match api results if taxonRank from the source file and GBIF are
                  not the same. But if the source file does not specify taxonRank, we have a problem.
                  The solution: when taxonRank is missing, we must derive it from scientificName.
                */
                if (!gbf.src.taxonRank) {await addTaxonRank(gbf.src, logStream);} //parse canonicalName to derive rank
                /*
                  A 'successful' match returns a single object or empty. If empty, we do not get an error.
                  If the match returned an object, check that the taxonRank is the same. If it is not,
                  then the match was not successful - proceed to findGbifSpecies.
                */
                rankMatch = gbf.rank.toLowerCase()==gbf.src.taxonRank.toLowerCase();
                if (!rankMatch) {
                  log(`${gbf.idx} | taxonRank mismatch|source:${gbf.src.taxonRank.toLowerCase()}|gbif:${gbf.rank.toLowerCase()}`, logStream, true);
                }
              }
              if (gbf.results && rankMatch) { //found a match - EXACT or FUZZY?
                await getGbifSpecies(gbf, gbf.src, gbf.idx)
                //getGbifSpecies(gbf, gbf.src, gbf.idx)
                  .then((res) => {processResults(res, res.src);})
                  .catch((err) => {
                    log(`${err.idx} | getGbifSpecies ERROR | ${err.src.scientificName} | ${JSON.stringify(err)}`, logStream, true);
                    //log(err.src.scientificName, errStream);
                    logErr(jsonToString(err.src, outFileDelim, errStream), errStream);
                  });
              } else { //empty or incorrect result - try to find the source object another way
                await findGbifSpecies(gbf.src, gbf.idx)
                //findGbifSpecies(gbf.src, gbf.idx)
                  .then((res) => {
                      processResults(res.self, res.src);
                  })
                  .catch(err => {
                    log(`${err.idx} | findGbifSpecies ERROR | ${err.src.scientificName} | ${JSON.stringify(err)}`, logStream, true);
                    //log(err.src.scientificName, errStream);
                    logErr(jsonToString(err.src, outFileDelim, errStream), errStream, true);
                  })
                } //end else
              })
            .catch((err) => {
              log(`${err.idx} | matchGbifSpecies ERROR | ${err.src.scientificName} | ${JSON.stringify(err)}`, logStream, true);
              //log(err.src.scientificName, errStream);
              logErr(jsonToString(err.src, outFileDelim, errStream), errStream);
            });
        } //end for loop
      })
      .catch(err => {
        log(`getSpeciesFile ERROR | ${JSON.stringify(err)}`, logStream, true);
      });
  })
  .catch(err => {
    log(`getColumns ERROR | ${JSON.stringify(err)}`, logStream, true);
  })

function getColumns() {
  //file scope list of target table columns retrieved on startup
  return pgUtil.getColumns("val_species", staticColumns);
}

/*
Parse the input file into a 2D array for processing.
*/
async function getSpeciesFile(inpFileName) {
  try {
    return await csvFileTo2DArray(inpFileName, inpFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
If a match was made, or a species was found, produce the output.
*/
function processResults(gbf, src) {
  try {

    log(`gbifToValIngest | gbifId:${gbf.key} | GBIF scientificName:${gbf.scientificName} | GBIF canonicalName:${gbf.canonicalName} | GBIF rank:${gbf.rank}`, logStream);

    var val = gbifToValIngest(gbf, src);
    writeResultToFile(val);
    if (dbInsert) {
      insertValTaxon(val, gbf.idx)
        .then(ins => {
          insCount++;
          log(`${ins.idx} | insertValTaxon SUCCESS | gbifId:${ins.val.taxonId} | Inserted:${insCount}`, logStream, true);
        })
        .catch(err => {
          //console.log(err.idx, 'insertValTaxon ERROR | gbifId:', err.val?err.val.taxonId:'err.val is empty', '|', err.message);
          log(`${err.idx} | insertValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, logStream);
          if (dbUpdate) {
            updateValTaxon(err.val, err.idx)
              .then(upd => {
                updCount++;
                log(`${upd.idx} | updateValTaxon SUCCESS | gbifId:${upd.val.taxonId} | Updated:${updCount}`, logStream, true);
              })
              .catch(err => {
                log(`${err.idx} | updateValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, logStream, true);
              });
          } //end if (dbUpdate)
        });
    } //end if (dbUpdate && dbInsert)
    if (dbUpdate && !dbInsert) {
      updateValTaxon(val, gbf.idx)
        .then(upd => {
          updCount++;
          log(`${upd.idx} | updateValTaxon SUCCESS | gbifId:${upd.val.taxonId} | Updated:${updCount}`, logStream, true);
        })
        .catch(err => {
          log(`${err.idx} | updateValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, logStream, true);
        });
    } //end if (dbUpdate && !dbInsert)
  } catch (err) {
    log(`processResults ERROR | ${JSON.stringify(err)}`, logStream, true);
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
        log(`matchGbifSpecies | err.code: ${err.code}`, errStream);
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        log(`${idx} | matchGbifSpecies(${src.scientificName}) | ${res.statusCode} | ${body.usageKey?1:0} results found.`, logStream, true);
        /*
        log(`matchGbifSpecies RESULT:
          GBIF RANK:${body.rank.toLowerCase()}
          SOURCE RANK:${src.taxonRank.toLowerCase()}
          COMPARISON:${body.rank.toLowerCase()==src.taxonRank.toLowerCase()}`, logStream);
        */
        body.results = body.usageKey?1:0;
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
  var name = src.canonicalName.trim(); //species api works on canonicalName

  var parms = {
    url: `http://api.gbif.org/v1/species?name=${name}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        log(`findGbifSpecies | err.code: ${err.code}`, errStream);
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        var res = body.results?body.results:[];

        if (res.length == 0) {
          notCount++;
          var err = {message:"No matching entries found."};
          err.src = src;
          err.idx = idx;
          reject(err);
        }
        else {
          log(`${idx} | findGbifSpecies(${src.canonicalName}) | ${res.statusCode} | ${res.length} Results. Searching for best match...`, logStream, true);

          var acpt=-1, self=-1;
          for (var i=0; i<res.length; i++) {
            if (res[i].taxonomicStatus == 'ACCEPTED') {
              if (acpt<0) {acpt=i;}
              if (res[i].rank && !res[acpt].rank) {acpt=i;}
            }
            //select the gbif raw match to supplied taxon as 'self' - 1) sciName is equal, 2) there is a rank listed
            //NOTE: this DOES NOT WORK YET for subspecies! ...because sciName contains var. variety, subsp. subspecies, ...
            //FURTHER NOTE: I don't see how this works with sciName containing author?!
            if (res[i].canonicalName == name) {
              if (self<0) self=i; //this keeps the earliest valid result (prefer res[1] over res[2], all things being equal)
              if (res[i].rank && !res[self].rank) {self=i;} //preferentially swap-in entries with defined RANK
            }
          }
          //if an accepted name or self-match was found, return the first, most valid instance
          //if no accepted name or self-match was found, return undefined for gbif or self
          var ret = {};
          ret.gbif = acpt<0?undefined:res[acpt];
          ret.self = self<0?undefined:res[self];
          log(`findGbifSpecies(${src.canonicalName}) |
            ACCEPTED:${ret.gbif?ret.gbif.key:undefined} - ${ret.gbif?ret.gbif.scientificName:undefined} |
            Self-status:${ret.self?ret.self.taxonomicStatus:undefined}`, logStream, true);
          ret.src = src; //attach incoming source row-object to returned object for downstream use
          ret.idx = idx; //attach incoming row index to returned object for downstream use
          resolve(ret);
        }
      }
    });
  });
}

/*

*/
function getGbifSpecies(gbf, src, idx) {
  var key = gbf.usageKey?gbf.usageKey:gbf.key;

  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        log(`getGbifSpecies|err.code: ${err.code}`, errStream);
        err.gbf = gbf;
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        log(`${idx} | getGbifSpecies(${gbf.scientificName}) | ${res.statusCode} | gbifKey:${body.key?key:undefined}`, logStream, true);
        body.gbf = gbf;
        body.src = src; //attach incoming gbif row-object to returned object for downstream use
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
    if (i < (arr.length-1)) out += outFileDelim;
  }

  outStream.write(`${out}\n`);
}

/*
This assumes that the incoming object is one line of data that was parsed into
named fields that are DwCA compliant.
*/
function writeResultToFile(val) {
  var out = '';
  var fld = '';

  if (!outStream) {
    outStream = fs.createWriteStream(`${dataDir}${subDir}${outFileName}`, {flags: 'w', encoding: 'utf8'});
    writeHeaderToFile(val);
  }

  //loop through values. add double quotes if not there
  for (const key in val) {
    //fld = val[key] || '';
    fld = val[key]!=null?val[key]:''; //this is not tested on a large set of data
    if (isNaN(fld)) { //check for null, numeric
      //check for leading and trailing double-quotes
      if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"`) {
        fld = `"${fld}"`;
      }
    }
    out += fld + outFileDelim;
  }
  //write output to file
  out = out.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter
  log(`writeResultToFile | ${out}`, logStream);
  outStream.write(`${out}\n`);
  outCount++;
}

function logErr(obj, stream=null, override=true) {
  try {
    if (errCount == 0) {
      log(obj.columns, stream);
    }
    errCount++;
    log(obj.values, stream, override);
  } catch(err) {
    console.log('logErr ERROR', err);
  }
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|output:${outCount}|not-found:${notCount}|errors:${errCount}`, logStream, true);
}
