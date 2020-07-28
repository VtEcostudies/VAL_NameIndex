/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 09_ingest_species_list_new_not_found.js

  Purpose: Incorporate new VT Species Registry checklist files into the val_species
  database that were NOT FOUND in GBIF. Also produce an output DwCA file fit to be
  published on our IPT.

  NOTE: this ingests files with the prefix 'new_', which should just be the output of
  files with the prefix 'err_' from the standard ingestion process.

  ALSO: Create a corrected taxon file for a future DwCA checklist to be
  published on our IPT, fill-in any missing higher-order taxa needed to support
  these new species in the ALA nameindexer, and create log files to keep an
  account of what was done.

  This ingestion tool is different from 08, which only ingests species that match
  in GBIF.

  Ingest those errors from a file that contains well-researched recent taxon
  changes or developments, and 'force' their ingestion.

  Specifics:

  1. include these species in val_species table of val_db with proper taxonIds, etc.
  2. produce a DwCA output file with proper taxonIds, etc. for publishing on our IPT

  To do that we will:

  1. open the file and parse rows into object having key:value pairs for all data


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
const addCanonicalName = require('./97_utilities').addCanonicalName;
const addTaxonRank = require('./97_utilities').addTaxonRank;
const log = require('./97_utilities').log;
const initValObject = require('./98_gbif_to_val_columns').initValObject;

const inputFileDelim = ",";
const outputFileDelim = ",";

var staticColumns = [];

console.log(`config paths: ${JSON.stringify(paths)}`);

var dataDir = paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var baseName = paths.baseName;

var dbInsert = 0;
var dbUpdate = 0; //BE CAREFUL - we use our own taxonIds in here. If you dont guarantee unique, you'll overwrite!

var subDir = baseName + '/';
var inpFileName = 'new_' + baseName + '.csv'; //only apply not-found taxa from files prefixed with 'new_'
var outFileName = 'val_' + inpFileName;
var logFileName = 'log_' + moment().format('YYYYMMDD-HHMMSSS') + '_' + inpFileName;
var errFileName = 'err_' + inpFileName;

//Don't create outStream here. An empty outStream is used later to flag the writing of a header to file.
//var outStream = fs.createWriteStream(`${dataDir}${subDir}${outFileName}`);
var outStream = null;
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`);
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`);

var headRow = true;
var rowCount = 0; //count records available
var outCount = 0; //count records completed so far
var insCount = 0; //count records inserted
var updCount = 0; //count records updated
var getCount = 0; //count records selected
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
        //for (var i=0; i<2; i++) {
          if (src.rows[i].action == 'delete') {log(`skipping ${src.rows[i].scientificName}`, logStream, true); continue;}
          await addCanonicalName(src.rows[i], logStream); //parse scientificName into canonicalName and add to src object
          await matchGbifSpecies(src.rows[i], i)
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
                  log(`taxonRank mismatch|source:${gbf.src.taxonRank.toLowerCase()}|gbif:${gbf.rank.toLowerCase()}`, logStream, true);
                }
              } else if (!src.taxonRank) {
                await addTaxonRank(gbf.src, logStream); //parse canonicalName to derive rank
              }
              if (gbf.results && rankMatch) { //found a match - EXACT or FUZZY?
                await getGbifSpecies(gbf, gbf.src, gbf.idx)
                  .then((res) => {processResults(res, res.src);})
                  .catch((err) => {
                    log(`getGbifSpecies ERROR | ${err.src.scientificName} | ${JSON.stringify(err)}`, logStream, true);
                    log(`${err.idx} | getGbifSpecies ERROR | gbifId:${err.src.taxonId} | error:${err.message}`, errStream);
                    logErr(jsonToString(err.src), errStream);
                  });
              } else { //empty or incorrect result - insert it with whatever data we've got
                  processResults(gbf, gbf.src);
                } //end else
              })
            .catch((err) => {
              log(`matchGbifSpecies ERROR | ${err.src.scientificName} | ${JSON.stringify(err)}`, logStream, true);
              log(`${err.idx} | matchGbifSpecies ERROR | gbifId:${err.src.taxonId} | error:${err.message}`, errStream);
              logErr(jsonToString(err.src,outputFileDelim,errStream), errStream);
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

Important: this defines which columns, and in what order are added to the output file.
*/
async function getSpeciesFile(inpFileName) {
  try {
    return await csvFileTo2DArray(inpFileName, inputFileDelim, headRow, true);
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
      gbifId:${gbf.key} | GBIF scientificName:${gbf.scientificName} | GBIF canonicalName:${gbf.canonicalName} | GBIF rank:${gbf.rank}`
    , logStream, true);

    var val = valIngestNew(gbf, src);
    //if this is a trial-run or we're only dbUpdating, write results to output file
    if (!dbInsert) {
      writeResultToFile(val);
    }
    if (dbInsert) {
      insertValTaxon(val, gbf.idx)
        .then(ins => {
          insCount++;
          log(`${ins.idx} | insertValTaxon SUCCESS | taxonId:${ins.res.taxonId} | Inserted:${insCount}`, logStream, true);
          writeResultToFile(ins.val); //insertValTaxon returns values returned from dbInsert returning *.
        })
        .catch(err => {
          if (Number(err.code)==23505) { //23505 duplicate key value violates unique constraint (not primary key)
            if (dbUpdate) {
              updateValTaxon(err.val, err.idx)
                .then(upd => {
                  updCount++;
                  log(`${upd.idx} | updateValTaxon SUCCESS | taxonId:${upd.val.taxonId} | Updated:${updCount}`, logStream, true);
                  writeResultToFile(upd.val); //returns values returned from dbUpdate returning *.
                })
                .catch(err => {
                  log(`${err.idx} | updateValTaxon ERROR | taxonId:${err.val.taxonId} | code:${err.code} | error:${err.message}`, logStream, true);
                  log(`${err.idx} | updateValTaxon ERROR | taxonId:${err.val.taxonId} | code:${err.code} | error:${err.message}`, errStream);
                  logErr(jsonToString(err.val), errStream);
                });
              } else { //not dbUpdate
                getVTRegistryTaxon(err.val, err.idx)
                  .then(get => {
                    getCount++;
                    log(`${get.idx} | getVTRegistryTaxon | taxonId:${get.rows[0].taxonId} | Selected:${getCount}`, logStream, true);
                    writeResultToFile(get.val); //returns val with updated key values from select query
                  })
                  .catch(err => {
                    log(`${err.idx} | getVTRegistryTaxon | sciName:${err.val.scientificName} | code:${err.code} | error:${err.message}`, logStream, true);
                    log(`${err.idx} | getVTRegistryTaxon | sciName:${err.val.scientificName} | code:${err.code} | error:${err.message}`, errStream, true);
                    logErr(jsonToString(err.val), errStream);
                  })
              } //end if (dbUpdate)
          } else { //end if (23505)
            log(`${err.idx} | insertValTaxon ERROR | taxonId:${err.val.taxonId} | code:${err.code} | error:${err.message}`, logStream, true);
            log(`${err.idx} | insertValTaxon ERROR | taxonId:${err.val.taxonId} | code:${err.code} | error:${err.message}`, errStream);
            logErr(jsonToString(err.val), errStream);
          }
        });
    } //end if (dbInsert)
    if (dbUpdate && !dbInsert) {
      updateValTaxon(val, gbf.idx)
        .then(upd => {
          updCount++;
          log(`${upd.idx} | updateValTaxon SUCCESS | gbifId:${upd.val.taxonId} | Updated:${updCount}`, logStream, true);
          writeResultToFile(upd.val); //returns values returned from dbUpdate returning *.
        })
        .catch(err => {
          log(`${err.idx} | updateValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, logStream, true);
          log(`${err.idx} | updateValTaxon ERROR | gbifId:${err.val.taxonId} | error:${err.message}`, errStream);
          logErr(jsonToString(err.val), errStream);
        });
    } //end if (dbUpdate && !dbInsert)
  } catch (err) {
    console.log('processResults', err);
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
        src.err = {"code":err.code, "func":"matchGbifSpecies"};
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        log(`matchGbifSpecies(${src.scientificName}) | ${res.statusCode} | ${body.usageKey?1:0} results found.`, logStream, true);
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
        src.err = {"code":err.code, "func":"getGbifSpecies"};
        err.gbf = gbf;
        err.src = src;
        err.idx = idx;
        reject(err);
      } else {
        log(`getGbifSpecies(${gbf.scientificName}) | ${res.statusCode} | gbifKey:${body.key?key:undefined}`, logStream, true);
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
  var sql_insert = `insert into val_species (${qryColumns.named}) values (${qryColumns.numbered}) returning *`;

  //console.log('insertValTaxon', sql_insert, qryColumns.values);

  return new Promise((resolve, reject) => {
    query(sql_insert, qryColumns.values)
      .then(res => {
        //console.log('insertValTaxon | res:', res.rows[0]);
        var ret = {};
        ret.res = res.rows[0];
        val.taxonId=ret.res.taxonId;
        val.taxonomicStatus=ret.res.taxonomicStatus;
        val.acceptedNameUsage=ret.res.acceptedNameUsage;
        val.acceptedNameUsageId=ret.res.acceptedNameUsageId;
        val.species=ret.res.species;
        val.speciesId=ret.res.speciesId;
        ret.val = val;
        ret.idx = idx;
        resolve(ret);
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

NOTE: update of non-existent rows returns SUCCESS. we should filter those and
return them as ERRORS.
*/
async function updateValTaxon(val, idx) {

  var qryColumns = pgUtil.parseColumns(val, 2, [val.taxonId], staticColumns);
  var sql_update = `update val_species set (${qryColumns.named}) = (${qryColumns.numbered}) where "taxonId"=$1 returning *`;

  //console.log('updateValTaxon', sql_update, qryColumns.values);

  return new Promise((resolve, reject) => {
    query(sql_update, qryColumns.values)
      .then(res => {
        console.log('updateValTaxon', res.rows);
        res.val = val;
        res.idx = idx;
        if (res.rows[0]) {
          resolve(res);
        } else {
          res.message = `val_species taxonId(${val.taxonId}) NOT Found`;
          reject(res);
        }
      })
      .catch(err => {
        err.val = val;
        err.idx = idx;
        reject(err);
      })
  })
}

/*
Retrieve a row of data for a scientificName. This is used to fill the DwCA addendum file (to be
appended to val_{filename}.csv) for both newly-inserted taxa, and previously-inserted taxa.

The need for this arose from having to repeat this process of insertion on the same set of errors
in the original species file.
*/
async function getVTRegistryTaxon(val, idx) {

  var sql_select = `select * from val_species where "scientificName"=$1`;

  return new Promise((resolve, reject) => {
    query(sql_select, [val.scientificName])
      .then(res => {
        console.log('getVTRegistryTaxon | Found |', res.rows[0].taxonId, res.rows[0].scientificName);
        res.val = val;
        res.idx = idx;
        if (res.rows[0]) {
          for (key in res.val) {
            res.val[key] = res.rows[0][key];
          }
          resolve(res);
        } else {
          res.message = `val_species scientificName(${val.scientificName}) NOT Found`;
          reject(res);
        }
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
    fld = val[key]!=null?val[key]:'';
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
  log(`writeResultToFile | ${out}`, logStream);
  outStream.write(`${out}\n`);
  outCount++;
}

/*
Convert json object to object with:
  columns: string of column names, separated by outputFileDelim
  values: string of values, separated by outputFileDelim
Returned as object, like {columns:'one,two,three', values:'1,2,3'}
*/
function jsonToString(obj) {
  var vals = ''; //string of column values, delim-separated
  var cols = ''; //string of column names, delim-separated

  //loop through values. add double quotes if not there
  for (const key in obj) {
    fld = obj[key]!=null?obj[key]:'';
    if (isNaN(fld)) { //check for null, numeric
      //check for leading and trailing double-quotes
      if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"`) {
        fld = `"${fld}"`;
      }
    }
    vals += fld + outputFileDelim;
    cols += key + outputFileDelim;
  }
  vals = vals.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter
  cols = cols.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter

  return {values:vals, columns:cols};
}

function logErr(obj, stream=null, override=true) {
  if (errCount == 0) {
    log(obj.columns, stream);
  }
  errCount++;
  log(obj.values, stream, override);
}

function displayStats() {
  log(`total:${rowCount}|inserted:${insCount}|updated:${updCount}|selected:${getCount}|output:${outCount}|not-found:${notCount}|errors:${errCount}`, logStream, true);
}

function valIngestNew(gbif, src) {
  try {

    var val = initValObject();

    val.gbifId=0;
    val.taxonId=src.taxonId || 'VTSR:*'; //this is how we invoke DB trigger to generate a new custom taxonId, and apply it to acceptedNameUsageId
    val.scientificName=src.canonicalName || null; //scientificName often contains author. nameindexer cannot handle that, so remove it.
    val.scientificNameAuthorship=src.scientificNameAuthorship || null;
    val.acceptedNameUsage=src.acceptedNameUsage || (src.taxonomicStatus=='accepted'?src.scientificName:null);
    val.acceptedNameUsageId=src.acceptedNameUsageId || (src.taxonomicStatus=='accepted'?src.taxonId:null);
    val.taxonRank=src.taxonRank?src.taxonRank.toLowerCase():null;
    val.parentNameUsage=src.parentNameUsage || null;
    val.parentNameUsageId=src.parentNameUsageId || null; //can't be null - handle below

    if (src.taxonRank && src.canonicalName) {
      var rank = src.taxonRank;
      var speciessub = src.canonicalName.split(" ").slice(); //break into tokens by spaces
      val.genus = speciessub[0]; //this special for ingesting 'new' - override GBIF
      val.species = speciessub[1]; //this special for ingesting 'new' - override GBIF
      val.specificEpithet=rank=='species'?speciessub[1]:null;
      val.infraspecificEpithet=rank=='subspecies'?speciessub[2]:null;
      val.infraspecificEpithet=rank=='variety'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
    }

    val.nomenclaturalCode=src.nomenclaturalCode || 'VTSR'; //VTSR = VT Species Registry
    val.vernacularName=src.vernacularName || null;
    val.taxonRemarks=src.taxonRemarks || null;
    val.taxonomicStatus=src.taxonomicStatus?src.taxonomicStatus.toLowerCase():null;

    val.kingdom=gbif.kingdom?gbif.kingdom:src.kingdom;
    val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:src.kingdomId;;
    val.phylum=gbif.phylum?gbif.phylum:src.phylum;
    val.phylumId=gbif.phylumKey?gbif.phylumKey:src.phylumId;
    val.class=gbif.class?gbif.class:src.class;
  	val.classId=gbif.classKey?gbif.classKey:src.classId;
    val.order=gbif.order?gbif.order:src.order;
    val.orderId=gbif.orderKey?gbif.orderKey:src.orderId;
    val.family=gbif.family?gbif.family:src.family;
    val.familyId=gbif.familyKey?gbif.familyKey:src.familyId;

    val.genus=val.genus?val.genus:null;
    val.genusId=(val.genus==gbif.genus)?gbif.genusKey:null; //can't handle new genus yet

    val.species=val.species?val.species:(val.taxonRank=='species'?val.scientificName:null);
    val.speciesId=(val.species==gbif.species)?gbif.speciesKey:null;

    if (!val.parentNameUsageId) {
      switch(val.taxonRank) {
        case 'species':
          val.parentNameUsageId = val.genusId;
          break;
        case 'subspecies':
        case 'variety':
          val.parentNameUsageId = val.speciesId;
          break;
      }
    }

    //append items specific to our species index
    val.datasetName=src.datasetName || null;
    val.datasetId=src.datasetId || null;
    val.bibliographicCitation=src.bibliographicCitation || null;
    val.references=src.references || null;
    val.institutionCode=src.institutionCode || null;
    val.collectionCode=src.collectionCode || null;
    val.establishmentMeans=src.establishmentMeans || null;
  } catch(err) {
    console.log('valIngestNew', err);
  }
    return val;
}
