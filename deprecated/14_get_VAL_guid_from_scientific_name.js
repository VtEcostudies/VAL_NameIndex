/*
  Project: VAL_Species

  File: 14_get_VAL_guid_from_scientific_name.js

  Purpose: A simple utility to add a column to a source CSV file for VAL DE GUID,
  so that VAL Wordpress site can link to VAL DE BIE species page, which is indexed
  by GUID, not by Scientific Name.

  Specifics:

  Kent and Nathaniel have a new Moths_Vermont page in VAL. We want to link to VAL
  species pages for each species.

  This seems simple, but it's not easy to get all those links for a large dataset,
  because VAL DE needs an internal LSID to show a unique species page. To retrieve
  a unique LSID, we need to uniquely ID a species by name, which is done via BIE search.

  So here we perform the BIE name-search lookup. If we get a single result, we assume
  it's a match, and we add the resulting LSID to a column in the source data file.
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
var baseName = 'empty';
baseName = 'Moths_Vermont';

var subDir = baseName + '/';

//here, input file name should be the post-processed species file fro step 08.
if (inpFileDelim == ",") {
  inpFileName = `val_${baseName}.csv`;
} else if (inpFileDelim == '\t') {
  inpFileName = `val_${baseName}.txt`;
}

var outFileName = 'guid_' + inpFileName;
var logFileName = 'guid_log_' + moment().format('YYYYMMDD-HHmmsss') + '_' + inpFileName;
var errFileName = 'guid_err_' + inpFileName;

//Don't create outStream here. Empty outStream flags writing header to file.
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

var dbInsert = 0;
var dbUpdate = 0;

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

getColumns()
  .then(col => {
    //console.log(`Getting Species File... ${dataDir+subDir+inpFileName}`);
    getSpeciesFile(dataDir+subDir+inpFileName)
      .then(async src => {
        log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
        rowCount = src.rows.length;
        for (var i=0; i<src.rows.length; i++) {
          await getVALSpeciesByName(src.rows[i], i)
            .then(res => { //handle all successful GETs (incl. empty results)
              //console.log(res);
              GetConservationStatus(res, i)
                .then(res => {
                  log(`${res.idx} | Result ${res.src.scientificName} | ${res.cvs.stateList} | ${res.cvs.stateRank} | ${res.cvs.SGCN}`, logStream, true);
                  var wrt = {};
                  wrt.scientificName = res.src.scientificName;
                  wrt.vernacular = res.src.vernacularName;
                  wrt.url_species = res.src.url_species;
                  wrt.stateRank = res.cvs.stateRank;
                  wrt.SGCN = res.cvs.SGCN;
                  writeResultToFile(wrt);
                })
                .catch(err => {
                  log(`${res.idx} | GetConservationStatus ERROR | ${err.message}`, logStream, true);
                })
            })
            .catch(err => { //an http request error (not just empty results)
              log(`getVALSpeciesByName ERROR | ${err.message}`, errStream, true);
            })
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
Get a VAL species result object from bie-ws API by Scientific Name.

Query to narrow search to a single result:

https://bie-ws.vtatlasoflife.org/search.json?q=taxon:Actias%20luna&fq=rank:species


*/
function getVALSpeciesByName(src, idx) {

  var sciName = src.scientificName;
  var ws = `https://bie-ws.vtatlasoflife.org/search.json`;
  var ui = `https://bie.vtatlasoflife.org/search`;
  var sp = `https://bie.vtatlasoflife.org/species/`;
  var pq = `?q=${sciName}`; //`?q=taxon:${sciName}`;
  var fq1 = `&fq=rank:species`;
  var fq2 = `&fq=scientificNameAuthorship:"${src.scientificNameAuthorship}"`;
  var pg = `&start=0&rows=100&dir=asc`;

  var parms = {
    url: `${ws}${pq}${fq1}${pg}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        log(`getVALSpeciesByName ERROR | ${err.message}`, errStream);
        err.src = src;
        err.idx = idx;
        reject(err);
      } else if (1 == body.searchResults.totalRecords) {
        var ret = body.searchResults.results[0];
        //console.log('getVALSpeciesByName 0th result', ret);
        log(`${idx} | getVALSpeciesByName(${sciName}) | ${res.statusCode} | GUID:${ret.guid?ret.guid:undefined}`, logStream, true);
        src.idx = idx; //attach incoming row index to returned object for downstream use
        src.guid = ret.guid;
        src.records = body.searchResults.totalRecords;
        src.url_species = `${sp}${ret.guid}`;
        src.url_search = `${ui}${pq}${fq1}`;
        src.url_excel = `=hyperlink("${sp}${ret.guid}")`;
        src.error = null;
        resolve(src);
      } else { //multiple results - search for our name among them
        var arr = body.searchResults.results;
        for (var i=0; i<arr.length; i++) {
          if (arr[i].name == src.scientificName) {
            src.idx = idx;
            src.guid = arr[i].guid;
            src.records = arr.length;
            src.url_species = `${sp}${arr[i].guid}`;
            src.url_search = `${ui}${pq}${fq1}`;
            src.url_excel = `=hyperlink("${sp}${arr[i].guid}")`;
            src.error = null;
            resolve(src);
          }
        }
        if (!src.guid) {
          var err = {};
          log(`${idx} | getVALSpeciesByName(${sciName}) | ${res.statusCode} | Error | Wrong number of records: ${body.searchResults.totalRecords}`, logStream, true);
          src.idx = idx;
          src.guid = null;
          src.records = body.searchResults.totalRecords;
          src.url_species = `${ui}${pq}${fq1}`;
          src.url_search = `${ui}${pq}${fq1}`;
          src.url_excel = `=hyperlink("${ui}${pq}${fq1}")`;
          src.error = `Error: wrong number of records: ${body.searchResults.totalRecords}`;
          resolve(src);
        }
      }
    });
  });
}

/*
Get Conservation Status for taxonId
*/
async function GetConservationStatus(src, idx) {

  var sql_select = `select * from val_conservation_status where "taxonId"=$1`;

  //console.log('GetConservationStatus', sql_select, src.taxonId);

  return new Promise((resolve, reject) => {
    query(sql_select, [src.taxonId])
      .then(res => {
        var ret = {};
        if (res.rowCount) {
          ret.cvs = res.rows[0];
          console.log(`GetConservationStatus | ${src.taxonId} | ${src.scientificName} | ${ret.cvs.stateRank} | ${ret.cvs.SGCN}`);
        } else {
          ret.cvs = {};
        }
        ret.src = src;
        ret.idx = idx;
        resolve(ret);
      })
      .catch(err => {
        log(`GetConservationStatus ERROR: ${err}`, logStream, true);
        err.src = src;
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
/*
    if (isNaN(fld)) { //check for null, numeric
      //check for leading and trailing double-quotes
      //check for leading equals sign ('=') - don't quote these. Excel f's it up.
      if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"` && fld.substring(0,1) != `=`) {
        fld = `"${fld}"`;
      }
    }
*/
    out += fld + outFileDelim;
  }
  //write output to file
  //NOTE: the next line will remove a column if the leading field is blank
  out = out.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter (hmm, trailing can't be correct. we get these, and we don't want to lose them!)
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
