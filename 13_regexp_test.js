var fs = require('fs');
var Request = require("request");
var moment = require('moment');
var paths = require('./00_config').paths;
const query = require('./database/db_postgres').query;
const pgUtil = require('./database/db_pg_util');
const log = require('./97_utilities').log;
const parseSciName = require('./97_utilities').parseSciName;
const csvFileTo2DArray = require('./99_parse_csv_to_array').csvFileTo2DArray;
var staticColumns = [];
var staticTypes = [];

var dataDir = paths.dataDir; //path to directory holding extracted GBIF DwCA species files
var baseName = 'Vermont_Conservation_Status';
//var baseName = 'Vermont_Conservation_Missing';
var subDir = baseName + '/';
var inpFileName = baseName + '.csv';
var outFileName = 'test_out_' + inpFileName;
var logFileName = 'test_log_' + moment().format('YYYYMMDD-HHMMSS') + '_' + inpFileName;
var errFileName = 'test_err_' + inpFileName;
var outStream = fs.createWriteStream(`${dataDir}${subDir}${outFileName}`, {flags: 'w'});
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});

const inputFileDelim = ",";
const outputFileDelim = ",";

function test_parseSciName(src={scientificName:"Xestia (Pachnobia) homogena spp. heterogena"}) {
  console.log(parseSciName(src));
  process.exit();
}
test_parseSciName({scientificName:"Papaipema sp. 2 nr. pterisii"});

getConservationStatusFile(dataDir+subDir+inpFileName)
  .then(async src => {
    log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
    rowCount = src.rows.length;
    for (var i=0; i<src.rows.length; i++) {
      //await parseSciName(src.rows[i]);
      await selectValSpecies(src.rows[i])
        .then(async val => {
          log(val.message, outStream);
          if (val.type == 'multiple') {
          for (var j=0; j<val.rows.length; j++) {
            log(`${val.rows[j].taxonId} | ${val.rows[j].scientificName} | ${val.rows[j].taxonomicStatus}`, outStream, true);
          }}
        }).catch(err => {
          if (err.type != 'ignore') {log(err.message, errStream); /*log(err.sql, errStream);*/}
        });
    }
  })
  .catch(err => {
    log(`ERROR: getConservationStatusFile | ${err.message}`, logStream);
  });

/*
  Parse the input file into a 2D array for processing.

  NOTE: The input file MUST have a header row whose headings exactly match the column
  names in the table val_conservation_status.

*/
async function getConservationStatusFile(inpFileName) {
  try {
    return await csvFileTo2DArray(inpFileName, inputFileDelim, true, true);
  } catch(err) {
    throw(err);
  }
}

/*
src - object with one row of data from the source file

The incoming scientificName may have variety (var.) or subspecies (ssp.) to
indicate a third identifier.
*/
async function selectValSpecies(src) {
    var sciName = await parseSciName(src);

    var text =
`
SELECT "taxonId", "scientificName", "taxonomicStatus" from val_species
WHERE "scientificName"='${sciName}'
ORDER BY "taxonomicStatus" ASC
`;

    return new Promise((resolve, reject) => {
      if (!sciName) { //used this initially to process only var. and spp.. Is now defunct.
        reject({type:'ignore', message:'scientificName does not contain var., spp., variety, or subspecies.', src:src});
      } else {
        query(text)
          .then(res => {
            //console.log(`selectValSpecies result: Rows: ${res.rowCount}, First Row:`, res.rows[0]);
            if (res.rowCount == 1) {
              res.type = 'single';
              res.message = `Found matching taxon ${src.scientificName} as ${sciName}`;
              console.log(res.message);
              res.src = src;
              resolve(res);
            } else if (res.rowCount == 0) {
              var err = {type:'absent', message:`Taxon ${src.scientificName} as ${sciName} NOT found.`};
              console.log(err.message);
              err.src = src;
              err.sql = text;
              reject(err);
            } else { //we now accept the top option as a valid taxon to attach conservation status to...
              res.type = 'multiple';
              res.message=`Taxon ${src.scientificName} as ${sciName} has multiple rows: ${res.rowCount}`;
              console.log(res.message);
              res.src = src;
              resolve(res);
            }
          })
          .catch(err => {
            err.src = src;
            reject(err);
          });
        }
    })
}
