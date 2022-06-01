/*
  Project: VAL_Species

  File: 17_text_to_table.js

  Creat a species index from dowloaded occurrences.

  Read header row for column names to create a new table.

  If table create succeeds, load the data.
*/

const fs = require('fs');
const path = require('path');
const Request = require("request");
const moment = require('moment');
const paths = require('./00_config').paths;
const db_params = require('./db_config.json').pg;
const db = require('./VAL_Utilities/db_postgres');
const query = require('./VAL_Utilities/db_postgres').query;
const pgUtil = require('./VAL_Utilities/db_pg_util');
const csvFileToArrayOfObjects = require('./VAL_Utilities/99_parse_csv_to_array').csvFileToArrayOfObjects;
const log = require('./VAL_Utilities/97_utilities').log;

console.log(`config paths: ${JSON.stringify(paths)}`);

const dataDir = ''; //paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
const subDir = ''; //'val_occ_species/';
const logsDir = 'logs/';
var fileName = 'val_occ_species'; //MUST me name without extension - this is used to name the db table
var fileExt = '.csv';
var inpFileName = fileName + fileExt;
var inpFilePath = dataDir+subDir+inpFileName;
var inpFileDelim = ",";

const dateTime = moment().format('YYYYMMDD-HHmmsss');
var logFileName = 'log_' + dateTime + '_' + fileName + '.log';
var errFileName = 'err_' + dateTime + '_' + fileName + '.log';

var logStream = fs.createWriteStream(`${logsDir}${subDir}${logFileName}`);
var errStream = fs.createWriteStream(`${logsDir}${subDir}${errFileName}`);

var headRow = true;
var rowCount = 0; //count records available
var notCount = 0; //count records NOT found in val_speies
var insCount = 0; //count records inserted
var errCount = 0; //count errors

process.on('exit', function(code) {
  displayStats();
  return console.log(`About to exit with code ${code}`);
});

console.log('Use command-line arguments like this: file=val_occ_species.csv | delim=, | delim=\t');
for (var i=0; i<process.argv.length; i++) {
    var all = process.argv[i].split('='); //the ith command-line argument
    var act = all[0]; //action, left of action=argument
    var arg = all[1]; //argument, right of action=argument
    console.log(`command-line argument ${i}`, all);
	switch(act) {
    case 'file':
      inpFilePath = arg;
      inpFileName = inpFilePath.split('\\').at(-1);
      //inpFilePath = path.resolve(inpFilePath); //this takes a partial path and resolves to absolute path SEE BELOW
      fileName = inpFileName.split('.')[0];
      fileExt =  inpFileName.split('.')[1];
      console.log('Command-line results | filePath', inpFilePath, '| fileName', fileName, '| fileExt', fileExt);
      break;
    case 'delim':
      inpFileDelim = arg;
      break;
    }
  }

inpFilePath = path.resolve(inpFilePath); //this takes a partial path and resolves to absolute path

db.connect(db_params)
  .then(res => {
  getSpeciesFile(inpFilePath)
    .then(async src => {
      log(`Input file rowCount:${src.rowCount}`)
      log(`Header Row: ${src.header}`);
      rowCount = src.rows.length;
      if (rowCount) {log(`First Row: ${JSON.stringify(src.rows[0])}`);}
      if (headRow) {headObj = src.rows[0];}
      else {headObj = {};}
      //for (var i=0; i<src.rows.length; i++) {}
      createTable(fileName, headObj)
        .then(res => {
          copyData(fileName, inpFilePath, inpFileDelim)
            .then(res => {
              console.log('00_text_to_table.js | copyData | SUCCESS');
            })
            .catch(err => {
              console.log('00_text_to_table.js | copyData | ERROR', err.message);
            })
        })
        .catch(err => {
          console.log('00_text_to_table.js | createTable | ERROR', err.message);
        });
    })
  })
  .catch(err => {
    console.log('db.connect ERROR', err);
  })

/*
Parse the input file into an array of objects for processing.
*/
async function getSpeciesFile(inpFileName) {
  try {
    return await csvFileToArrayOfObjects(inpFileName, inpFileDelim, headRow, true);
  } catch(err) {
    throw(err);
  }
}

/*
*/
function createTable(tbl_name, obj_cols) {
  var tbl_cols = '';
  var col_prfx = '';

  Object.keys(obj_cols).forEach((col, idx) => {
    console.log('header column', idx, col);
    //tbl_cols += `"${col_prfx}${col}" TEXT,`;
    tbl_cols += `"${col_prfx}${col}"`;
    if (col.includes('Key')) {
      tbl_cols += ' BIGINT,'
    } else {
      tbl_cols += ' TEXT,';
    }
  })

  tbl_cols = tbl_cols.slice(0, -1);

  var sql_create = `CREATE TABLE ${tbl_name} (${tbl_cols})`;

  console.log('00_text_to_table.js::createTable', sql_create);

  return new Promise((resolve, reject) => {
    query(sql_create, [])
      .then(res => {
        resolve(res);
      })
      .catch(err => {
        reject(err);
      })
  })
}

/*
*/
function copyData(tbl_name, file_path, row_delim=',', headRow=true) {

  sql_copy = `COPY ${tbl_name} FROM '${file_path}' DELIMITER '${row_delim}' CSV`;
  if (headRow) {sql_copy += ' HEADER';}

  console.log('00_text_to_table.js::copyData', sql_copy);

  return new Promise((resolve, reject) => {
    query(sql_copy, [])
      .then(res => {
        resolve(res);
      })
      .catch(err => {
        reject(err);
      })
  })
}

function displayStats() {
  log(`total:${rowCount}|missing:${notCount}|inserted:${insCount}|errors:${errCount}`, logStream, true);
  log(`Log file name: ${logsDir+subDir+logFileName}`, logStream, true);
  log(`Error file name: ${logsDir+subDir+errFileName}`, logStream, true);
}
