/*
  Author: Jason Loomis

  Project: GBIF_Species_to_VAL_Species

  Parse GBIF species download DWcA into a VAL species list DwCA that can be
  used by the VAL ALA nameindexer.

  As of the fall of 2019, the GBIF API does not provide a species download that
  includes checklists. Instead, they provide an occurrence download that
  enumarates species.

  File: 03_update_core_from_gbif.js

  Specifics:

*/

//https://nodejs.org/api/readline.html
var readline = require('readline');
var fs = require('fs');
var Request = require("request");
var paths = require('./00_config').paths;
var delim = require('./00_config').delim;
var csvLineTo1DArray = require('./99_parse_csv_to_array').csvLineTo1DArray;

const id = ","; //delim.outfield;
const od = ","; //delim.outfield;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dDir = paths.dwcaDir; //path to directory holding extracted GBIF DWcA species files
var readFile = "val_species.csv";
var writeFile = "val_species_nameindexer.txt";

var wStream = []; //array of write streams

var idx = 0; //read file row index
var out = '';
var val = [];
var flg = false;

var fRead = readline.createInterface({
  input: fs.createReadStream(`${dDir}/${readFile}`)
});

wStream[0] = fs.createWriteStream(`${dDir}/${writeFile}`, {flags: 'w'});

//read species file
fRead.on('line', function (row) {
    val = csvLineTo1DArray(row, id);

    //look for empty scientificName. if empty, put acceptedNameUsage there.
    //ALA nameindexer barfs on null scientificName.
    out = `"${idx}"${od}`; //put line# in column 0
    if (!val[1]) {
      val[1] = val[3];
      flg = true;
    }

    //loop through values. add double quotes if not there
    //write back out to file
    for (var i=0; i<val.length; i++) {
      if (val[i].substring(0,1) != `"` && val[i].substring(val[i].length-1,val[i].length) != `"`) {
        val[i] = `"${val[i]}"`;
      }
      out += val[i];
      if (i < (val.length-1)) out += od;
    }

    if (flg) {console.log(idx, out); flg=false;}

    wStream[0].write(`${out}\n`);

    //logArrayToConsole(idx, val);

    idx++;
});

fRead.on('close', async function() {
  console.log(`${readFile} closed.`);
});

function getGbifSpecies(idx, key) {
  var parms = {
    url: `http://api.gbif.org/v1/species/${key}`,
    json: true
  };

  return new Promise((resolve, reject) => {
    Request.get(parms, (err, res, body) => {
      if (err) {
        reject(err);
      } else if (res.statusCode > 299) {
        console.log(` ${idx} | GBIF Species | ${key} | ${res.statusCode}`);
        reject(res);
      } else {
        console.log(` ${idx} | GBIF Species | ${key} | ${res.statusCode}`);
        resolve(body);
      }
    });
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
