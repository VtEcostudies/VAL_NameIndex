/*
  Author: Jason Loomis

  Project: GBIF_Species_to_VAL_Species

  Parse GBIF species download DWcA into a VAL species list DwCA that can be
  used by the VAL ALA nameindexer.

  As of the fall of 2019, the GBIF API does not provide a species download that
  includes checklists. Instead, they provide an occurrence download that
  enumarates species.

  File: 02_add_missing_accepted_id_from_gbif.js

  Specifics:

  While converting gbif species to val species, we found that the dataset
  had taxa rows with acceptedNameUsageID which were not defined within the file.
  This causes a referential dead-end that needs remedy.

  In the previous step, created by the file 01_convert_gbif_to_val.js, we created
  2 files:

  1) val_species.txt
  2) val_species_mistmatch.txt

  File (2) is all rows in val_species.txt where taxonId != acceptedNameUsageID. That mismatch
  is not itself a problem. However, a sub-set of those values refer to an acceptedNameUsageID
  which is not elsewhere defined with a primary taxonID within the same file val_species.txt.
  This is a problem: accepted taxa have no definition.

  There was not an easy way to write code to search for each missing acceptedNameUsageID
  withing the file, and that's what relational databases are for, so we imported both
  into postgres and used a query to select just those acceptedNameUsageIDs that were
  not defined as primary taxonIDs.

  The solution:

  1) Load val_species.txt into postgres table val_species
  2) Load val_mismatch.txt into postgres table val_mismatch
  3) Query RIGHT JOIN on val_mismatch.acceptedNameUsageID NOT in
  val_species.taxonId and output to file. => 1357 results
  4) Iterate over acceptedNameUsageIDs in result set, hit GBIF API for those
  taxonIds, add them to our original val_species.txt.

*/

//https://nodejs.org/api/readline.html
var readline = require('readline');
var fs = require('fs');
var Request = require("request");
var paths = require('./00_config').paths;
var delim = require('./00_config').delim;
var csvLineTo1DArray = require('./99_parse_csv_to_array').csvLineTo1DArray;

const id = delim.outfield;
const od = delim.outfield;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dDir = paths.dwcaDir; //path to directory holding extracted GBIF DWcA species files

var wStream = []; //array of write streams

var idx = 0; //read file row index
var out = '';
var log = '';
var val = [];
var keys = [];
var nue = [];

var fRead = readline.createInterface({
  input: fs.createReadStream(`${dDir}/val_species_missing.csv`)
});

//read missing species file
fRead.on('line', function (row) {
    //val = row.split(id).slice(); //using .slice() copies by value, not by reference
    val = csvLineTo1DArray(row, id);

    keys[idx] = val[3].replace(/"+/g,''); //acceptedNameUsageID with double-quotes removed
    keys[idx][0] = row;

    console.log(idx, keys[idx]);

    idx++;
});

fRead.on('close', async function() {
  console.log('Read file closed.');
  wStream[0] = fs.createWriteStream(`${dDir}/val_species_add.txt`, {flags: 'w'});
  //wStream[1] = fs.createWriteStream(`${dDir}/val_species`, {flags: 'a'});
  wStream[2] = fs.createWriteStream(`${dDir}/val_species_err.txt`, {flags: 'a'});

  var gbif = null;
  for (var idx=0; idx < keys.length; idx++) {
    try {
      gbif = await getGbifSpecies(idx, keys[idx]);
      if (gbif) {

        console.log(`taxonId = ${gbif.key} | scientificName = ${gbif.scientificName}`);

        var tokens = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
        var specificEpithet=gbif.rank=='SPECIES'?tokens[1]:'';
        var infraspecificEpithet=gbif.rank=='SUBSPECIES'?tokens[2]:'';

        nue[0]=gbif.key;
        nue[1]=`"${gbif.scientificName}"`;
        nue[2]=gbif.key;
        nue[3]=`"${gbif.scientificName}"`;
        nue[4]=gbif.rank.toLowerCase();
        nue[5]=gbif.taxonomicStatus.toLowerCase()
        nue[6]=gbif.parentKey;
        nue[7]='GBIF';
        nue[8]=`"${gbif.authorship}"`;
        nue[9]=specificEpithet;
        nue[10]=infraspecificEpithet;
        nue[11]=gbif.vernacularName?gbif.vernacularName:'';
        nue[12]=gbif.remarks;
        nue[13]='';
        nue[14]='';

        if (!wStream[0]) {
          wStream[0] = fs.createWriteStream(`${dDir}/val_species_add.txt`, {flags: 'w'});
        }
        /*
        if (!wStream[1]) {
          wStream[1] = fs.createWriteStream(`${dDir}/val_species`, {flags: 'a'});
        }
        */
        out = nue[0] + od; //dupe taxonId in zeroth column
        for (var i=0; i<nue.length; i++) {
          out += nue[i];
          if (i < (nue.length-1)) out += od;
        }

        wStream[0].write(`${out}\n`);
        //wStream[1].write(`${out}\n`);
      }
    } catch(err) {
      console.log(err);
      if (!wStream[2]) {
        wStream[2] = fs.createWriteStream(`${dDir}/val_species_err.txt`);
      }
      wStream[2].write(`${idx} | ${keys[idx]} | ${keys[idx][0]}\n`);
    } //end of catch()
  } //end of for loop
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
