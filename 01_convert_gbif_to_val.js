/*
Author: Jason Loomis

Project: VAL_Species

Parse GBIF species occurrence download DwCA into a VAL species list DwCA that
can be used by the VAL ALA nameindexer. This processed output will also serve
as the basis for the VAL Vermont Life List (or the VT Species Registry).

As of the fall of 2019, the GBIF API does not provide a species download that
includes checklists. Instead, they provide an occurrence download that
enumerates species.

File: 01_convert_gbif_to_val.js

Notes:

Specifics:

index GBIF name              index	ALA name
1  taxonKey                  1	taxonID
2  scientificName            2	scientificName
3  acceptedTaxonKey          3	acceptedNameUsageID
4  acceptedScientificName    4	acceptedNameUsage
5  taxonRank                 5	taxonRank
6  taxonomicStatus           6	taxonomicStatus
8  kingdomKey	               7	parentNameUsageID
10 phylumKey                 8	nomenclaturalCode
12 classKey                  9	scientificNameAuthorship
14 orderKey                  10	specificEpithet
16 familyKey                 11	infraspecificEpithet
18 genusKey                  12	taxonRemarks
20 speciesKey
                            ...also add these for future checklists:
                            13  datasetName
                            14  datasetID

- Conversions for index values 1-6 are 1:1.

- To derive ALA index 7, parentNameUsageID, we find the second-to-last value of
GBIF index 8-20 and use that.

- ALA index 8, nomenclaturalCode, will be assigned the static value 'GBIF'
because the source nomenclatural index is the GBIF backbone, which itself
comprises multiple nomenclatureal indices and is where this initial dataset
originated.

- ALA index 9, scientificNameAuthorship, will be derived from the parsed ending
of GBIF index 4. We find authorship by removing the leading 1, 2 or 3 tokens of
scientificName.

*/

//https://nodejs.org/api/readline.html
var readline = require('readline');
var fs = require('fs');
var paths = require('./00_config').paths;
var delim = require('./00_config').delim;
var csvLineTo1DArray = require('./99_parse_csv_to_array').csvLineTo1DArray;

const id = delim.infield;
const od = delim.outfield;

console.log(`config paths: ${JSON.stringify(paths)}`);

var dDir = paths.dwcaDir; //path to directory holding extracted GBIF DWcA species files

var wStream = []; //array of write streams for (1) ALA species list output file, (2) rows having taxonID != acceptedNameUsageId, ...

var idx = 0; //file row index
var out = '';
var log = '';
var top = [
  'taxonID',
  'scientificName',
  'acceptedNameUsageID',
  'acceptedNameUsage',
  'taxonRank',
  'taxonomicStatus',
  'parentNameUsageID',
  'nomenclaturalCode',
  'scientificNameAuthorship',
  'specificEpithet',
  'infraspecificEpithet',
  'vernacularName',
  'taxonRemarks',
  'datasetName',
  'datasetID'
];
var gbf = [];
var val = [];

var taxonRank = ''

var accAuth = []; //array of tokens parsed from acceptedNameUsage
var tk_idx = 0; //acceptedNameUsage token index
var author = '';
var subsps = ''; //sub-species
var variety = ''; //variety is to botanists what sub-species is for others

var fRead = readline.createInterface({
  input: fs.createReadStream(`${dDir}/gbif_species.txt`)
});

//read occurrence.txt
fRead.on('line', function (row) {
    //gbf = row.split(id).slice(); //using .slice() copies by value, not by reference
    gbf = csvLineTo1DArray(row, id);

    val[0] = gbf[0];  //taxonID ~ taxonKey
    val[1] = `"${gbf[1]}"`;  //scientificName
    val[2] = gbf[2];  //acceptedNameUsageID ~ acceptedTaxonKey
    val[3] = `"${gbf[3]}"`;  //acceptedNameUsage ~ acceptedScientificName
    val[4] = gbf[4].toLowerCase();  //taxonRank is often missing. see below
    val[5] = gbf[5].toLowerCase();;  //taxonomicStatus
    val[6] = '';      //parentNameUsageID
    val[7] = 'GBIF';   //nomenclaturalCode
    val[8] = '';      //scientificNameAuthorship
    val[9] = '';      //specificEpithet (==species)
    val[10] = '';     //infraspecificEpithet (==sub-species or variety)
    val[11] = '';     //vernacularName
    val[12] = '';     //taxonRemarks
    val[13] = '';     //datasetName
    val[14] = '';     //datasetID

    //fix taxonomicStatus: if there's an acceptedNameUsageId, it's 'accepted'
    val[5] = gbf[2] ? 'accepted' : '';

    //parentNameUsageID and taxonRank
    for (var i=7; i<=19; i+=2) {
      if (gbf[i]) { //test for non-null key
        if (i == 7) { //in the special case of kingdom, parentNameUsageID is self-referential
          val[6] = gbf[7];
        } else {
          val[6] = gbf[i-2];
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
        val[4] = taxonRank;
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
        val[9] = accAuth[1]; //specificEpithet is a single token naming a species
      } else if (taxonRank.toLowerCase() == 'genus') {
        tk_idx = 1; //scientificName for genus has only one token. scientificNameAuthorship would begin at 2nd token.
      }
    }

    if (accAuth[tk_idx]) { //if there's a token after the scientificName...
      if (accAuth[tk_idx].substring(0,6).toLowerCase() == 'subsp.') {
        //parse sub-species from name and move token count forward to author
        val[4] = 'subspecies'; //taxonRank
        val[6] = gbf[19]; //parentNameUsageID is species
        val[10] = accAuth[3]; //infraspecificEpithet
        tk_idx = 4; //this is where any author's name will be
      } else if (accAuth[tk_idx].substring(0,4).toLowerCase() == 'var.') {
        //parse variety from name and move token count forward to author
        val[4] = 'variety'; //taxonRank
        val[6] = gbf[19]; //parentNameUsageID is species
        val[10] = accAuth[3]; //infraspecificEpithet
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
    if (author) {val[8] = `"${author}"`;}

    //look for already-open write stream
    if (!wStream[0]) {
      wStream[0] = fs.createWriteStream(`${dDir}/val_species.txt`);
    }
    if (!wStream[1]) {
      wStream[1] = fs.createWriteStream(`${dDir}/val_species_mismatch.txt`);
    }

    if (idx == 0) {
      out = 'id' + od; //zeroth column is 'id', a copy of taxonID
      for (var i=0; i<top.length; i++) {
        out += top[i];
        if (i < (top.length-1)) out += od;
      }
      wStream[0].write(`${out}\n`);
    } else {
      out = val[0] + od; //dupe taxonID in zeroth column
      for (var i=0; i<val.length; i++) {
        out += val[i];
        if (i < (val.length-1)) out += od;
      }
      wStream[0].write(`${out}\n`);
    }

    //write separate file for rows having taxonID != acceptedNameUsageID
    if (idx > 0 && val[0] != val[2]) {
      out = val[0] + od; //dupe taxonID in zeroth column
      for (var i=0; i<val.length; i++) {
        out += val[i];
        if (i < (val.length-1)) out += od;
      }
      wStream[1].write(`${out}\n`);
    }

    if (idx == 0) {
      logArrayToConsole(idx, gbf, 'GBIF-TOP');
    }
    if (idx > 0 && idx < 5) {
      logArrayToConsole(idx, gbf, 'GBIF');
      logArrayToConsole(idx, top, 'VAL-TOP');
      logArrayToConsole(idx, val, 'VAL');
    }

    idx++;
});

function logArrayToConsole(idx, arr, txt='VAL') {
  var log = '';
  for (var i=0; i<arr.length; i++) {
    log += `${i}:${arr[i]}|`;
  }
  console.log(idx, txt, log);
  return log;
}

fRead.on('close', function() {
  console.log('Read file closed.');
});
