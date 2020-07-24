/*
  Author: Jason Loomis

  Project: VAL_Species

  File: 13_Conservation_Code_to_Name.js

  Purpose: Process Conservation Status codes into readable text for display.

  Specifics:

  - Perform conversion
  - add a column containing the result to the incoming file and output as newly-
  named file.
*/
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
dataDir = 'C:/Users/jloomis/Documents/VCE/VAL_NameIndex/repo/database/export/conservation';
//var baseName = 'Vermont_Conservation_Status';
//var baseName = 'Vermont_Conservation_Missing';
var baseName = '';
var subDir = baseName + '/';
var inpFileName = baseName + '.csv';
inpFileName = 'val_species_state_rank.txt';
var outFileName = '13_out_' + inpFileName;
var logFileName = '13_log_' + moment().format('YYYYMMDD-HHMMSSS') + '_' + inpFileName;
var errFileName = '13_err_' + inpFileName;
var outStream = null; //fs.createWriteStream(`${dataDir}${subDir}${outFileName}`, {flags: 'w'});
var errStream = fs.createWriteStream(`${dataDir}${subDir}${errFileName}`, {flags: 'w'});
var logStream = fs.createWriteStream(`${dataDir}${subDir}${logFileName}`, {flags: 'w'});

const inpFileDelim = "\t";
const outFileDelim = "\t";

var conTest = 0;
var sciTest = 0;
var test = 0;

function test_parseSciName(src={scientificName:"Xestia (Pachnobia) homogena spp. heterogena"}) {
  console.log(parseSciName(src));
  console.log(moment().format('YYYYMMDD-HHMMSSS'))
  process.exit();
}
if (sciTest) test_parseSciName({scientificName:"Papaipema sp. 2 nr. pterisii"});

var scope = {
    'G': {name:'Global',desc:''},
    'N': {name:'National',desc:''},
    'S': {name:'State',desc:''}
  };
var level = {
    'X':{name:'Presumed Extirpated',desc:'Species or community is believed to be extirpated from the nation or state/province. Not located despite intensive searches of historical sites and other appropriate habitat, and virtually no likelihood that it will be rediscovered'},
    'H':{name:'Possibly Extirpated',desc:'(Historical)—Species or community occurred historically in the nation or state/province, and there is some possibility that it may be rediscovered. Its presence may not have been verified in the past 20-40 years. A species or community could become NH or SH without such a 20-40 year delay if the only known occurrences in a nation or state/province were destroyed or if it had been extensively and unsuccessfully looked for. The NH or SH rank is reserved for pecies or communities for which some effort has been made to relocate occurrences, rather than simply using this status for all elements not known from verified extant occurrences.'},
    '1':{name:'Critically Imperiled',desc:'Critically imperiled in the nation or state/province because of extreme rarity (often 5 or fewer occurrences) or because of some factor(s) such as very steep declines making it especially vulnerable to extirpation from the state/province.'},
    '2':{name:'Imperiled',desc:'Imperiled in the nation or state/province because of rarity due to very restricted range, very few populations (often 20 or fewer), steep declines, or other factors making it very vulnerable to extirpation from the nation or state/province.'},
    '3':{name:'Vulnerable',desc:'Vulnerable in the nation or state/province due to a restricted range, relatively few populations (often 80 or fewer), recent and widespread declines, or other factors making it vulnerable to extirpation.'},
    '4':{name:'Apparently Secure',desc:'Uncommon but not rare; some cause for long-term concern due to declines or other factors.'},
    '5':{name:'Secure',desc:'Common, widespread, and abundant in the nation or state/province.'},
    'NR':{name:'Unranked',desc:'Nation or state/province conservation status not yet assessed.'},
    'U':{name:'Unrankable',desc:'Currently unrankable due to lack of information or due to substantially conflicting information about status or trends.'},
    'NA':{name:'Not Applicable',desc:'A conservation status rank is not applicable because the species is not a suitable target for conservation activities.'}
  };
var qualifier = {
    'B':{name:'Breeding',desc:'Conservation status refers to the breeding population of the species in the nation or state/province.'},
    'N':{name:'Nonbreeding',desc:'Conservation status refers to the non-breeding population of the species in the nation or state/province.'},
    'M':{name:'Migrant',desc:'Migrant species occurring regularly on migration at particular staging areas or concentration spots where the species might warrant conservation attention. Conservation status refers to the aggregating transient population of the species in the nation or state/province.'},
    '?':{name:'Inexact or Uncertain',desc:'Denotes inexact or uncertain numeric rank. (The ? qualifies the character immediately preceding it in the N- or S-rank.)'}
};

function test_getConservationNames() {
  getConservationNames('S3B,S4N');
  getConservationNames('S5B,S4N');
  getConservationNames('SNRB');
  getConservationNames('SNR?');
  getConservationNames('S1S2B');
  getConservationNames('SNAN');
  getConservationNames('N1?');
  getConservationNames('S1B,SNAN');
  getConservationNames('{S1B,S2M,N3B}');
  process.exit();
}
if (conTest) test_getConservationNames();

console.log(dataDir+subDir+inpFileName);

getConservationStatusFile(dataDir+subDir+inpFileName)
  .then(async src => {
    log(`Input file rowCount:${src.rowCount} | Header:${src.header}`);
    rowCount = src.rows.length;
    for (var i=0; i<src.rows.length; i++) {
      //await parseSciName(src.rows[i]); //this is called within selectValSpecies() now.
      await selectValSpecies(src.rows[i])
        .then(async val => {
          log(val.message, logStream);
          if (val.type == 'multiple') {
            for (var j=0; j<val.rows.length; j++) {
              log(`MULTIPLE | ${val.rows[j].taxonId} | ${val.rows[j].scientificName} | ${val.rows[j].taxonomicStatus}`, logStream);
            }
          }
          val.src.stateText = await getConservationNames(val.src.stateRank);
          writeResultToFile(val.src);
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
    return await csvFileTo2DArray(inpFileName, inpFileDelim, true, true);
  } catch(err) {
    throw(err);
  }
}

/*
  Handle an array of names from postgres, like {S1,S3B,N2?}, or a simple CSV
  list.
*/
function getConservationNames(Rs) {
  try {
    var Ra = Rs.replace(/{|}/g,'').split(',').slice(); //Strip braces, convert Rank Code CSV String to Rank Code Array
    var Ro = {}; //Rank Result Object from conversion of Code to Text and Tokens
    var Rt = ''; //Rank Text String
    if (test) console.log(`${Ra.length}:`,Ra);
    for (var i=0; i<Ra.length; i++) {
      if (test) console.log(i, Ra[i]);
      Ro = getConservationName(Ra[i], i, Ro.sId); //pass the previous Scope Id for comparison
      Rt += Ro.text;
      if (i<Ra.length-1) Rt += ', ';
    }
    console.log(Rs, '==>', Rt);
    return Rt;
  } catch(err) {
    console.log('getConservationNames', err);
    return null;
  }
}
/*
  Parse a single Conservation Rank into tokens and build Conservation Status Name from them.
  sR: incoming Rank Code - a single, parsed code (eg. N1?) from code string (eg. N1?,N2N3B)
  nR: numeric position (eg. 1) of this Rank Code (eg. S2M) within a series of Rank Codes (ie. S1B,S2M,N3B)
  pS: previous Scope value (eg. S), if there was one for a series of Rank Codes (ie. S1B,S2M,N3B)
*/
function getConservationName(sR, nR=0, pS=null) {
  try {
    //we can assume that, since this is stateRank, the tokens will all begin with 'S'
    var sId=null; //scope id
    var lId=null; //level id
    var rId=null; //range id
    var qId=null; //qualifier id
    var reg=null; //regular expresssion

    sId=sR.charAt(0); //Scope is always the 1st character

    //test for Range Rank (always Numeric) (S#S# or N#N#) with Qualifier
    reg = /(^[GNS][12345][GNS][12345][BNM?]$)/g;
    if (reg.test(sR)) {
      if (test) console.log(reg);
      lId=sR.charAt(1);
      rId=sR.charAt(3);
      qId=sR.charAt(4);
    }
    //test for Range Rank (always Numeric) (S#S# or N#N#) w/o Qualifier
    reg = /(^[GNS][12345][GNS][12345]$)/g;
    if (reg.test(sR)) {
      if (test) console.log(reg);
      lId=sR.charAt(1);
      rId=sR.charAt(3);
    }
    //test for Single Value Rank with Qualifier (eg. S3B, N1?, SXN)
    reg = /(^[GNS][XH12345U][BNM?]$)/g;
    if (reg.test(sR)) {
      if (test) console.log(reg);
      lId=sR.charAt(1);
      qId=sR.charAt(2);
    }
    //test for Single Value Rank w/o Qualifier (eg. S3, SH)
    reg = /(^[GNS][XH12345U]$)/g;
    if (reg.test(sR)) {
      if (test) console.log(reg);
      lId=sR.charAt(1);
    }
    //test for Double Letter Rank with Qualifier (eg. SNRB, SNA?)
    reg = /(^[GNS]N[AR][BNM?]$)/g;
    if (reg.test(sR)) {
      if (test) console.log(reg);
      lId=sR.substr(1,2); //ie. 'NR'
      qId=sR.charAt(3);
    }
    //test for Double Letter Rank w/o Qualifier (eg. NNR)
    reg = /(^[GNS]N[AR]$)/g;
    if (reg.test(sR)) {
      if (test) console.log(reg);
      lId=sR.substr(1,2); //ie. 'NR'
    }

    var rName = '';
    if (test) console.log(`nR:${nR} | pS:${pS} | sId:${sId}`);
    //don't add the text 'Scope-name Rank: ', it's superfluous for ALA display
    if (0 && (nR == 0 || sId != pS)) {rName += scope[sId].name + ' Rank: ';}
    if (rId) {
      //console.log('rId', rId);
      if (level[rId]) rName += `${level[lId].name} ~ ${level[rId].name}`;
    } else if (lId) {
      //console.log('lId', lId);
      if (level[lId]) rName += level[lId].name;
    }
    if (qId) {
      //console.log('qId:', qId);
      if (qualifier[qId]) rName += ` (${qualifier[qId].name})`;
    }

    if (test) console.log('getConservationName | ', sR, '|', rName);

    return {text:rName, sId:sId, lId:lId, rId:rId, qId:qId};
  } catch(err) {
    console.log(`getConservationName(${sR}) ERROR | `, err);
    return null;
  }
}

/*
src - object with one row of data from the source file

The incoming scientificName may have variety (var.) or subspecies (ssp.) to
indicate a third identifier.
*/
async function selectValSpecies(src) {
    var sciName = await parseSciName(src, logStream);

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
              //console.log(res.message);
              res.src = src;
              resolve(res);
            } else if (res.rowCount == 0) {
              var err = {type:'absent', message:`Taxon ${src.scientificName} as ${sciName} NOT found.`};
              //console.log(err.message);
              err.src = src;
              err.sql = text;
              reject(err);
            } else { //we now accept the top option as a valid taxon to attach conservation status to...
              res.type = 'multiple';
              res.message=`Taxon ${src.scientificName} as ${sciName} has multiple rows: ${res.rowCount}`;
              //console.log(res.message);
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
  out = out.replace(/(^,)|(,$)/g, "");//remove leading, trailing comma
  out = out.replace(/(^\t)|(\t$)/g, "");//remove leading, trailing tab
  log(`writeResultToFile | ${out}`, logStream);
  outStream.write(`${out}\n`);
  outCount++;
}
