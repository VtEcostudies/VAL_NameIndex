/*
Take in ePs file as input.
Process file and output to csv.

Tags are ePs commands, like 'f71sf'. Here's what appears to be a single record:

f71 sf
.301 .03(Lepidoptera Libytheidae)J
8 56 :M
1 G
0 G
.237 .024(Libytheinae carinenta )J
gR
1 G
gS 0 36 184 694 rC
6 61 178 35 rF
0 36 184 59 rC
8 70 :M
0 G
f71 sf
-.088(NC, Haywood Co., Cold Springs Creek Rd., )A
8 81 :M
-.023(Harmon Den. .  Netted in flight. 18-VII-1995. )A
8 92 :M
(M.S. Griggs 4411)S
gR
gS 184 36 184 23 rC
192 45 :M
0 G

Data is always in parentheses. It looks like every record is contained in 2 iterations
of the 'f71 sf' tag.

*/
const { once } = require('events');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');
const fs = require('fs');
const paths = require('./00_config').paths;

var dataDir = paths.dataDir; //path to directory holding source data files - INCLUDING TRAILING SLASH
var baseName = 'ePs_Lepidoptera';
var subDir = baseName + '/';
var inpFileName = baseName + '.txt';
var outFileName = 'val_' + inpFileName;
var outStream = null;

getSpeciesFile(dataDir+subDir+inpFileName);

/*
Parse the input file into a 2D array for processing.
*/
async function getSpeciesFile(inpFilePath) {
  try {
    return await ePsFileTo2DArray(inpFilePath);
  } catch(err) {
    throw(err);
  }
}

async function ePsFileTo2DArray(file) {
  var idx = 0; //line count. 1 less than total rows if headRow.
  var rows = []; //2D array of rows. rows in array form or object form depending upon header.
  var head = [];
  var rowA = [];
  var ret = {"rows":rows, "rowCount":idx, "header":head};
  var tct = 0; //token count
  var regExp = /\(([^)]+)\)/;

  try {
    const rl = createInterface({
      input: createReadStream(file),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      //console.log(`${idx} | Raw: ${line}`);
      var fldArr = [];
      var fldVal = null;
      if (line.includes('f71 sf')) {
        if (tct == 2) {
          tct = 1;
          rows[idx++] = rowA;
          console.log(rowA);
          writeResultToFile(rowA);
          rowA = [idx]; //init for next round
        }
        else {tct++;}
      }
      fldArr = regExp.exec(line);
      if (fldArr) {
        fldVal = fldArr[1].trim();
        rowA.push(fldVal);
        //console.log(`${idx}: ${fldVal}`);
        //console.log(rowA);
      }
    });

    await once(rl, 'close');

    console.log(`File ${file} processed and closed.`);

    ret = {
      "rows": rows,
      "rowCount": idx,
      "header": head
    };

    return ret;

  } catch (err) {
    console.error(err);
    throw err; //?
  }
};

/*
This assumes that the incoming object is one line of data that was parsed into
named fields that are DwCA compliant.
*/
function writeResultToFile(row) {
  var out = row.valueOf();
  var out = row.join("|");

  if (!outStream) {
    outStream = fs.createWriteStream(`${dataDir}${subDir}${outFileName}`, {flags: 'w', encoding: 'utf8'});
  }

  outStream.write(`${out}\n`);
}

module.exports.ePsFileTo2DArray = ePsFileTo2DArray;
