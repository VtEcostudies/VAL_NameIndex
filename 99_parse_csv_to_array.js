/*
Parsing function copied from one of the answers here:
https://stackoverflow.com/questions/8493195/how-can-i-parse-a-csv-string-with-javascript-which-contains-comma-in-data
Readfile code copied from nodejs help:
https://nodejs.org/api/readline.html#readline_example_read_file_stream_line_by_line
*/
const { once } = require('events');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

async function csvFileTo2DArray(file, delim=',', headRow=true, filterAscii=false) {
  var idx = 0; //line count. 1 less that total rows if headRow.
  var rows = []; //2D array of rows. rows in array form or object form depending upon header.
  var head = [];
  var ret = {"rows":rows, "rowCount":idx, "header":head};

  try {
    const rl = createInterface({
      input: createReadStream(file),
      crlfDelay: Infinity
    });

    rl.on('line', (line) => {
      //console.log(`${idx}: ${line}`);
      var rowA = csvLineTo1DArray(line, delim, filterAscii);
      var rowO = {};
      if (headRow && idx==0 && head.length==0) {
        head = rowA;
      } else if (headRow) {
        for (var i=0; i<rowA.length; i++) {
          rowO[head[i]] = rowA[i];
        }
        rows[idx++] = rowO;
      } else {
        rows[idx++] = rowA;
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
    //throw err; //?
  }
};

function csvTextTo2DArray(text, delim=',', filterAscii=false) {
    let p = '', row = [''], ret = [row], i = 0, r = 0, s = !0, l;
    for (l of text) {
        if ('"' === l) {
            if (s && l === p) row[i] += l;
            s = !s;
        } else if (delim === l && s) {
          row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
          if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
          l = row[++i] = '';
        } else if ('\n' === l && s) {
            if ('\r' === p) row[i] = row[i].slice(0, -1);
            row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
            if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
            row[i] = row[i].trim();
            row = ret[++r] = [l = '']; i = 0;
        } else row[i] += l;
        p = l;
    }
    return ret;
};

function csvLineTo1DArray(text, delim=',', filterAscii=false) {
    let p = '', row = [''], ret = [row], i = 0, r = 0, s = !0, l;
    for (l of text) {
        if ('"' === l) {
            if (s && l === p) row[i] += l;
            s = !s;
        } else if (delim === l && s) {
            row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
            if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
            row[i] = row[i].trim();
            l = row[++i] = '';
        } else if ('\n' === l && s) {
            row[i] = row[i].replace(/\s+/g, " "); //replace whitespce with actual space
            if (filterAscii) row[i] = row[i].replace(/[^ -~]+/g, ""); //filter ASCII
            row[i] = row[i].trim();
            break; //exit loop at line-ending char
        } else row[i] += l;
        p = l;
    }

    return row;
};

module.exports.csvFileTo2DArray = csvFileTo2DArray;
module.exports.csvTextTo2DArray = csvTextTo2DArray;
module.exports.csvLineTo1DArray = csvLineTo1DArray;
