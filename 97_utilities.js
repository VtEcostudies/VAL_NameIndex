
module.exports.log = log;
module.exports.logErr = logErr;
module.exports.jsonToString = jsonToString;
module.exports.addTaxonRank = addTaxonRank;
module.exports.addCanonicalName = addCanonicalName;
module.exports.parseSciName = parseSciName;

function log(out, stream=null, consoul=false) {
  if (consoul || !stream) {console.log(out);}
  if (stream) stream.write(`${out}\n`);
}

function logErr(out, stream=null, override=true) {
  log(out, stream, override);
}

/*
Convert json object to object with:
  columns: string of column names, separated by delim
  values: string of values, separated by delim
Returned as object, like {columns:'one,two,three', values:'1,2,3'}
*/
function jsonToString(obj, delim=',', stream=null) {
  var vals = ''; //string of column values, delim-separated
  var cols = ''; //string of column names, delim-separated

  try {
    //loop through values. add double quotes if not there
    for (const key in obj) {
      fld = obj[key]!=null?obj[key]:'';
      if (isNaN(fld)) { //check for null, numeric
        //check for leading and trailing double-quotes
        if (fld.substring(0,1) != `"` && fld.substring(fld.length-1,fld.length) != `"`) {
          fld = `"${fld}"`;
        }
      }
      vals += fld + delim;
      cols += key + delim;
    }
    vals = vals.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter
    cols = cols.replace(/(^,)|(,$)/g, "");//remove leading, trailing delimiter

    return {values:vals, columns:cols};
  } catch(err) {
    log(`ERROR in jsonToString: ${err.message}`, stream, true);
    return {values:null, columns:null};
  }
}

function addTaxonRank(src, stream=null) {
  try {
    if (!src.canonicalName) {addCanonicalName(src, stream);}

    log(`addTaxonRank for canonicalName ${src.canonicalName}`, stream);

    var tokens = src.canonicalName.split(" ").slice(); //break name into tokens by spaces

    switch(tokens.length) {
      case 3:
        if (src.kingdom && src.kingdom.toLowerCase()=='plantae') {src.taxonRank = 'variety';}
        else {src.taxonRank = 'subspecies';}
        break;
      case 2:
        src.taxonRank = 'species';
        break;
      case 1:
        src.taxonRank = 'genus';
        break;
      default:
        src.taxonRank = 'error';
        throw `Wrong number of tokens (${tokens.length}) in scientificName '${src.canonicalName}'. taxonRank: ${src.taxonRank}`;
        break;
    }
    return src.taxonRank;
  } catch(err) {
    log(`ERROR in addTaxonRank: ${err.message}`, stream);
    src.taxonRank = 'error';
    return src.taxonRank;
  }
}

function addCanonicalName(src, stream=null) {
  try {
    log(`addCanonicalName for scientificName ${src.scientificName}`, stream);
    src.canonicalName = parseSciName(src, stream);
    return src.canonicalName;
  } catch(err) {
    log(`ERROR IN addCanonicalName for scientificName ${src.scientificName}: ${err.message}`, stream, true);
    throw `addCanonicalName: ${err}`;
    return null;
  }
}

function parseSciName(src, stream=null) {
  try {
    log(`parseSciName for scientificName ${src.scientificName}`, stream);
    var sciName = src.scientificName;
    var regex = /( var. )|( spp. )|( ssp. )|( variety )|( subspecies )/g;

    sciName.replace(/\s+/g, " "); //replace whitespce with actual space

    //find and remove subspecies/variety indentifiers from scientificName
    if (regex.test(sciName)) {
      sciName = sciName.replace(regex, " ");
      log(`parseSciName|${src.scientificName}|${sciName}`, stream);
    }

    //find and replace parenthetical names from scientificName
    regex = /( \(.+?\) )/g;
    if (regex.test(sciName)) {
      sciName = sciName.replace(regex, " ");
      log(`parseSciName|${src.scientificName}|${sciName}`, stream);
    }

    //find and replace ' x ' or ' X ' from name
    regex = /( x )|( X )/g;
    if (regex.test(sciName)) {
      sciName = sciName.replace(regex, " ");
      log(`parseSciName|${src.scientificName}|${sciName}`, stream);
    }

    //find and remove ' sp. ' and ' nr .' from name
    regex = /( sp. )|( nr. )/g;
    if (regex.test(sciName)) {
      sciName = sciName.replace(regex, " ");
      log(`parseSciName|${src.scientificName}|${sciName}`, stream);
    }

    //remove numbers
    regex = /[\d-]/g; //this removes dashes (-), which is bad
    regex = /(0)|(1)|(2)|(3)|(4)|(5)|(6)|(7)|(8)|(9)/g;
    if (regex.test(sciName)) {
      sciName = sciName.replace(regex, "");
      log(`parseSciName|${src.scientificName}|${sciName}`, stream);
    }

    //remove double spaces
    regex = /  /g;
    if (regex.test(sciName)) {
      sciName = sciName.replace(regex, " ");
      log(`parseSciName|${src.scientificName}|${sciName}`, stream);
    }

    return sciName;
  } catch(err) {
    log(`ERROR IN parseSciName for scientificName ${src.scientificName}: ${err.message}`, stream, true);
    throw `parseSciName: ${err}`;
    return sciName;
  }
}
