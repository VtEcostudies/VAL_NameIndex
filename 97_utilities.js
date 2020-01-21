
module.exports.log = log;
module.exports.logErr = logErr;
module.exports.addTaxonRank = addTaxonRank;
module.exports.addCanonicalName = addCanonicalName;
module.exports.parseSciName = parseSciName;

function log(out, stream=null, override=false) {
  if (override||!stream) {console.log(out);}
  if (stream) stream.write(`${out}\n`);
}

function logErr(out, stream=null, override=true) {
  log(out, stream, override);
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
    log(`ERROR in addTaxonRank: ${err}`, stream);
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
    throw `parseSciName: ${err}`;
    return sciName;
  }
}
