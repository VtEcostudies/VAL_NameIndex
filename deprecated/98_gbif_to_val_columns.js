module.exports.initValObject = initValObject;
module.exports.gbifToValDirect = gbifToValDirect;
module.exports.gbifToValIngest = gbifToValIngest;
module.exports.getParentKeyFromTreeKeys = getParentKeyFromTreeKeys;

function initValObject() {

  var val = {
    gbifId:'',taxonId:'',scientificName:'',scientificNameAuthorship:'',
    acceptedNameUsage:'',acceptedNameUsageId:'',taxonRank:'',taxonomicStatus:'',
    parentNameUsage:'',parentNameUsageId:'',
    specificEpithet:'',infraspecificEpithet:'',nomenclaturalCode:'',
    vernacularName:'',taxonRemarks:'',
    kingdom:'',kingdomId:'',phylum:'',phylumId:'',
    class:'',classId:'',order:'',orderId:'',family:'',familyId:'',
    genus:'',genusId:'',species:'',speciesId:'',
    datasetName:'',datasetId:'',bibliographicCitation:'',references:'',
    institutionCode:'',collectionCode:'',establishmentMeans:''
    };

    return val;
}

/*
Convert gbif fields to val_species columns for output file and ingestion into
val_species database table.
*/
function gbifToValDirect(gbif) {

  var val = initValObject(); //necessary to make field-order consistent across rows and datasets

  if (gbif.canonicalName) {
    var rank = gbif.rank?gbif.rank.toLowerCase():undefined;
    var speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=rank=='species'?speciessub[1]:null;
    val.infraspecificEpithet=rank=='subspecies'?speciessub[2]:null;
    val.infraspecificEpithet=rank=='variety'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
  }

  val.gbifId=gbif.key;
  val.taxonId=gbif.key;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:gbif.scientificName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedKey?gbif.acceptedKey:gbif.key;
  val.acceptedNameUsage=gbif.accepted?gbif.accepted:gbif.scientificName;
  val.taxonRank=gbif.rank?gbif.rank.toLowerCase():null;
  val.taxonomicStatus=gbif.taxonomicStatus?gbif.taxonomicStatus.toLowerCase():null;
  val.parentNameUsageId=gbif.parentKey?gbif.parentKey:getParentKeyFromTreeKeys(gbif);
  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=gbif.authorship?gbif.authorship:null;
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:null;
  val.taxonRemarks=gbif.remarks?gbif.remarks:null;

  val.kingdom=gbif.kingdom?gbif.kingdom:null;
  val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:null;;
  val.phylum=gbif.phylum?gbif.phylum:null;
  val.phylumId=gbif.phylumKey?gbif.phylumKey:null;
  val.class=gbif.class?gbif.class:null;
  val.classId=gbif.classKey?gbif.classKey:null;
  val.order=gbif.order?gbif.order:null;
  val.orderId=gbif.orderKey?gbif.orderKey:null;
  val.family=gbif.family?gbif.family:null;
  val.familyId=gbif.familyKey?gbif.familyKey:null;
  val.genus=gbif.genus?gbif.genus:null;
  val.genusId=gbif.genusKey?gbif.genusKey:null;
  val.species=gbif.species?gbif.species:null;
  val.speciesId=gbif.speciesKey?gbif.speciesKey:null;

  return val;
}

/*
Translate GBIF taxon data to VAL taxon data for insert/update into database and
output to file.

The format of the incoming (source) data should conform to the output of the GBIF
/species API, not the  GBIF /match API.

inputs:

gbif - object returned from GBIF /species query - best match available
src - object from source input row

outputs:

scientificName without Author - We should never return a scientificName with author included (use authorship for that)
because the ala nameindexer can't handle it.

val_species columns mapped to their GBIF equivalents

additional val_species columns, if present in the incoming dataset.

2021-02-05 Important Note: Need to learn how to handle species and speciesId fields when taxonomicStatus
is '*synonym'.
*/
function gbifToValIngest(gbif, src) {

  var val = initValObject();

  if (src.id) {val.id=src.id;} //spit this back out for incoming DwCA that use it to map val_taxon.txt to other incoming dwca extensions

  //Species API returns key. Fuzzy match API returns usageKey. we handle both, in case this function was called with
  //fuzzy match output. However, the proper approach is to use GBIF match API to search for a match, then use usageKey
  //against GBIF species API to return a complete set of GBIF fields.
  gbif.key=gbif.key?gbif.key:gbif.usageKey;

  val.gbifId=gbif.key;
  val.taxonId=gbif.key;
  val.scientificName=gbif.canonicalName?gbif.canonicalName:src.canonicalName; //scientificName often contains author. nameindexer cannot handle that, so remove it.
  val.acceptedNameUsageId=gbif.acceptedKey?gbif.acceptedKey:gbif.key;
  val.acceptedNameUsage=gbif.accepted?gbif.accepted:gbif.scientificName;
  val.taxonRank=gbif.rank?gbif.rank.toLowerCase():null;
  val.parentNameUsageId=gbif.parentKey || getParentKeyFromTreeKeys(gbif);

  if (gbif.authorship) {
    val.scientificNameAuthorship = gbif.authorship;
  } else if (gbif.canonicalName) {
    var authorsub = gbif.scientificName.split(gbif.canonicalName);
    val.scientificNameAuthorship = authorsub[1]?authorsub[1].trim():null;
    console.log(`Split Author from GBIF scientificName: ${val.scientificNameAuthorship}`);
  }

  if (gbif.canonicalName) {
    var rank = gbif.rank?gbif.rank.toLowerCase():undefined;
    var speciessub = gbif.canonicalName.split(" ").slice(); //break into tokens by spaces
    val.specificEpithet=rank=='species'?speciessub[1]:null;
    val.infraspecificEpithet=rank=='subspecies'?speciessub[2]:null;
    val.infraspecificEpithet=rank=='variety'?speciessub[2]:val.infraspecificEpithet; //don't overwrite previous on false...
  }

  val.nomenclaturalCode='GBIF';
  val.scientificNameAuthorship=val.scientificNameAuthorship?val.scientificNameAuthorship:null;
  val.vernacularName=gbif.vernacularName?gbif.vernacularName:null;
  val.vernacularName=val.vernacularName?val.vernacularName+', ':null+src.vernacularName?src.vernacularName:null;
  //val.vernacularName=src.vernacularName; //temporary override used for a specific import
  src.taxonRemarks=src.taxonRemarks?src.taxonRemarks.trim():null;
  val.taxonRemarks=gbif.remarks?'gbif:'+gbif.remarks:null+src.taxonRemarks?'val:'+src.taxonRemarks:null;
  val.taxonomicStatus=gbif.taxonomicStatus?gbif.taxonomicStatus.toLowerCase():null;

  val.kingdom=gbif.kingdom?gbif.kingdom:null;
  val.kingdomId=gbif.kingdomKey?gbif.kingdomKey:null;;
  val.phylum=gbif.phylum?gbif.phylum:null;
  val.phylumId=gbif.phylumKey?gbif.phylumKey:null;
  val.class=gbif.class?gbif.class:null;
	val.classId=gbif.classKey?gbif.classKey:null;
  val.order=gbif.order?gbif.order:null;
  val.orderId=gbif.orderKey?gbif.orderKey:null;
  val.family=gbif.family?gbif.family:null;
  val.familyId=gbif.familyKey?gbif.familyKey:null;
  val.genus=gbif.genus?gbif.genus:null;
  val.genusId=gbif.genusKey?gbif.genusKey:null;
  if (val.taxonomicStatus.includes('synonym')) {
    val.species=src.species?src.species:null;
    val.speciesId=null;
  } else {
    val.species=gbif.species?gbif.species:null;
    val.speciesId=gbif.speciesKey?gbif.speciesKey:null;
  }

  //append items specific to our species index
  val.datasetName=src.datasetName || null;
  val.datasetId=src.datasetId || null;
  val.bibliographicCitation=src.bibliographicCitation || null;
  val.references=src.references || null;
  val.institutionCode=src.institutionCode || null;
  val.collectionCode=src.collectionCode || null;
  val.establishmentMeans=src.establishmentMeans || null;

  return val;
}


function getParentKeyFromTreeKeys(gbif) {
  var parentId = null;

  if (!gbif.rank) {return null;}

  //parentNameUsageID is key of next higher rank (except for kingdom, which is itself)
  switch(gbif.rank.toLowerCase()) {
    case 'kingdom': parentId = gbif.kingdomKey; break;
    case 'phylum': parentId = gbif.kingdomKey; break;
    case 'class': parentId = gbif.phylumKey; break;
    case 'order': parentId = gbif.classKey; break;
    case 'family': parentId = gbif.orderKey; break;
    case 'genus': parentId = gbif.familyKey; break;
    case 'species': parentId = gbif.genusKey; break;
    case 'subspecies': parentId = gbif.speciesKey; break;
    case 'variety': parentId = gbif.speciesKey; break;
  }

  return parentId;
}
