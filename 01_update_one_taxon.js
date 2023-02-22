/*
    Update one row of data from GBIF in $speciesTable by taxonKey.
*/

const { getGbifTaxon } = require('./VAL_Utilities/95_gbif_api_calls');

//get taxonKey from command line
const process = require('process');
for (var i=2; i<process.argv.length; i++) {
    var arg = process.argv[i]; //the ith command-line argument
    console.log(`command-line argument ${i}`, arg);
}
const speciesTable = 'new_species';
const taxonKey = process.argv[2];
console.log(`Attempting to update ${speciesTable} for taxonKey ${taxonKey}`)
