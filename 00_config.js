
exports.paths = {
  dwcaDir: "../",
  dataDir: "../datasets/"
};

//infield: field-delimiter for incoming gbif_species.txt
//outfield: field-delimiter for outgoing val_spceies.txt
exports.delim = {
  infield: "\t",
  outfield: ","
};

exports.urls = {
  val_docker_beta: "https://beta.vtatlasoflife.org",
  primary:  "https://vtatlasoflife.org",
  collectory: "https://collectory.vtatlasoflife.org"
};
