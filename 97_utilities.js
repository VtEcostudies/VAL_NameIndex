
function log(out, stream, override=false) {
  if (override) {console.log(out);}
  if (stream) stream.write(`${out}\n`);
}

function logErr(out, stream, override=true) {
  log(out, stream, override);
}

module.exports.log = log;
module.exports.logErr = logErr;
