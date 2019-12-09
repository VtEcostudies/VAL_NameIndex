
function log(out, wStream) {
  console.log(out);
  if (wStream) wStream.write(`${out}\n`);
}
