const query = require('./db_postgres').query;

module.exports = {
  getColumns: (tableName, columns, types) => getColumns(tableName, columns, types),
  whereClause: (params, columns, types) => whereClause(params, columns, types),
  parseColumns: (body, idx, cValues, staticColumns, staticTypes) => parseColumns(body, idx, cValues, staticColumns, staticTypes)
}

/*
    Load just columns from the db and return array of columns.

    CORRECTION: it DOES NOT WORK to return an array.

    HOWEVER: it does work to pass an array as an argument to
    this funtion, by reference, and update that array here.

    OPTIONS: (1) Pass an empty array to be filled here, or
    (2) Use the object returned from here.

 */
async function getColumns(tableName, columns=[], types=[]) {

    const text = `select * from ${tableName} limit 0;`;

    await query(text)
        .then(res => {
            //console.log(res);
            res.fields.forEach(fld => {
                columns.push(String(fld.name));
                types.push(fld.dataTypeID);
            });
            //console.log(`${tableName} columns:`, columns);
            //console.log(`${tableName} type OIDs:`, types);
            return {tableName: columns, types: types};
        })
        .catch(err => {
            throw err;
        });
}

/*
    Parse route query params into valid pg-Postgres where clause parameter list.
    This returns an object having where-clause text and values, which looks like
    the following:

    text: WHERE "column1" = $1 AND "column2" LIKE $2 AND ...

    values: []

    We created a home-grown syntax for sending a logical comparison operator to
    this API using the pipe ("|") when an operator other than "=" is desired. An
    example is:

    GET http://vpatlas.org/pools/mapped/page?mappedPoolId|LIKE='AAA' (roughly)

    TO-DO: find a way to enable the IN operator. As currently implemented, IN
    can't wlgc because node-postgres automatically applies single quotes around
    parameter values. If we receive an http request like

    GET http://vpatlas.org/pools/mapped/page?mappedPoolStatus|IN=(Potential,Probable)

    Parsing here leads pg-postgres to send the values like

    ['(Potential,Probable)', ...]

    ...when what's needed is

    [('Potential','Probable'), ...]

    Arguments:
       params: a valid express query param object
       staticColumns: array of valid columns in the table

    NOTE: Through mistakes made in trying to send operators using the field 'logical',
    discovered a way to send IN params: send the same field N times. The express parser
    puts the N different values for a repeated argument into a sub-array of values for us.

 */
function whereClause(params={}, staticColumns=[], staticTypes=[]) {
    var where = '';
    var values = [];
    var idx = 1;
    if (Object.keys(params).length) {
        for (var key in params) {
            console.log('key', key);
            var col = key.split("|")[0];
            var opr = key.split("|")[1];
            opr = opr ? opr : '='; //default operator is '='
            opr = opr==='!' ? '!=' : opr; //turn '!' operator into its intended operator: '!='
            if (staticColumns.includes(col) || 'logical'===col.substring(0,7)) {
                if (where == '') where = 'where';
                if ('logical'!=col.substring(0,7)) {
                  if (Array.isArray(params[key])) { //search token has multiple values, passed as array
                    params[key].forEach((item, index) => {
                      values.push(item);
                    });
                  } else {
                    values.push(params[key]); //not an array of values
                  }
                }
                //if (idx > 1) where += ` AND `;
                if (col.includes(`."`)) {
                  where += ` ${col} ${opr} $${idx++}`; //columns with table spec have double-quotes already
                } else if ('logical'===col.substring(0,7)) {
                  where += ` ${params[key]} `; //append logical operator
                } else if (Array.isArray(params[key])) { //break array of values into list like '($2,$3,...)'
                  if (Array.isArray(params[key])) { //search token has multiple values, passed as array
                    where += ` "${col}" ${opr} (`; //() around array of args
                    params[key].forEach((item, index) => {
                      where += index>0 ? ',': '';
                      where += `$${idx++}`;
                    });
                    where += `)`;
                  } else {
                    where += ` "${col}" ${opr} $${idx++}`; //add double-quotes to plain columns
                  }
                } else {
                  where += ` "${col}" ${opr} $${idx++}`; //add double-quotes to plain columns
                }
            }
          console.log('where', where);
        }
    }
    return { 'text': where, 'values': values };
}

/*
    Parse {column:value, ...} pairs from incoming http req.body object into structures used by postgres

    This wlgcs for postgres INSERT and UPDATE queries by allowing for injection of a starting index and
    pre-populated array of values.

    Arguments:

    body: an express req.body object
    idx: positive integer starting value for the returned 'numbered' value list
    cValues: empty or pre-populated array of query values
    staticColumns: array of valid columns in the table

    returns object having:
    {
        'named': "username,email,zipcode,..."
        'numbered': $1,$2,$3,...
        'values': ['jdoh','jdoh@dohsynth.com','91837',...]
    }

    new: handles array of strings type, OID=1015
    incoming csv values like 'one, two, three' converted to {"one","two","three"}
 */
function parseColumns(body={}, idx=1, cValues=[], staticColumns=[], staticTypes=[]) {
    var cNames = ''; // "username,email,zipcode,..."
    var cNumbr = ''; // "$1,$2,$3,..."

    //console.log(`db_pg_util.parseColumns`, body, idx, cValues, staticColumns, staticTypes);

    if (Object.keys(body).length) {
        for (var key in body) {
            if (staticColumns.includes(key)) { //test for key (db column) in staticColumns, a file-scope array of db columns generated at server startup
                if (staticTypes[staticColumns.indexOf(key)]==1015 && body[key]) { //type == array of strings
                  //make incoming one, two, three into {"one","two","three"}
                  var valArr = body[key].split(",");
                  var valList = '';
                  for (var aid in valArr) { //an array
                    valList += `"${valArr[aid]}",`
                  }
                  valList = valList.replace(/(^,)|(,$)/g, "");
                  cValues.push(`{${valList}}`);
                } else {
                  cValues.push(body[key]);
                }
                cNames += `"${key}",`;
                cNumbr += `$${idx++},`;
            } else {
              //console.log(key, 'is not in the table');
            }
        }
        //remove leading and trailing commas
        cNames = cNames.replace(/(^,)|(,$)/g, "");
        cNumbr = cNumbr.replace(/(^,)|(,$)/g, "");
    }

    return { 'named': cNames, 'numbered': cNumbr, 'values': cValues };
}
