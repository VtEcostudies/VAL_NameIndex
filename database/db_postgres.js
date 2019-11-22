const config = require('../config.json');
const { Pool } = require('pg'); //a Postgres Connection Pool
const connPool = new Pool(config.pg); //connect to a specific database with a specific user. defined in config.json.
var types = require('pg').types

/*
 * Fix date display error.
 * Simply by adding a function to return the raw value, we
 * override the pg_postgres default behavior, which mucks
 * date-only values by ‘correcting’ for local TZ. This is
 * confusing, because pg_postgres lives on the server, whose
 * TZ is UTC. It may be that moment(), running on the client
 * cannot properly process dates that contain an explicit
 * UTC TZ indicator with time set to 00:00, causing a correction
 * for TZ EST by subtracting 5 hours from midnight. In theory,
 * this would set the UI time back to the previous evening at 7P.
 *
 * pg_types uses postgres OIDs to identify db data types:
 *
 * date OID=1082
 * timestamp OID=1114
 *
*/
parseDate = function(val) {
  //NOTE: this log is hit 2x per row. HUGE API performance hit.
  //console.log('db_postgres.parseDate', val);
  return val;
}

types.setTypeParser(1082, parseDate);

/*
  This is it.
 */
module.exports = {

  query: (text, params) => connPool.query(text, params)

};
