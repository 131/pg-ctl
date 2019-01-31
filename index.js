"use strict";

const fs      = require('fs');
const Pg      = require('pg-aa');
const debug   = require('debug');

const spawn   = require('child_process').spawn;

const strftime  = require('mout/date/strftime');
const get       = require('mout/object/get');

const defer     = require('nyks/promise/defer');
const promisify = require('nyks/function/promisify');
const which     = require('nyks/path/which');
const wait      = require('nyks/child_process/wait');
const sprintf   = require('nyks/string/format');
const passthru  = promisify(require('nyks/child_process/passthru'));



class Server {

  constructor(config) {
    this.config = JSON.parse(fs.readFileSync(config));
  }

  async connect_or_start_server() {

    var lnk = new Pg(this.config.admin);

    //forget this and you'll crash your main loop
    lnk.on('error', function(err) {
      console.error("FAILURE IN sql client link", err);
    });

    await  lnk.connect();
    console.error("Connected to an existing pg server instance");
    lnk.close();

    console.error("Server is up & running");

    if(!this.config.database)
      return;

    await this.init_database();
  }


  //return true if database has been created, false if already existing
  async init_database() {
    console.log('Checking main database');

    var lnk = new Pg(this.config.admin);
    //forget this and you'll crash your main loop
    lnk.on('error', function(err) {
      console.error("FAILURE IN sql client link", err);
    });

    //now that the server is ready, check for ivscsac database
    var databases = await lnk.col("pg_database", true, "datname");
    var users     = await lnk.col("pg_user", true, "usename");


    if(users.indexOf(this.config.user) == -1) {
      console.error("Should create user '%s'", this.config.user);
      await lnk.query(`CREATE USER "${this.config.user}" ${sprintf(this.config.password ? "WITH PASSWORD '%s'" : '', this.config.password)};`);
    }

    var create = databases.indexOf(this.config.database) == -1;

    if(create) {
      console.error("Should create database '%s'", this.config.database);
      await lnk.query(`CREATE DATABASE "${this.config.database}" ENCODING 'utf8';`);

      if(get(this.config, 'dbschema.type') == 'clyks') {
        var stdio = ['inherit', 'ignore', 'ignore'];
        if(debug.enabled('myks'))
          stdio = ['inherit', 'inherit', 'inherit'];

        var child = spawn(which('clyks'), [this.config.dbschema.site,  "sql", "--ir://run=init_database"], {stdio});
        await wait(child);
      }

      if(get(this.config, 'dbschema.type') == 'rawsql')
        await this.rawsql(this.config.dbschema.path, this.config.admin);

    }

    lnk.close();
    return create;
  }

  //database already exists, we just need to check the current schema
  async update_database() {
    if(this.config.dbschema.type == 'clyks')
      await passthru(which('clyks'), [this.config.dbschema.site,  "sql", "--ir://run=update_database"]);
  }



  async dumpdb(datname, outfile) /**
  * @param {string} [datname=]
  * @param {string} [outfile=-]
  */{
    try {
      outfile = strftime(new Date(), outfile);
      datname = datname || this.config.database;
      var target = outfile == "-" ? process.stdout :  fs.createWriteStream(outfile);
      var defered = defer();
      var child = spawn(which('pg_dump'), ['-h', this.config.admin.host,  "-U", this.config.admin.user, datname], {stdio : ['inherit', 'pipe', 'inherit']}, defered.chain);
      child.stdout.pipe(target);
      console.error("Dumped '%s' to '%s', have a nice day", datname, outfile);
    } catch(err) {
      console.error("Could backup database", err);
    }

  }

  async dropdb(datname) {
    try {
      var lnk = new Pg(this.config.admin);
      //kick all previous user
      await lnk.select("pg_stat_activity", {datname}, "pg_terminate_backend(pid)");
      await lnk.query(`DROP DATABASE IF EXISTS "${datname}" `);

    } finally {
      lnk.close();
    }
  }

  async rotate_db(current_database_name, archive_database_name) {
    // Move and recreate database
    await this.dropdb(archive_database_name);

    try {
      var lnk = new Pg(this.config.admin);

      await lnk.select("pg_stat_activity", {datname : current_database_name}, "pg_terminate_backend(pid)");
      await lnk.query(`ALTER DATABASE "${current_database_name}" RENAME TO "${archive_database_name}" `);
    } finally {
      lnk.close();
    }

    await this.init_database();
  }


  async rawsql(mock_data, config) /**
  * @alias populate
  * @param object [config=]
  */ {

    try {
      config = config || this.config;
      config.database = this.config.database;
      var querymode = this.config.querymode || "psql";
      console.log("Working with raw file in query mode '%s'", querymode);
      if(querymode == "psql") {
        var args =  ["-U", config.user, "-h", config.host, "-f", mock_data, config.database];
        var psql_bin = which('psql');

        var stdio = ['inherit', 'ignore', 'ignore'];
        if(debug.enabled('myks'))
          stdio = ['inherit', 'inherit', 'inherit'];

        var child = spawn(psql_bin, args, {stdio});
        await wait(child);
      } else {
        try {
          var lnk = new Pg(config);
          //kick all previous user
          var contents = fs.readFileSync(mock_data, 'utf-8');
          await lnk.query(contents);
        } finally {
          lnk.close();
        }
      }
    } catch(err) {
      console.error("Could not populate db with mock data", err);
    }

    /*  //works well, yet, not in docker (psql binary is unavailable)
    try {


    } catch(err) {
      console.error("Could not populate db with mock data", psql_bin, args, err);
    }
*/
  }


  stop() {
    setTimeout(function() {
      process.exit();
    }, 1000);
  }

}

module.exports = Server;
