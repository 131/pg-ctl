"use strict";

const fs      = require('fs');
const path    = require('path');
const defer   = require('nyks/promise/defer');
const sleep   = require('nyks/function/sleep');
const strftime   = require('mout/date/strftime');
const pg      = require('pg-co');
const promisify = require('nyks/function/promisify');
const which     = require('nyks/path/which');
const spawn    = require('child_process').spawn;
const passthru  = promisify(require('nyks/child_process/passthru'));
const sprintf = require('nyks/string/format');


if(process.platform == 'win32')
  require('nyks/path/extend')(path.resolve(__dirname, 'node_modules/pg-server-9.5-win-x86/server/bin'));


class Server {

  constructor(config) {
    this.config = JSON.parse(fs.readFileSync(config));
  }

  * connect_or_start_server() {

    var lnk = new pg(this.config.admin);

      //forget this and you'll crash your main loop
    lnk.on('error', function(err) {
      console.error("FAILURE IN sql client link", err);
    });

      //maybe the server is already running (!)
    try {
      yield  lnk.connect();
      console.error("Connected to an existing pg server instance");
    } catch(err) {
      let defered = defer();
      let server = require('pg-server-9.5-win-x86');

      server(this.config.admin.dataDir, defered.chain);
      var instance = yield defered;

      instance.stderr.pipe(process.stderr);
      instance.stdout.pipe(process.stdout);

      this.instance = instance;

      console.error("Server is running, trying to connect");
      var tries = 5;

      yield sleep(1500);
      while(true) {

        if(!tries --)
          throw "Could not connect";

        try {
          yield  lnk.connect();
          break;
        } catch(err) { yield sleep(1500); }
      }
    }
    lnk.close();

    console.error("Server is up & running");

    if (!this.config.database)
      return;

    yield this.init_database();
  }


    //return true if database has been created, false if already existing
  * init_database() {
    console.log('Checking main database');

    var lnk = new pg(this.config.admin);
      //forget this and you'll crash your main loop
    lnk.on('error', function(err) {
      console.error("FAILURE IN sql client link");
    });

    //now that the server is ready, check for ivscsac database
    var databases = yield lnk.col("pg_database", true, "datname");
    var users     = yield lnk.col("pg_user", true, "usename");


    if(users.indexOf(this.config.user) == -1) {
      console.error("Should create user '%s'", this.config.user);
      yield lnk.query(`CREATE USER "${this.config.user}" ${sprintf(this.config.password ? "WITH PASSWORD '%s'" : '', this.config.password)};`);
    }

    var create = databases.indexOf(this.config.database) == -1;

    if(create) {
      console.error("Should create database '%s'", this.config.database);
      yield lnk.query(`CREATE DATABASE "${this.config.database}" ENCODING 'utf8';`);

      if(this.config.dbschema.type == 'clyks')
        yield passthru(which('clyks'), [this.config.dbschema.site,  "sql", "--ir://run=init_database"]);

      if(this.config.dbschema.type == 'rawsql')
        yield this.populate(this.config.dbschema.path);

    }

    lnk.close();
    return create;
  }

    //database already exists, we just need to check the current schema
  * update_database() {
    if(this.config.dbschema.type == 'clyks')
      yield passthru(which('clyks'), [this.config.dbschema.site,  "sql", "--ir://run=update_database"]);
  }



  * dumpdb(datname, outfile) /**
  * @param {string} [datname=]
  * @param {string} [outfile=-]
  */{
    try {
      outfile = strftime(new Date(), outfile);
      datname = datname || this.config.database;
      var target = outfile == "-" ? process.stdout :  fs.createWriteStream(outfile);
      var defered = defer();
      var child = spawn(which('pg_dump'), ['-h', this.config.admin.host,  "-U", this.config.admin.user, datname] , {stdio : ['inherit', 'pipe', 'inherit']}, defered.chain);
      child.stdout.pipe(target);
      console.error("Dumped '%s' to '%s', have a nice day", datname, outfile);
    } catch(err) {
      console.error("Could backup database", err);
    }

  }

  * dropdb(datname) {
    try {
      var lnk = new pg(this.config.admin);
        //kick all previous user
      yield lnk.select("pg_stat_activity", {datname}, "pg_terminate_backend(pid)");
      yield lnk.query(`DROP DATABASE IF EXISTS "${datname}" `);

    } finally {
      lnk.close();
    }
  }

  * rotate_db(current_database_name, archive_database_name) {
    // Move and recreate database
    yield this.dropdb(archive_database_name);

    try {
      var lnk = new pg(this.config.admin);
      yield lnk.query(`ALTER DATABASE "${current_database_name}" RENAME TO "${archive_database_name}" `);
    } finally {
     lnk.close();
    }

    yield this.init_database();
  }


  * populate(mock_data) {
    try {
      var args =  ["-U", "postgres",  "-f", mock_data, this.config.database],
           psql_bin = which('psql');
      yield passthru(psql_bin, args)

    } catch(err) {
      console.error("Could not populate db with mock data", psql_bin, args, err);
    }
  }

  stop() {
    if(this.instance) {
      this.instance.softkill();
      console.error("Server killed, bye");
    }

    setTimeout(function(){
      process.exit();
    }, 1000);
  }

}


module.exports = Server;
