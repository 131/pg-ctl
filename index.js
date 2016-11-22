"use strict";

const fs      = require('fs');
const path    = require('path');
const defer   = require('nyks/promise/defer');
const sleep   = require('nyks/function/sleep');

const server = require('pg-server-9.5-win-x86');
const pg     = require('pg-co');
const promisify = require('nyks/function/promisify');
const which = require('nyks/path/which');

const passthru = promisify(require('nyks/child_process/passthru'));



class Server {

  constructor(config) {
    this.config = JSON.parse(fs.readFileSync(config));
  }

  * run() {

    var lnk = new pg(this.config.admin);

      //forget this and you'll crash your main loop
    lnk.on('error', function(err) {
      console.log("FAILURE IN sql client link");
    });

      //maybe the server is already running (!)
    try {
      yield  lnk.connect();
      console.log("Connected to an existing pg server instance");
    } catch(err) {
      let defered = defer();

      server(this.config.admin.dataDir, defered.chain);
      var instance = yield defered;

      instance.stderr.pipe(process.stderr);
      instance.stdout.pipe(process.stdout);

      this.instance = instance;

      console.log("Server is running, trying to connect");
      var tries = 5;


      while(true) {
        if(!tries --)
          throw "Could not connect";

        try {
          yield  lnk.connect();
          break;
        } catch(err) { yield sleep(1500); }
      }
    }

    //now that the server is ready, check for ivscsac database
    var databases = yield lnk.col("pg_database", true, "datname");
    var users     = yield lnk.col("pg_user", true, "usename");


    if(users.indexOf(this.config.dbuser) == -1) {
      console.log("Should create user '%s'", this.config.dbuser);
      yield lnk.query(`CREATE USER "${this.config.dbuser}";`);  //WITH PASSWORD \'%s\''
    }


    if(databases.indexOf(this.config.dbname) == -1) {
      console.log("Should create database '%s'", this.config.dbname);
      yield lnk.query(`CREATE DATABASE "${this.config.dbname}" ENCODING 'utf8';`);

      if(this.config.dbschema.type == "clyks")
        yield passthru(which("clyks"), [this.config.dbschema.site,  "sql", "--ir://run=init_database"]);
    }

    lnk.close();
  }

  * populate() {
    try {
      var psql_path = path.resolve(__dirname, "node_modules/pg-server-9.5-win-x86/server/bin/psql.exe");
      var mock_data = './test/mock/ta.sql';
      yield passthru(psql_path, ["-U", "postgres",  "-f", mock_data, this.config.dbname])
    } catch(err) {
      console.error("Could not populate db with mock data", err);
    }
  }

  stop() {
    if(this.instance) {
      this.instance.softkill();
      console.log("Server killed, bye");
    }

    setTimeout(function(){
      process.exit();
    }, 1000);
  }

}


module.exports = Server;