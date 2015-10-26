var express = require('express');
var mysql = require('mysql');
var config = require('../config/config');

var router = express.Router();

/* HUB GET home page. */
//router.get('/:requesthub', function(req, res, next) {
//    res.render('index', { title: 'Express',requesthub: req.params['requesthub'] });
//});

var pool  = mysql.createPool({
    host: config.db.host,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    port: config.db.port
});

router.get('/:shadow/:file', function(req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log(req.params);

    switch(req.params['file']){
        case "hub.txt":
            pool.getConnection(function(err, connection) {
                connection.query(
                    'SELECT name,COALESCE(description,"SciDAP") as description from egroup where shadow=?',[req.params['shadow']], function (err, rows, fields) {
                        if (err == null && rows.length > 0)
                            res.render('hub',{'name':rows[0].name,'description':rows[0].description});
                            //res.render('hub',{'name':rows[0].name.replace(/ /g,'_'),'description':rows[0].description});
                        connection.release();
                    });
            });
            break;
        case "genomes.txt":
            pool.getConnection(function(err, connection) {
                connection.query(
                    'SELECT db from genome where id in (select distinct genome_id FROM labdata where egroup_id = (select id from egroup where shadow=?))',[req.params['shadow']],
                    function (err, rows, fields) {
                        if (!err)
                            res.render('genomes',{"data": rows});
                        connection.release();
                    });
            });
            break;
        default:
            next();
            break;
    }
}).get('/:shadow/:db/:file', function(req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log(req.params);
    switch(req.params['file']){
        case "trackDb.txt":
            pool.getConnection(function(err, connection) {
                connection.query(
                    'SELECT id,uid,name4browser from labdata where egroup_id = (select id from egroup where shadow=?) and genome_id in ' +
                    '((select id from ems.genome where db = ?)) and deleted = 0',[req.params['shadow'],req.params['db']],
                    function (err, rows, fields) {
                        console.log(err,rows,req.params);
                        connection.release();
                        if (err == null && rows.length > 0) {
                            res.render('trackdb', {"data": rows});
                        } else {
                            next();
                        }

                    });
            });
            break;
        default:
            next();
            break;
    }

});

module.exports = router;
