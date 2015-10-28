var express = require('express');
var mysql = require('mysql');
var config = require('../config/config');

var router = express.Router();


var pool = mysql.createPool({
    host: config.db.host,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    port: config.db.port
});

router.get('/wustl/:shadow/:db/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log("wustl",req.params);
    switch (req.params['file']) {
        case "trackDb.txt":
            get_experiments(function(rows){
                res.render('track_wustl', {"data": rows,"baseurl":config.hub.baseurl});
            },req.params['shadow'],req.params['db'],next);
            break;
        default:
            next();
            break;
    }

}).get('/:shadow/:id/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log(req.params);

    switch (req.params['file']) {
        case "hub.txt":
            pool.getConnection(function (err, connection) {
                connection.query(
                    'SELECT name,COALESCE(description,"SciDAP") as description from egroup where shadow=?', [req.params['shadow']], function (err, rows, fields) {
                        if (err == null && rows.length > 0)
                            res.render('hub', {'name': rows[0].name, 'description': rows[0].description});
                        //res.render('hub',{'name':rows[0].name.replace(/ /g,'_'),'description':rows[0].description});
                        connection.release();
                    });
            });
            break;
        case "genomes.txt":
            pool.getConnection(function (err, connection) {
                connection.query(
                    'SELECT db from genome where id in (select distinct genome_id FROM labdata where egroup_id = (select id from egroup where shadow=?))', [req.params['shadow']],
                    function (err, rows, fields) {
                        if (!err)
                            res.render('genomes', {"data": rows});
                        connection.release();
                    });
            });
            break;
        default:
            next();
            break;
    }
}).get('/:shadow/:id/:db/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log(req.params);
    switch (req.params['file']) {
        case "trackDb.txt":
            get_experiments(function(rows){
                res.render('trackdbs', {"data": rows,"vid":req.params['id']});
            },req.params['shadow'],req.params['db'],next);
            break;
        default:
            next();
            break;
    }

});


function get_experiments(callback,shadow,db,next){
    pool.getConnection(function (err, connection) {
        connection.query(
            'SELECT l.id as id,uid,name4browser, etype ' +
            ' from labdata l ' +
            ' left join  experimenttype e on e.id=l.experimenttype_id ' +
            ' where egroup_id = (select id from egroup where shadow=?)' +
            ' and genome_id in ((select id from ems.genome where db = ?)) and deleted = 0'
            , [shadow, db],
            function (err, rows, fields) {
                connection.release();
                if (err == null && rows.length > 0) {
                    callback(rows);
                } else {
                    next();
                }

            });
    });

}

module.exports = router;
