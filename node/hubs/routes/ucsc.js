/**
 * Created by porter on 10/28/15.
 */

var express = require('express');
var db_req = require('../database/mysql');
var get_experiments = db_req.get_experiments;
var get_hub = db_req.get_hub;
var get_genomes = db_req.get_genomes;
var get_track = db_req.get_track;
var router = express.Router();
var config = require('../config/config');

router.get('/:shadow/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log(req.params);

    switch (req.params['file']) {
        case "hub.txt":
            get_hub(function (rows) {
                res.render('hub', {'name': rows[0].name, 'description': rows[0].description});
            }, req.params['shadow'], next);
            break;
        case "genomes.txt":
            get_genomes(function (rows) {
                res.render('genomes', {"data": rows});
            }, req.params['shadow'], next);
            break;
        default:
            next();
            break;
    }
}).get('/:shadow/:db/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log(req.params);
    switch (req.params['file']) {
        case "trackDb.txt":
            get_experiments(function (rows) {
                res.render('trackdbs', {"data": rows, "vid": req.params['id']});
            }, req.params['shadow'], req.params['db'], next);
            break;
        default:
            next();
            break;
    }

});

var router_track = express.Router();


router_track.get('/:shadow/:id/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');
    console.log(req.params);
    switch (req.params['file']) {
        case "track.txt":
            get_track(function (rows) {
                if(rows.length>0) {
                    res.render('track_ucsc', {"data": rows[0], "baseurl": config.hub.baseurl});
                } else {
                    next();
                }
            }, req.params['shadow'],req.params['id'], next);
            break;
        default:
            next();
            break;
    }
});

module.exports.tracks = router;
module.exports.track = router_track;
