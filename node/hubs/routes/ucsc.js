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
var router_track = express.Router();
var config = require('../config/config');

// res.writeHead(200, { 'Content-Type': 'text/html' });

router
    .get('/*', function (req, res, next) {
        res.set('Content-Type', 'text/plain');
        console.log("all");
        next();
    }
)
    .get('/:shadow/:file', function (req, res, next) {
        //res.set('Content-Type', 'text/plain');
        console.log(req.params);
        var id = req.params['file'].replace(".txt", "");
        if (id && Number(id)) {
            get_hub(function (rows) {
                res.render('hub', {'name': rows[0].name, 'description': rows[0].description, 'id': id});
            }, req.params['shadow'], next);
            return;
        }
        next();
    })
    .get('/:shadow/:id/:file', function (req, res, next) {
        //res.set('Content-Type', 'text/plain');
        console.log(req.params);
        if (req.params['file'] != "genomes.txt") next();
        get_genomes(function (rows) {
            res.render('genomes', {'data': rows, 'id': req.params['id']});
        }, req.params['shadow'], next);
    })
    .get('/:shadow/:id/:db/:file', function (req, res, next) {
        //res.set('Content-Type', 'text/plain');
        console.log(req.params);
        if (req.params['file'] != "trackDb.txt") next();
        get_experiments(function (rows) {
            res.render('trackdbs', {"data": rows, "vid": req.params['id']});
        }, req.params['shadow'], req.params['db'], next);

    });


router_track.get('/:shadow/:id/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');
    console.log(req.params);
    if (req.params['file']) {
        get_track(function (rows) {
            if (rows.length > 0) {
                res.render('track_ucsc', {"data": rows[0], "baseurl": config.hub.baseurl});
            } else {
                next();
            }
        }, req.params['shadow'], req.params['id'], next);

        return;
    }
    next();

});

module.exports.tracks = router;
module.exports.track = router_track;
