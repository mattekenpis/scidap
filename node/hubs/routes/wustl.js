/**
 * Created by porter on 10/28/15.
 */

var express = require('express');
var config = require('../config/config');
var get_experiments = require('../database/mysql').get_experiments;

var router = express.Router();


router.get('/:shadow/:db/:file', function (req, res, next) {
    res.set('Content-Type', 'text/plain');

    console.log("wustl", req.params);
    switch (req.params['file']) {
        case "trackDb.txt":
            get_experiments(function (rows) {
                res.render('tracks_wustl', {"data": rows, "baseurl": config.hub.baseurl});
            }, req.params['shadow'], req.params['db'], next);
            break;
        default:
            next();
            break;
    }
});

module.exports.tracks = router;
