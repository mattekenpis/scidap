/**
 * Created by porter on 10/26/15.
 */

var path = require('path'),
    rootPath = path.normalize(__dirname + '/..'),
    env = process.env.NODE_ENV || 'development',
    fs = require('fs'),
    confPath = env === 'development' ? rootPath + '/wardrobe.json' : '/etc/wardrobe/wardrobe.json',
    conf;

///**
// * Wrapping reading of json in case configuration is passed as a deamon argument and will be parsed later
// */
try {
    conf = JSON.parse(fs.readFileSync(confPath).toString());
} catch (e){
    conf = {
        mail: {},
        db:{
            host:"localhost",
            user: "",
            password: "",
            database: "ems",
            port:3306
        }
    };
}

module.exports = conf;