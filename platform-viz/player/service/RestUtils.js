/*
 * Copyright (c) 2013-2014 Canopus Consulting. All rights reserved.
 *
 * This code is intellectual property of Canopus Consulting. The intellectual and technical
 * concepts contained herein may be covered by patents, patents in process, and are protected
 * by trade secret or copyright law. Any unauthorized use of this code without prior approval
 * from Canopus Consulting is prohibited.
 */

/**
 * Helper class to invoke MW API's
 *
 * @author Santhosh
 */
var Client = require('node-rest-client').Client;
var client = new Client();

exports.postCall = function(url, arguments, callback) {
    var args = {
        path: arguments.path,
        headers: {
            "Content-Type": "application/json"
        },
        data: arguments.data
    };
    client.post(url, args, function(data, response) {
        parseResponse(data, callback);
    }).on('error', function(err) {
        callback(err);
    });
}

exports.patchCall = function(url, arguments, callback) {
    var args = {
        path: arguments.path,
        headers: {
            "Content-Type": "application/json"
        },
        data: arguments.data
    };
    client.patch(url, args, function(data, response) {
        parseResponse(data, callback);
    }).on('error', function(err) {
        callback(err);
    });
}

function parseResponse(data, callback) {
    if(typeof data == 'string') {
        try {
            data = JSON.parse(data);
            callback(null, data.result);
        } catch(err) {
            console.log('RestUtils.parseResponse(). Err', err);
            callback(err);
        }
    } else {
        callback(null, data.result);
    }
}

exports.getCall = function(url, arguments, callback) {
    var args = {
        path: arguments.path,
        headers: {
            "accept": "application/json"
        },
        parameters: arguments.parameters
    };
    client.get(url, args, function(data, response) {
        parseResponse(data, callback);
    }).on('error', function(err) {
        callback(err);
    });
}

exports.putCall = function(url, arguments, callback) {
    var args = {
        path: arguments.path,
        headers: {
            "Content-Type": "application/json"
        },
        parameters: arguments.data
    };
    client.put(url, args, function(data, response) {
        parseResponse(data, callback);
    }).on('error', function(err) {
        callback(err);
    });
}

exports.deleteCall = function(url, arguments, callback) {

    var args = {
        path: arguments.path,
        headers: {
            "accept": "application/json"
        },
        data: arguments.data
    };
    client.delete(url, args, function(data, response) {
        callback(null, data);
    }).on('error', function(err) {
        callback(err);
    });
}
