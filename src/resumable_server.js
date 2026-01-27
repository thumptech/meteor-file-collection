//###########################################################################
//     Copyright (C) 2014-2017 by Vaughn Iverson
//     fileCollection is free software released under the MIT/X11 license.
//     See included LICENSE file for details.
//###########################################################################

//import {Mongo, MongoInternals} from 'meteor/mongo';

import {MongoClient, ObjectId, GridFSBucket} from 'mongodb';
import async from 'async';

if (Meteor.isServer) {
    // This function checks to see if all of the parts of a Resumable.js uploaded file are now in the gridFS
    // Collection. If so, it completes the file by moving all of the chunks to the correct file and cleans up
    const check_order = function (file, callback) {

        //const fileId = new Mongo.ObjectID(file.metadata._Resumable.resumableIdentifier);
        const fileId = new ObjectId(file._id.toHexString());
        const files = this.db.collection(`${this.root}.files`);
        const main = this.db.collection(`${this.root}`);
        const query = {
            'metadata._Resumable.resumableIdentifier': file.metadata._Resumable.resumableIdentifier,
            length: {
                $ne: 0
            }
        };
        const cursor = files.find(
            query,
            {
                fields: {
                    length: 1,
                    metadata: 1
                },
                sort: {
                    'metadata._Resumable.resumableChunkNumber': 1
                }
            }
        );
        //console.log("check_order: got files cursor", cursor);

        files.countDocuments(query).then(count => {
            if (!(count >= 1)) {
                cursor.close();
                return callback();
            }

            if (count !== file.metadata._Resumable.resumableTotalChunks) {
                cursor.close();
                return callback();
            }

            // Manipulate the chunks and files collections directly under write lock
            const chunks = this.db.collection(`${this.root}.chunks`);

            cursor.batchSize(file.metadata._Resumable.resumableTotalChunks + 1);

            cursor.toArray().then(parts => {
                async.eachLimit(parts, 5,
                    (part, cb) => {
                        if (!part) {
                            return cb(new Meteor.Error("Received null part"));
                        }
                        const partId = part._id;
                        async.series([
                                // Move the chunks to the correct file
                                cb => {
                                    chunks.updateMany({
                                        files_id: partId,
                                        n: 0
                                    }, {
                                        $set: {
                                            files_id: fileId,
                                            n: part.metadata._Resumable.resumableChunkNumber - 1
                                        }
                                    })
                                        .then(res => {
                                            cb()
                                        })
                                        .catch(err => cb(err));
                                },
                                // Delete the temporary chunk file documents
                                cb => {
                                    files.deleteOne({
                                        _id: partId
                                    })
                                        .then(res => {
                                            cb();
                                        })
                                        .catch(err => cb(err));
                                }
                            ],
                            (err, res) => {
                                if (err) {
                                    return cb(err);
                                }
                                if (part.metadata._Resumable.resumableChunkNumber !== part.metadata._Resumable.resumableTotalChunks) {
                                    return cb();
                                } else {
                                    // check for a final hanging gridfs chunk
                                    chunks.updateMany({files_id: partId, n: 1},
                                        {
                                            $set: {
                                                files_id: fileId,
                                                n: part.metadata._Resumable.resumableChunkNumber
                                            }
                                        }
                                    ).then(res => {
                                        cb()
                                    });
                                }
                            });
                    },
                    err => {
                        if (err) return callback(err);
                        // Update the size, this will trigger md5 generation in gridFS_server.js
                        files.updateOne({_id: fileId}, {
                                $set: {
                                    length: file.metadata._Resumable.resumableTotalSize,
                                }
                            },
                        ).then(res => {
                           // console.log(`...done`, res);
                            callback();
                        }).catch(err => callback(err));

                    });
            }).catch(err => {
                return callback(err);
            });
        }).catch(err => {
            return callback(err);
        });

    };

    // Handle HTTP POST requests from Resumable.js

    const resumable_post_lookup = (params, query, multipart) => {
        return {
            _id: share.safeObjectID(multipart?.params?.resumableIdentifier)
        }
    };

    const resumable_post_handler = async function (req, res, next) {
        // This has to be a resumable POST
        if (!req.multipart?.params?.resumableIdentifier) {
            console.error("Missing resumable.js multipart information");
            res.writeHead(501, share.defaultResponseHeaders);
            res.end();
            return;
        }

        const resumable = req.multipart.params;
        resumable.resumableTotalSize = parseInt(resumable.resumableTotalSize);
        resumable.resumableTotalChunks = parseInt(resumable.resumableTotalChunks);
        resumable.resumableChunkNumber = parseInt(resumable.resumableChunkNumber);
        resumable.resumableChunkSize = parseInt(resumable.resumableChunkSize);
        resumable.resumableCurrentChunkSize = parseInt(resumable.resumableCurrentChunkSize);

        if (req.maxUploadSize > 0) {
            if (!(resumable.resumableTotalSize <= req.maxUploadSize)) {
                console.error("Resumable.js upload size exceeds maxUploadSize");
                res.writeHead(413, share.defaultResponseHeaders);
                res.end();
                return;
            }
        }

        // Sanity check the chunk sizes that are critical to reassembling the file from parts
        if (((req.gridFS.chunkSize !== resumable.resumableChunkSize) ||
                (!(resumable.resumableChunkNumber <= resumable.resumableTotalChunks)) ||
                (!((resumable.resumableTotalSize / resumable.resumableChunkSize) <= (resumable.resumableTotalChunks + 1))) ||
                (resumable.resumableCurrentChunkSize !== resumable.resumableChunkSize)) &&
            ((resumable.resumableChunkNumber !== resumable.resumableTotalChunks) ||
                (!(resumable.resumableCurrentChunkSize < (2 * resumable.resumableChunkSize))))) {
            console.error("Invalid chunk sizes");
            res.writeHead(501, share.defaultResponseHeaders);
            res.end();
            return;
        }

        const chunkQuery = {
            length: resumable.resumableCurrentChunkSize,
            'metadata._Resumable.resumableIdentifier': resumable.resumableIdentifier,
            'metadata._Resumable.resumableChunkNumber': resumable.resumableChunkNumber
        };

        // This is to handle duplicate chunk uploads in case of network weirdness
        const findResult = await this.findOneAsync(chunkQuery, {fields: {_id: 1}});

        if (findResult) {
            // Duplicate chunk... Don't rewrite it.
            console.warn(`Duplicate chunk detected: ${resumable.resumableChunkNumber}, ${resumable.resumableIdentifier}`);
            res.writeHead(200, share.defaultResponseHeaders);
            return res.end();
        } else {
            // Everything looks good, so write this part
            req.gridFS.metadata._Resumable = resumable;
            const writeStream = this.upsertStream({
                filename: `_Resumable_${resumable.resumableIdentifier}_${resumable.resumableChunkNumber}_${resumable.resumableTotalChunks}`,
                metadata: req.gridFS.metadata
            });

            if (!writeStream) {
                console.error("Error writing chunk to gridFS");
                res.writeHead(404, share.defaultResponseHeaders);
                res.end();
                return;
            }

            return req.multipart.fileStream.pipe(share.streamChunker(this.chunkSize)).pipe(writeStream)
                .on('close', share.bind_env(() => {
                    // Check to see if all of the parts are now available and can be reassembled
                    return check_order.bind(this)(req.gridFS, function (err) {
                        if (err) {
                            console.error("Error reassembling chunks of resumable.js upload", err);
                            res.writeHead(500, share.defaultResponseHeaders);
                        } else {
                            res.writeHead(200, share.defaultResponseHeaders);
                        }
                        return res.end();
                    });

                }));
        }
    };

    const resumable_get_lookup = function (params, query) {
        return {_id: share.safeObjectID(query.resumableIdentifier)};
    };

    // This handles Resumable.js "test GET" requests, that exist to determine
    // if a part is already uploaded. It also handles HEAD requests, which
    // should be a bit more efficient and resumable.js now supports
    const resumable_get_handler = async function (req, res, next) {
        const {
            query
        } = req;
        const chunkQuery = {
            $or: [
                {
                    _id: share.safeObjectID(query.resumableIdentifier),
                    length: parseInt(query.resumableTotalSize)
                },
                {
                    length: parseInt(query.resumableCurrentChunkSize),
                    'metadata._Resumable.resumableIdentifier': query.resumableIdentifier,
                    'metadata._Resumable.resumableChunkNumber': parseInt(query.resumableChunkNumber)
                }
            ]
        };

        const result = await this.findOneAsync(chunkQuery, {fields: {_id: 1}});
        if (result) {
            // Chunk is present
            res.writeHead(200, share.defaultResponseHeaders);
        } else {
            // Chunk is missing
            res.writeHead(204, share.defaultResponseHeaders);
        }

        return res.end();
    };

    // Setup the GET and POST HTTP REST paths for Resumable.js in express
    share.resumablePaths = [
        {
            method: 'head',
            path: share.resumableBase,
            lookup: resumable_get_lookup,
            handler: resumable_get_handler
        },
        {
            method: 'post',
            path: share.resumableBase,
            lookup: resumable_post_lookup,
            handler: resumable_post_handler
        },
        {
            method: 'get',
            path: share.resumableBase,
            lookup: resumable_get_lookup,
            handler: resumable_get_handler
        }
    ];
}
