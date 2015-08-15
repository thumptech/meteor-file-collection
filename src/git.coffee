############################################################################
#     Copyright (C) 2015 by Vaughn Iverson
#     file-collection is free software released under the MIT/X11 license.
#     See included LICENSE file for details.
############################################################################

if Meteor.isServer

   gbs = Npm.require 'git-blob-stream'

   share.Git = class Git

      constructor: (@fC, @repo = '') ->
         unless @ instanceof Git
            return new Git(@fC, @repo)

         unless @fC instanceof FileCollection
            throw new Error "Git error: Invalid fileCollection provided"

         unless typeof @repo is 'string'
            throw new Error "Git error: Invalid repository name provided"

         @prefix = "#{@repo}.git"

         # Initialize the repo if necessary
         unless @_readHead()
            @_writeHead 'ref: refs/heads/master'
            @_updateServerInfo()

      _objPath: (hash) ->
         return "objects/#{hash.slice(0,2)}/#{hash.slice(2)}"

      _updateServerInfo: () ->
         Async.runSync (done) =>
            query =
               filename:
                  $regex: new RegExp "^#{@prefix}/refs/"
            refs = ""
            @fC.find(query).forEach (d) ->
               console.log "%%%%%%%%%%%%%%%%%%%%%%%", d
               refs += "#{d.metadata._Git.ref}\t#{d.filename.slice(d.filename.indexOf('/')+1)}\n"
            name = "#{@prefix}/info/refs"
            query =
               _id: name
               filename: name
               metadata:
                  _Git:
                     type: 'refs'
            outStream = @fC.upsertStream query, (err, f) =>
               console.dir f
               done err, f
            console.log "Here are the refs!", refs
            outStream.end refs

      _readHead: () ->
         query =
            _id: "#{@prefix}/HEAD"
         ref = @fC.findOne query
         if ref
            return ref.metadata._Git.ref
         else
            console.warn "Missing HEAD"
            return null

      _writeHead: (ref) ->
         Async.runSync (done) =>
            unless ref and typeof ref is 'string'
               done new Error "_writeHead must have a valid reference"
            query =
               _id: "#{@prefix}/HEAD"
               filename: "#{@prefix}/HEAD"
               metadata:
                  _Git:
                     type: 'HEAD'
                     ref: "#{ref}"
            outStream = @fC.upsertStream query, (err, f) =>
               console.dir f
               done err, f
            outStream.end "#{ref}\n"

      _readRef: (ref) ->
         query =
            _id: "#{@prefix}/#{ref}"
         ref = @fC.findOne query
         if ref
            return ref.metadata._Git.ref
         else
            console.warn "Missing Ref"
            return null

      _writeRef: (ref, commit) ->
         Async.runSync (done) =>
            unless ref and typeof ref is 'string'
               done new Error "_writeRef must have a valid reference"
            unless commit and typeof commit is 'string' and commit.length is 40
               done new Error "_writeRef must have a valid commit"
            name = "#{@prefix}/#{ref}"
            query =
               _id: name
               filename: name
               metadata:
                  _Git:
                     type: 'ref'
                     ref: commit
            outStream = @fC.upsertStream query, (err, f) =>
               done err if err
               console.dir f
               @_updateServerInfo()
               done null, f
            outStream.end "#{commit}\n"

      _writeTree: (tree) ->
         Async.runSync (done) =>
            console.log "Making a tree!"
            data = Async.wrap(gbs.treeWriter) tree, { arrayTree: true, noOutput: true }
            console.log "tree should be: #{data.hash}, #{data.size}"
            name = "#{@prefix}/#{@_objPath data.hash}"
            console.log "!!! 1", name
            if @fC.findOne { _id: name }
               done null, data
            else
               console.log "!!! 2"
               outStream = @fC.upsertStream
                     _id: name
                     filename: name
                     metadata:
                        _Git:
                           type: 'tree'
                           size: data.size
                           tree: data.tree
                  , (err, f) =>
                     console.log "!!! 5"
                     console.dir f, { depth: null }
                     console.log "#{data.hash} written! as #{f._id}", err
                     done err, data
               console.log "!!! 3"
               gbs.treeWriter(tree).pipe(outStream)
               console.log "!!! 4"

      _writeCommit: (commit) ->
         Async.runSync (done) =>
            console.log "Making a commit!", commit
            data = Async.wrap(gbs.commitWriter) commit, { noOutput: true }
            console.log "commit should be: #{data.hash}, #{data.size}"
            name = "#{@prefix}/#{@_objPath data.hash}"
            if @fC.findOne { _id: name }
               done null, data
            else
               outStream = @fC.upsertStream
                     _id: name
                     filename: name
                     metadata:
                        _Git:
                           type: 'commit'
                           size: data.size
                           commit: data.commit
                  , (err, f) =>
                     console.dir f.metadata._Git.commit, { depth: null }
                     console.log "#{data.hash} written! as #{f._id}", err
                     done null, data
               gbs.commitWriter(commit).pipe(outStream)

      _writeTag: (tag) ->
         Async.runSync (done) =>
            console.log "Making a tag!"
            data = Async.wrap(gbs.tagWriter) tag, { noOutput: true }
            console.log "tag should be: #{data.hash}, #{data.size}"
            name = "#{@prefix}/#{@_objPath data.hash}"
            if @fC.findOne { _id: name }
               done null, data
            else
               outStream = @fC.upsertStream
                     _id: name
                     filename: name
                     metadata:
                        _Git:
                           type: 'tag'
                           size: data.size
                           tag: data.tag
                  , (err, f) =>
                     console.dir f.metadata._Git.tag, { depth: null }
                     console.log "#{data.hash} written! as #{f._id}", err
                     @_writeRef "refs/tags/#{tag.tag}", tag.object
                     done null, data
               gbs.tagWriter(tag).pipe(outStream)

      _checkFile: (callback) ->
         return gbs.blobWriter { type: 'blob', noOutput: true }, Meteor.bindEnvironment (err, data) =>
            if err
               return callback err
            name = "#{@prefix}/#{@_objPath data.hash}"
            if @fC.findOne { _id: name }
               callback null, false
            else
               callback null, data

      _writeFile: (data, callback) ->
         name = "#{@prefix}/#{@_objPath data.hash}"
         bw = gbs.blobWriter
               type: 'blob'
               size: data.length
            , (err, obj) =>
               console.dir obj
               callback? err, obj
         outStream = @fC.upsertStream
               _id: name
               filename: name
               metadata:
                  _Git:
                     type: 'blob'
                     size: data.length
            , (err, f) =>
               console.dir f, { depth: null }
               console.log "#{data.hash} written! as #{f._id}", err
         bw.pipe(outStream)
         return bw