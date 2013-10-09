stream = require 'stream'

 # This needs fixed to handle the last line in a file when there is no newline
class NewlineSplitter extends stream.Transform
  constructor: ->
    @_buffer = ""
    super

  _transform: (chunk, encoding, callback) ->
    @_buffer += chunk.toString()

    if @_buffer.indexOf("\n") != -1
      parts = @_buffer.split("\n")
      for part in parts[0...parts.length - 1]
        @push(part)
      @_buffer = parts[parts.length - 1]
    callback()

class JsonParser extends stream.Transform
  constructor: (options) ->
    super

  _transform: (chunk, encoding, callback) ->
    try
      @push JSON.parse(chunk)
    catch error
      console.log "ERROR: #{error}, CHUNK: ", chunk
    callback()

class NarratusClassifier extends stream.Transform

  _transform: (json, encoding, callback) ->
    banHammer.checkIt json, (data)->
      callback()

class BanHammer
  
  constructor: ->
    @store = {}
    @banHammered = []

  checkIt: (json, callback) ->
    return if @banHammered.indexOf(json.user_id) != -1
    if @store[json.user_id]?.reqs
      @store[json.user_id].reqs.push(json.ts)
      # console.log "size", @store[json.user_id].reqs.length
      numRequests = @store[json.user_id].reqs.length
      if numRequests > 5
        latestReq = @store[json.user_id].reqs[numRequests-1]
        firstReq = @store[json.user_id].reqs[numRequests-5]
        if latestReq - firstReq < 10
          @banHammered.push(json.user_id)
          console.log(json.user_id, "BAN HAMMERED!", "5 or more requests in ", (latestReq - firstReq), "ms")
    else
      @store[json.user_id] = {reqs: [json.ts], timer: @setupReqTimer(json)}
      
    
    callback(@store[json.user_id])
    
  setupReqTimer: (json) ->
    store = @store
    timer = setTimeout (->
      store[json.user_id] = {reqs: []}
    ), 1000
    timer
    
class JsonEncoder extends stream.Transform
  _transform: (obj, encoding, callback) ->
    @push JSON.stringify(obj) + "\n"
    callback()


banHammer = new BanHammer()

process.stdin
.pipe(new NewlineSplitter(encoding: 'utf8'))
.pipe(new JsonParser(objectMode: true))
.pipe(new NarratusClassifier(objectMode: true))
.pipe(new JsonEncoder(objectMode: true))
.pipe(process.stdout)
