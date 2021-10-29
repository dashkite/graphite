import {confidential} from "panda-confidential"
import {wrap, curry, pipe, flow, tee} from "@pandastrike/garden"
import {isObject, isString, include, toJSON, clone, merge} from "panda-parchment"

import Sundog from "sundog"

{Model, qv, to} = Sundog().DynamoDB()
{Message, hash} = confidential()

# Hides overlapped variable names with scoping.
protect = (f) ->
  do ({
    beginsWith
    body
    created
    direction
    edge
    expires
    limit
    origin
    range
    sort
    startKey
    stash
    target
    vertex, } = {}) -> f

validateModel = (options) ->
  unless options?
    throw new Error "Cannot establish Graphite instance for undefined model."

  {table, types={}, label} = options

  unless table?
    throw new Error "Cannot establish Graphite instance for undefined DynamoDB table name."

  for key, value of types
    if key in ["origin", "target", "typeTarget", "typeOrigin", "created", "stash", "dynamoExpires"]
      throw new Error "The key #{key} specified in model #{label} is reserved by graphite to specify a vertex/edge property."

# Wraps the Sundog DynamoDB key-value store model to handle edges.
EdgeModel = (options) ->
  validateModel options

  model =
    table: options.table
    key: ["origin", "typeTarget"]
    types: merge options.types,
      origin: "S"
      target: "S"
      typeTarget: "S"
      typeOrigin: "S"
      created: "S"
      stash: "JSON"
      dynamoExpires: "N"

  m = Model model

  # Allow parse to apply to collections of DynamoDB-typed items.
  parse = (list) -> m.parse item for item in list


  buildExpressions = protect (context) ->
    {vertex, edge, direction, sort} = context
    unless vertex && direction && vertex && sort
      throw new Error "unable to build query expression without direction, vertex, and sort combinators."

    if sort == "value" && direction == "out"
      context.index = false
      context.keyExpression = "origin = #{qv to.S vertex}"
      context.projectionExpression = "typeTarget, created, stash"

    else if sort == "value" && direction == "in"
      context.index = "InEdgesByValue"
      context.keyExpression = "target = #{qv to.S vertex}"
      context.projectionExpression = "typeOrigin, created, stash"

    else if sort == "time" && direction == "out"
      throw new Error "time sorting requires edge combinator" unless edge
      context.index = "OutEdgesByTime"
      context.keyExpression = "typeOrigin = #{qv to.S "#{edge}:#{vertex}"}"
      context.projectionExpression = "target, created, stash"

    else if sort == "time" && direction == "in"
      throw new Error "time sorting requires edge combinator" unless edge
      context.index = "InEdgesByTime"
      context.keyExpression = "typeTarget = #{qv to.S "#{edge}:#{vertex}"}"
      context.projectionExpression = "origin, created, stash"

    else
      console.error context
      throw new Error "unable to produce key and projection expressions."

    context

  applyRange = protect (context) ->
    {sort, direction, beginsWith, edge, range} = context

    if sort == "time"
      field = "created"
      needsPrefix = false
    else if sort == "value" && direction == "out"
      field = "typeTarget"
      needsPrefix = if edge then true else false
    else if sort == "value" && direction == "in"
      field = "typeOrigin"
      needsPrefix = if edge then true else false
    else
      console.error context
      throw new Error "unknown sort type"



    # Special casing for value sorting. Time sorting auto-includes edge in key
    if sort == "value"
      if edge && !beginsWith && !range?.before && !range?.after
        context.keyExpression +=
          " AND (begins_with (#{field}, #{qv to.S edge}))"

    if beginsWith
      beginsWith = "#{edge}:#{beginsWith}" if needsPrefix
      context.keyExpression +=
        " AND (begins_with (#{field}, #{qv to.S beginsWith}))"

    if range?.before
      before = if needsPrefix then "#{edge}:#{range.before}" else range.before
      context.keyExpression += " AND (#{field} < #{qv to.S before})"
    if range?.after
      after = if needsPrefix then "#{edge}:#{range.after}" else range.after
      context.keyExpression += " AND (#{field} > #{qv to.S after})"

    context

  compileOptions = protect (context) ->
    context.options =
      ProjectionExpression: context.projectionExpression
      ScanIndexForward: context.ascending
      Limit: context.limit
      ExclusiveStartKey: context.startKey

    context

  runQuery = protect (context) ->
    {index, keyExpression, options} = context

    if index
      {Items, LastEvaluatedKey} =
        await m.queryIndex index, keyExpression, null, options
    else
      {Items, LastEvaluatedKey} =
      await m.queryTable keyExpression, null, options

    context.results = parse Items
    context.next = LastEvaluatedKey if context.returnNext
    context


  query = (fx) ->
    do pipe [
      -> {}
      fx...
      buildExpressions
      applyRange
      compileOptions
      runQuery
    ]


  buildItem = ({origin, _origin, target, _target, edge, created, stash, dynamoExpires, body}) ->
    stash ?= {}
    stash.__key = origin: _origin, target: _target

    merge body,
      origin: origin
      target: target
      typeTarget: "#{edge}:#{target}"
      typeOrigin: "#{edge}:#{origin}"
      created: created ? new Date().toISOString()
      stash: stash
      dynamoExpires: dynamoExpires

  buildKey = ({origin, edge, target}) ->
    origin: origin
    typeTarget: "#{edge}:#{target}"


  get = (args) ->
    do pipe [
      -> {}
      args...
      (context) -> m.get buildKey context
    ]

  put = (args) ->
    do pipe [
      -> {}
      args...
      (context) -> m.put buildItem context
    ]

  del = (args) ->
    do pipe [
      -> {}
      args...
      (context) -> m.del buildKey context
    ]

  update = (args) ->
    do pipe [
      -> {}
      args...
      (context) -> m.update (buildKey context), context.body, context.drop
    ]

  increment = (args) ->
    do pipe [
      -> {}
      args...
      (context) -> m.increment (buildKey context), context.field
    ]

  decrement = (args) ->
    do pipe [
      -> {}
      args...
      (context) -> m.decrement (buildKey context), context.field
    ]






  beginsWith = curry (value, context) ->
    context.beginsWith = value
    context

  body = curry (value, context) ->
    context.body = value
    context

  created = curry (value, context) ->
    if isObject value
      context.created = value.created
    else
      context.created = value
    context

  direction = curry (name, context) ->
    context.direction = name
    context

  drop = curry (dropFields, context) ->
    context.drop = dropFields
    context

  edge = curry (name, context) ->
    context.edge = name
    context

  expires = curry (value, context) ->
    if isObject value
      context.dynamoExpires = value.dynamoExpires
    else
      context.dynamoExpires = value
    context

  field = curry (value, context) ->
    context.field = value
    context

  limit = curry (value, context) ->
    context.limit = value
    context

  origin = curry (value, labels, context) ->
    key = value
    key += "::#{label}" for label in labels
    context.origin = key
    context._origin = {value, labels}
    context

  range = curry ({before, after}, context) ->
    context.range = {before, after}
    context

  returnNext = curry (value, context) ->
    context.returnNext = value
    context

  sort = curry (type, context) ->
    switch type
      when "chronological"
        context.sort = "time"
        context.ascending = true
      when "reverse-chronological"
        context.sort = "time"
        context.ascending = false
      when "alphabetical"
        context.sort = "value"
        context.ascending = true
      when "reverse-alphabetical"
        context.sort = "value"
        context.ascending = false

    context

  startKey = curry (value, context) ->
    context.startKey = value if value
    context

  stash = curry (value, context) ->
    context.stash = merge context.stash, value
    context

  target = curry (value, labels, context) ->
    key = value
    key += "::#{label}" for label in labels
    context.target = key
    context._target = {value, labels}
    context

  vertex = curry (value, labels, context) ->
    key = value
    key += "::#{label}" for label in labels
    context.vertex = key
    context._vertex = {value, labels}
    context

  {
    query, get, put, del, update, increment, decrement,
    beginsWith, body, created, direction, drop, edge, expires, field,
    limit, origin, range, returnNext, sort, startKey, stash, target, vertex
  }


ReferenceModel = do ->
  map = (model, raw) ->
    data = clone raw
    reference = _type: model.label

    if data.created?
      reference.created = data.created


    key = [ data[model.primaryField] ]
    reference[model.primaryField] = data[model.primaryField]

    for x in model.secondaryFields
      if data[x]?
        key.push data[x]
        reference[x] = data[x]
      else
        console.error model, data
        throw new Error "field #{x} is not available in edge reference data. Unable to construct graph edge."


    if model.useLabel
      key.push model.label


    for x in model.projectedFields
      if data[x]?
        reference[x] = data[x]


    {key, reference}


  origin = curry (model, reference) ->
    e = EdgeModel model
    {key, reference:A} = map model, reference

    pipe [
      e.origin key[0], key[1..]
      e.stash {A}
    ]

  target = (model, reference) ->
    e = EdgeModel model
    {key, reference:B} = map model, reference

    pipe [
      e.target key[0], key[1..]
      e.stash {B}
    ]

  vertex = (model, reference) ->
    e = EdgeModel model
    {key} = map model, reference

    e.vertex key[0], key[1..]

  query = curry (e, fx) ->
    context = await e.query fx

    if context.direction == "in"
      key = "A"
    else
      key = "B"

    for result in context.results
      result.reference = result.stash[key]

    context


  {origin, target, vertex, query}





# Wraps the Sundog DynamoDB key-value store model to handle vertices.
VertexModel = (model) ->

  e = EdgeModel model

  flatten = protect (reference) ->
    unless reference?
      throw new Error "Vertex reference is undefined."

    result =
      if isObject reference
        reference[model.primaryField]
      else
        reference

    unless result? && isString result
      throw new Error "Vertex reference must be a string."

    result

  # Based on uniform hashing algorithm here:
  # https://www.d.umn.edu/~gshute/cs2511/slides/hash_tables/sections/uniform_hashing.xhtml
  shard = protect (reference) ->
    # "hash" returns a Uint8Array of length 64.
    {hash:ax} = hash Message.from "utf8", flatten reference

    r = 1n
    r = (r * 31n) + BigInt a for a in ax
    r %= BigInt model.shards
    model.label + r.toString()

  body = (_data) ->
    data = clone _data
    delete data[model.primaryField]
    _body data

  # This parse is specific to vertices to remove edge info for the application
  parse = protect (item) ->
    if item
      item[model.primaryField] = item.stash.__key.origin.value
      delete item.origin
      delete item.target
      delete item.typeTarget
      delete item.typeOrigin
      delete item.stash
    item

  get = (reference, labels = []) ->
    parse await e.get [
      e.origin (flatten reference), [labels..., model.label]
      e.edge "vertex"
      e.target (shard reference), [ "vertex-label" ]
    ]

  put = (data, labels = []) ->
    e.put [
      e.origin (flatten data), [labels..., model.label]
      e.edge "vertex"
      e.target (shard data), [ "vertex-label" ]
      e.created data
      e.expires data
      e.body data
    ]

  del = (reference, labels = []) ->
    e.del [
      e.origin (flatten reference), [labels..., model.label]
      e.edge "vertex"
      e.target (shard reference), [ "vertex-label" ]
    ]

  update = (reference, labels = [], data, dropFields) ->
    e.update [
      e.origin (flatten reference), [labels..., model.label]
      e.edge "vertex"
      e.target (shard reference), [ "vertex-label" ]
      e.body data
      e.drop dropFields
    ]

  increment = (reference, labels = [], field) ->
    e.increment [
      e.origin (flatten reference), [labels..., model.label]
      e.edge "vertex"
      e.target (shard reference), [ "vertex-label" ]
      e.field field
    ]

  decrement = (reference, labels = [], field) ->
    e.decrement [
      e.origin (flatten reference), [labels..., model.label]
      e.edge "vertex"
      e.target (shard reference), [ "vertex-label" ]
      e.field field
    ]

  {get, put, del, update, increment, decrement}


# Picks a shard between 0 and the shardCount by hashing on current timestamp,
# featuring millisecond resolution.
randomShard = (shardCount) ->
  # "hash" returns a Uint8Array of length 64.
  {hash:ax} = hash Message.from "utf8", new Date().toISOString()

  r = 1n
  r = (r * 31n) + BigInt a for a in ax
  r %= BigInt shardCount
  r.toString()

export {VertexModel, EdgeModel, ReferenceModel, randomShard}
