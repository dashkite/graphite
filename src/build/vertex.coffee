import { VertexModel } from "../graph-interface"
import { generateID } from "../helpers"

findByName = (ax, value) -> ax.find ({name}) -> name == value 

mapTypes = (object) ->
  output = {}
  for key, type of object
    output[key] = switch type
      when "string", "date-time" then "S"
      when "json" then "JSON"
      when "number" then "N"
  output
      

buildVertices = (spec) ->
  _build = (name) ->
    vertex = findByName spec.vertices, name
    vertex.shards ?= 10
    vertex.properties = {
      vertex.properties...
      created: "date-time"
      updated: "date-time"
    }

    { get, put, del } = VertexModel
      table: spec.table
      primaryField: vertex.primary
      label: vertex.name
      shards: vertex.shards
      types: mapTypes vertex.properties

    { get, put, delete: del }

  _abstract = (name, model) ->
    vertex = findByName spec.vertices, name

    create: (data) ->
      now = (new Date).toISOString()
      
      object = 
        created: now
        updated: now
      
      for field of vertex.properties
        if field == vertex.primary
          object[field] = await generateID()
        else
          object[field] = data[field]

      await model.put object
      object

    get: model.get

    put: (data) ->
      data.updated = (new Date).toISOString()
      await model.put data
      data

    delete: model.delete


  model = {}
  model[name] = _abstract name, _build(name) for { name } in spec.vertices 
  model

export default buildVertices