import * as _ from "@dashkite/joy"
import { EdgeModel, randomShard } from "../graph-interface"
import * as h from "../graph-helpers"

findByName = (ax, value) -> ax.find ({name}) -> name == value 

buildEdges = (spec) ->
  e = EdgeModel table: spec.table

  buildSearchEdge = (name) ->
    vertex = findByName spec.vertices, name

    if !vertex.search?
      put: ->, delete: ->, get: ->
    else
      put: (_origin) ->
        for property in vertex.search
          for i in [0..vertex.shards]
            await e.put [
              e.origin "#{vertex.name}Search#{i}", [ "#{vertex.name}SearchIndex" ]
              e.edge "#{property}SearchIndex"
              e.target _origin[property], [ "#{property}SearchIndex" ]
              e.stash do ->
                [vertex.primary]: _origin[vertex.primary]
                [property]: _origin[property]
            ]
    
      delete: (_origin) ->
        for property in vertex.search
          for i in [0..vertex.shards]
            await e.del [
              e.origin "#{vertex.name}Search#{i}", [ "#{vertex.name}SearchIndex" ]
              e.edge "#{property}SearchIndex"
              e.target _origin[property], [ "#{property}SearchIndex" ]
            ]

      get: (q, {limit, next, type}) ->
        h.reshape [
           e.query [
            e.vertex "#{vertex.name}Search#{randomShard vertex.shards}", [ "#{vertex.name}SearchIndex" ]
            e.edge "#{type}SearchIndex"
            e.direction "out"
            e.beginsWith q
            e.limit limit ? 25
            e.startKey next
            e.returnNext true
            e.sort "alphabetical"
          ]
          _.get "results"
          _.tee (x) -> console.log x
          h.pluck vertex.primary
          h.pluck type
          _.map _.mask [ 
            vertex.primary
            type
          ]
        ]
       

  buildSortEdge = (name) ->
    edge = findByName spec.edges, name
    originVertex = findByName spec.vertices, edge.from
    targetVertex = findByName spec.vertices, edge.to

    if !edge.sort?
      put: ->, get: ->, delete: ->
    else 
      put: (_origin, _target) ->
        Promise.all do ->
          for sort in edge.sort
            e.put [
              e.origin _origin[originVertex.primary], [ edge.from ]
              e.edge "#{edge.name}-#{sort}"
              e.target _target[targetVertex.primary], [ edge.to ]
              e.created _target[ sort ]
              e.stash do ({output} = {})->
                output = {} 
                { properties } = targetVertex
                for property in edge.properties
                  output[property] = _target[property]
                output 
            ]

      getOut: (_origin, {before, after, limit, next, sort}) ->
        h.reshape [
          e.query [
            e.vertex _origin[originVertex.primary], [ edge.from ]
            e.edge "#{edge.name}-#{sort}"
            e.direction "out"
            e.range { before, after }
            e.limit limit ? 25
            e.startKey next
            e.returnNext true
            e.sort "reverse-chronological"
          ]
          _.get "results"
          _.tee (x) -> console.log x
          h.fromKey "origin", originVertex.primary
          h.fromKey "target", targetVertex.primary
          (h.pluck property for property in edge.properties)...
          _.map _.mask [ 
            originVertex.primary 
            targetVertex.primary
            edge.properties...
          ]
        ]

    getIn: (_target, {before, after, limit, next, sort}) ->
        h.reshape [
          e.query [
            e.vertex _target[targetVertex.primary], [ edge.to ]
            e.edge "#{edge.name}-#{sort}"
            e.direction "in"
            e.range { before, after }
            e.limit limit ? 25
            e.startKey next
            e.returnNext true
            e.sort "reverse-chronological"
          ]
          _.get "results"
          _.tee (x) -> console.log x
          h.fromKey "origin", originVertex.primary
          h.fromKey "target", targetVertex.primary
          (h.pluck property for property in edge.properties)...
          _.map _.mask [ 
            originVertex.primary 
            targetVertex.primary
            edge.properties...
          ]
        ]

    delete: (_origin, _target) ->
      Promise.all do ->
        for sortName in edge.sort
          e.del [
            e.origin _origin[originVertex.primary], [ edge.from ]
            e.edge "#{edge.name}-#{sortName}"
            e.target _target[targetVertex.primary], [ edge.to ]
          ]


  sort: do ->
    model = {}
    if spec.edges?
      for edge in spec.edges when edge.sort?
        model[ edge.to ] = buildSortEdge edge.name
    model

  search: do ->
    model = {}
    for vertex in spec.vertices when vertex.search?
      model[ vertex.name ] = buildSearchEdge vertex.name
    model



export default buildEdges
