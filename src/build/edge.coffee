import { EdgeModel } from "../graph-interface"

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
              e.stash [property]: _origin[property]
            ]
    
      delete: (_origin) ->
        for property in vertex.search
          for i in [0..vertex.shards]
            await e.del [
              e.origin "#{vertex.name}Search#{i}", [ "#{vertex.name}SearchIndex" ]
              e.edge "#{property}SearchIndex"
              e.target _origin[property], [ "#{property}SearchIndex" ]
            ]

      get: (q, {limit, type}) ->
        e.query [
          e.vertex "#{vertex.name}Search#{randomShard vertex.shards}", [ "#{vertex.name}SearchIndex" ]
          e.edge "#{type}SearchIndex"
          e.direction "out"
          e.beginsWith q
          e.limit limit ? 25
          e.sort "alphabetical"
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
          for sortName in edge.sort
            e.put [
              e.origin _origin[originVertex.primary], [ edge.from ]
              e.edge "#{edge.name}-#{sortName}"
              e.target _target[targetVertex.primary], [ edge.to ]
              e.created sortName
              e.stash do ({output} = {})->
                output = {} 
                { properties } = targetVertex
                for property in edge.properties
                  output[property] = _target[property]
                output 
            ]

      get: (_origin, {before, after, limit, type}) ->
        e.query [
          e.vertex _origin[originVertex.primary], [ edge.from ]
          e.edge "#{edge.name}-#{type}"
          e.direction "out"
          e.range { before, after }
          e.limit limit ? 25
          e.sort "reverse-chronological"
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
