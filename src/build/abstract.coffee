import { project, find } from "@dashkite/joy"

findByName = (ax, value) -> ax.find ({name}) -> name == value 

buildModel = (spec, vertices, edges) ->
  model = {}

  # create: [ vertices[ name ].create ]
  # put: [ vertices[ name ].put ]
  # get: [ vertices[ name ].get ]
  # delete: [ vertices[ name ].delete ]

  if spec.edges?
    verticesWithInEdges = project "to", spec.edges
    verticesWithOutEdges = project "from", spec.edges
  else 
    verticesWithInEdges = []
    verticesWithOutEdges = []

  for { name, search: searchEdges } in spec.vertices
    do (name, searchEdges, {vertex, search, sort, hasSearch, hasSort} = {}) ->
      vertex = vertices[ name ]
      search = edges.search[ name ]
      sort = edges.sort[ name ]
      hasSearch = searchEdges?
      hasSort = (name in verticesWithInEdges)

      model[ name ] = 
        create: (args...) ->
          if hasSort
            [ origin, data ] = args
          else 
            [ data ] = args

          target = await vertex.create data
          await search.put target if hasSearch 
          await sort.put origin, target if hasSort 
          target

        get: vertices[ name ].get

        search: do ->
          output = {}   
          if hasSearch
            for type in searchEdges
              do (type) ->
                output[ type ] = (q, parameters = {}) -> 
                  search.get q, { parameters..., type }
          output

        list: do ->
          output = {}
          if (name in verticesWithOutEdges)
            for edgeSpec in spec.edges when edgeSpec.from == name  
              do (edgeSpec) ->
                output[ edgeSpec.name ] = {}
                for sortName in edgeSpec.sort
                  do (sortName) ->
                    output[ edgeSpec.name ][ sortName ] = 
                      (origin, parameters = {}) ->
                        edges.sort[ edgeSpec.to ].getOut origin, {parameters..., sort:sortName }
          output

        
          

        put: (args...) ->
          if hasSort
            [ origin, target ] = args
          else 
            [ target ] = args
          
          await vertex.put target
          await search.put target if hasSearch
          await sort.put origin, target if hasSort

        delete: (_vertex) ->
   
          # When vertex is target (incoming edges)
          # Find all incoming edges (find origins)
          # Delete them
          for edgeSpec in spec.edges when edgeSpec.to == name
            _origins = edges.sort[ name ].getIn _vertex
            for _origin in _origins
              # TODO: Investigate the parallelism limits around requests to 
              #       DynamoDB. We can't Promise.all here, but can we partition?
              await edges.sort[ name ].delete _origin, _vertex


          # When vertex is origin (outgoing edges)
          # Find all outgoing edges (find targets)
          # Delete them
          # Apply deleteSubgraph to the targets if target.center != true
          for edgeSpec in spec.edges when edgeSpec.from == name
            _targets = edges.sort[ edgeSpec.to ].getOut _vertex
            for _target in _targets
              await edges.sort[ edgeSpec.to ].delete _vertex, _target
            
            # TODO: Can we currently get the type metadata on the vertex now?
            #       If not, we should add that to the lower layers of Graphite.
            targetVertex = findByName spec.vertices, edgeSpec.to
            if targetVertex.center != true
              # When target is a non-central vertex, recurse on delete.
              for _target in _targets
                await model[ edgeSpec.to ].delete _target 
          

          # Delete search edges and the vertex itself.
          await search.delete _vertex if hasSearch
          await vertex.delete _vertex
            

          
          

  console.log("model", model)
  model

export default buildModel