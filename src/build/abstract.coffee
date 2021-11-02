import { project } from "@dashkite/joy"

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
                        edges.sort[ edgeSpec.to ].get origin, {parameters..., sort:sortName }
          output

        
          

        put: (args...) ->
          if hasSort
            [ origin, target ] = args
          else 
            [ target ] = args
          
          await vertex.put target
          await search.put target if hasSearch
          await sort.put origin, target if hasSort

        delete: (target) ->
          
          # if hasSort
          #   origins = spec.edges.filter (edge) -> edge.to == name
          #   for reference in origins
          #     origin = await verticies[reference].get reference
          #     await sort.delete origin, target 
          
          await search.delete target if hasSearch
          await vertex.delete target

  console.log("model", model)
  model

export default buildModel