import { project } from "@dashkite/joy"

buildModel = (spec, vertices, edges) ->
  model = {}

  # create: [ vertices[ name ].create ]
  # put: [ vertices[ name ].put ]
  # get: [ vertices[ name ].get ]
  # delete: [ vertices[ name ].delete ]

  if spec.edges?
    verticesWithSortEdges = project "to", spec.edges
  else 
    verticesWithSortEdges = []

  for { name, search: searchEdges } in spec.vertices
    do (name, searchEdges, {vertex, search, sort, hasSearch, hasSort} = {}) ->
      vertex = vertices[ name ]
      search = edges.search[ name ]
      sort = edges.sort[ name ]
      hasSearch = searchEdges?
      hasSort = (name in verticesWithSortEdges)

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

        list: (q, parameters = {}) ->

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