import buildVertices from "./vertex"
import buildEdges from "./edges"



build = (spec) ->
  model = buildVertices spec
  # TODO: Edges

  model

export { build }