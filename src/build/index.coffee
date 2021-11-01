import buildVertices from "./vertex"
import buildEdges from "./edge"
import buildModel from "./abstract"



build = (spec) ->
  vertices = buildVertices spec
  edges = buildEdges spec
  buildModel spec, vertices, edges

export { build }