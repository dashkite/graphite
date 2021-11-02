import {flow, curry, wrap} from "@pandastrike/garden"
import {clone, merge, first, rest} from "panda-parchment"

# This transforms dash edge query results so they are list of fields that the vertex model uses. We also flaten out non-linear information from the edge stash field, if needed.
add = curry (data, collection) ->
  merge edge, data for edge in collection

cleave = curry (field, prefix, collection) ->
  edge[field] = edge[field][prefix.length...] for edge in collection
  collection

pluck = curry (field, collection) ->
  edge[field] = clone edge.stash[field] for edge in collection
  collection

pluckAs = curry (oldField, newField, collection) ->
  edge[newField] = clone edge.stash[oldField] for edge in collection
  collection

fromKey = curry (end, field, collection) ->
  edge[field] = edge.stash.__key[end].value for edge in collection
  collection

prune = (collection) ->
  for edge in collection
    delete edge.stash
  collection

rename = curry (name, collection) ->
  for edge in collection
    if edge.origin
      edge[name] = edge.origin
      delete edge.origin
    else if edge.target
      edge[name] = edge.target
      delete edge.target
    else if edge.typeTarget
      edge[name] = edge.typeTarget
      delete edge.typeTarget
    else if edge.typeOrigin
      edge[name] = edge.typeOrigin
      delete edge.typeOrigin
    else
      console.error {collection}
      throw new Error "unable to rename edge. unknown type."

  collection

reshape = (args) ->
  promise = first args
  do flow [
    wrap await promise
    (rest args)...
    prune
  ]

export {add, cleave, pluck, pluckAs, fromKey, prune, rename, reshape}
