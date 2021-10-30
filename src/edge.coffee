import { EdgeModel } from "./graph-interface"
import { generateID } from "./helpers"

findByName = (ax, value) -> ax.find ({name}) -> name == value 

mapTypes = (object) ->
  output = {}
  for key, type of object
    output[key] = switch type
      when "string", "date-time" then "S"
      when "json" then "JSON"
      when "number" then "N"
  output

      

buildEdges = (spec) ->
  buildSearch = (name) ->
    vertex = findByName spec.vertices, name

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
            e.origin "authorIndex#{i}", [ "authorIndex" ]
            e.edge "indexes"
            e.target id, [ Author.label ]
          ]

    get: (q, {limit, type}) ->
      query [
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
    output = put: {}


    for sortName in edge.sort
      output.put[sortName] = (_origin, _target) ->
        put [
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

    output.get = (_origin, {before, after, limit, type}) ->
      query [
        e.vertex _origin[originVertex.primary], [ edge.from ]
        e.edge "#{edge.name}-#{type}"
        e.direction "out"
        e.range { before, after }
        e.limit limit ? 25
        e.sort "reverse-chronological"
      ]

    output.delete = (_origin, _target) ->
      Promise.all do -> 
        for sortName in edge.sort
          del [
            e.origin _origin[originVertex.primary], [ edge.from ]
            e.edge "#{edge.name}-#{sortName}"
            e.target _target[targetVertex.primary], [ edge.to ]
          ]
      
    output


  sort: buildSortEdges edge.name for edge in spec.edges when edge.sort? 
  search: buildSearchEdges vertex.name for vertex in spec.vertices when vertex.search?



export default buildEdges

_spec = """
table: byline-development
vertices:
  - name: author
    primary: id
    search:
      - nickname
    properties:
      id: string
      nickname: string
      display: string
      blurb: string
      posts: number
      title: string
      about: string
      figure: string
      caption: string
      integrations: json
  
  - name: profile
    primary: nickname
    properties:
      nickname: string
      publicKeys: json

  - name: post
    primary: id
    properties:
      id: string
      nickname: string
      content: string
      title: string
      preview: string
      seoKey: string
      keywords: string
      figure: string
      caption: string

  - name: draft
    primary: id
    properties:
      id: string
      nickname: string
      content: string
      title: string
      preview: string
      seoKey: string
      keywords: string
      figure: string
      caption: string


  - name: registry
    primary: id
    properties:
      id: string
      nickname: string

  - name: rss 
    primary: encoding
    properties:
      encoding: string
      label: string
      nickname: string
      content: string

  - name: atom
    primary: encoding
    properties:
      encoding: string
      label: string
      nickname: string
      content: string

edges:
  - name: posts
    from: author
    to: post
    sort: 
      - created
      - updated
    properties:
      - seoKey

  - name: drafts
    from: author
    to: draft
    sort: 
      - created
      - updated
    properties:
      - seoKey

"""

Models =

  author:
    ###
    * Search Stuff *
    We index all nickname references across many shards to provide massive
    potential throughput.
    We use uniform randomness to spread the search load.
    We use unmetered put and del to avoid spamming logs.
    ###

    deindex: meter "de-index author", ({id}) ->
      for i in [0..Author.shards]
        await e.del [
          e.origin "authorIndex#{i}", [ "authorIndex" ]
          e.edge "indexes"
          e.target id, [ Author.label ]
        ]

    index: meter "index author", ({nickname, id}) ->
      for i in [0..Author.shards]
        await e.put [
          e.origin "authorIndex#{i}", [ "authorIndex" ]
          e.edge "indexes"
          e.target id, [ Author.label ]
          e.stash {nickname}
        ]

    list: (q, {limit}) ->
      query [
        e.vertex "authorIndex#{randomShard Author.shards}", [ "authorIndex" ]
        e.edge "indexes"
        e.direction "out"
        e.beginsWith q
        e.limit limit ? 25
        e.sort "alphabetical"
      ]

  draft:
    indexCreated: ({nickname, id, created}) ->
      put [
        e.origin nickname, [ Author.label ]
        e.edge "drafts-created"
        e.target id, [ nickname, Draft.label ]
        e.created created
      ]

    indexUpdated: ({nickname, id, updated}) ->
      put [
        e.origin nickname, [ Author.label ]
        e.edge "drafts-updated"
        e.target id, [ nickname, Draft.label ]
        e.created updated
      ]

    deindex: ({nickname, id}) ->
      await del [
        e.origin nickname, [ Author.label ]
        e.edge "drafts-created"
        e.target id, [ nickname, Draft.label ]
      ]

      await del [
        e.origin nickname, [ Author.label ]
        e.edge "drafts-updated"
        e.target id, [ nickname, Draft.label ]
      ]

    list: (author, {before, after, limit, type}) ->
      if type == "created"
        edge = "drafts-created"
      else if type == "updated"
        edge = "drafts-updated"
      else
        edge = "drafts-updated"

      query [
        e.vertex author.nickname, [ Author.label ]
        e.edge edge
        e.direction "out"
        e.range {before, after}
        e.limit limit ? 25
        e.sort "reverse-chronological"
      ]

  post:
    indexCreated: ({nickname, id, created, seoKey}) ->
      put [
        e.origin nickname, [ Author.label ]
        e.edge "posts-created"
        e.target id, [ nickname, Post.label ]
        e.created created
        e.stash {seoKey}
      ]

    indexUpdated: ({nickname, id, updated, seoKey}) ->
      put [
        e.origin nickname, [ Author.label ]
        e.edge "posts-updated"
        e.target id, [ nickname, Post.label ]
        e.created updated
        e.stash {seoKey}
      ]

    deindex: ({nickname, id}) ->
      await del [
        e.origin nickname, [ Author.label ]
        e.edge "posts-created"
        e.target id, [ nickname, Post.label ]
      ]

      await del [
        e.origin nickname, [ Author.label ]
        e.edge "posts-updated"
        e.target id, [ nickname, Post.label ]
      ]

    list: (author, {before, after, limit, type}) ->
      if type == "created"
        edge = "posts-created"
      else if type == "updated"
        edge = "posts-updated"
      else
        edge = "posts-updated"

      query [
        e.vertex author.nickname, [ Author.label ]
        e.edge edge
        e.direction "out"
        e.range {before, after}
        e.limit limit ? 25
        e.sort "reverse-chronological"
      ]