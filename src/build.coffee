build = (spec) ->

  console.log(spec)

export { build }

#   Model = do ->

#     initalize = (context) ->
#         now = (new Date).toISOString()

#         account: await generateID 16, "base36"
#         displayName: context.displayName
#         created: now
#         updated: now

#     create = flow [
#         initalize
#         tee dynamodb.account.put
#     ]

#     get = dynamodb.account.get

#     put = flow [
#         tee (account) -> account.updated = (new Date).toISOString()
#         tee dynamodb.account.put
#     ]

#     _delete = flow [
#         dynamodb.account.delete
#     ]



#     {
#         create
#         get
#         put
#         delete: _delete
#     }