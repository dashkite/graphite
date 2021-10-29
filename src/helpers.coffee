import { confidential } from "panda-confidential"

# TODO: We need KMS randomness access in Lambda contexts.
{ randomBytes, convert } = confidential()
generateID = (length, format) ->
  if !length then throw new Error "Must provide length parameter."
  if !format then throw new Error "Must provide output format parameter."

  if format == "base36"
    r = 0n
    for b, k in (await randomBytes length)
      r += (BigInt b) * (256n ** (BigInt k))
    r.toString 36
  else
    convert from: "bytes", to: format, (await randomBytes length)

export {
  generateID
}