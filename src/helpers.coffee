import { confidential } from "panda-confidential"

# TODO: We need KMS randomness access in Lambda contexts.
{ randomBytes } = confidential()

generateID = () ->
  bytes = await randomBytes 16
  result = 0n
  power = bytes.length - 1
  
  for byte in bytes
    result += (BigInt byte) * (256n ** (BigInt power))
    power--

  result.toString 36

export {
  generateID
}