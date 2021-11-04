import { generateID } from "../src/helpers"

do ->
  await generateID 16, "base36"