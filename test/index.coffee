import { generateID } from "../src/helpers"

do ->
  console.log await generateID 20000, "base36"