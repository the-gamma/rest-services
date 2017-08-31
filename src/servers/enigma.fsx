#if INTERACTIVE
#I "../../packages"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "System.Transactions"
#load "../common/serializer.fs"
#else
module GovUk.Airquality
#endif
open System
open System.IO
open System.Collections.Generic

open TheGamma
open TheGamma.Serializer

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

let app = 
  returnMembers [
    Member("test", None, Nested("/"), [], [])
  ]