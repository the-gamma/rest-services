#if INTERACTIVE
#I "../../packages"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "System.Transactions"
#I "../common"
#load "../../packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
#load "serializer.fs" "storage.fs" "pivot.fs" "database.fs"
#else
module TheGamma.Services.Eurostat
#endif
open System
open System.IO
open System.Collections.Generic

open TheGamma.Services.Storage
open TheGamma.Services.Serializer
open TheGamma.Services

open FSharp.Data
open Newtonsoft.Json
open System.Threading.Tasks

// ------------------------------------------------------------------------------------------------
// Config and data loading helpers
// ------------------------------------------------------------------------------------------------

#if INTERACTIVE
#load "../common/config.fs"
let connStrBlob = Config.TheGammaDataStorage
let connStrSql = Config.TheGammaSqlStorage
let enigmaApiKey = Config.EnigmaApiKey
#else
let connStrBlob = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMADATA_BLOBS")
let connStrSql = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMADATA_SQL")
let enigmaApiKey = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_ENIGMA_APIKEY")
#endif

// ------------------------------------------------------------------------------------------------
// Minimal demo showing how to create SQL tables and insert data
// ------------------------------------------------------------------------------------------------

// Say we have a CSV file with the following headers and data
let headers = 
  ["Country"; "Year"; "Value"]

let data = 
  [ [| "CZ"; "1999"; "10" |]
    [| "CZ"; "2000"; "10.5" |]
    [| "GB"; "2000"; "2.5" |] ]

// First, we infer types of the columns, generate SQL CREATE script and run it to create the table
let schema = Pivot.inferTypes headers data
printfn "INFERRED SCHEMA:\n  %A\n" schema
let sql = Database.scriptTable "my-table" schema 
printfn "GENERATED SQL COMMAND:\n%s\n" sql
try Database.executeCommand connStrSql sql
with e -> printfn "FAILED TO CREATE TABLE: %s" e.Message

// Table should now be created and it should be empty...
Database.executeScalarCommand connStrSql "SELECT Count(*) FROM [my-table]"
|> printfn "TABLE EXISTS: %A rows"

// BULK INSERT can insert multiple rows into the SQL database fairly efficiently 
// from a CSV file stored in a blob storage, so we first create a temp blob container
let container = createCloudBlobClient(connStrBlob).GetContainerReference("my-temp-blob")
container.CreateIfNotExists(Microsoft.WindowsAzure.Storage.Blob.BlobContainerPublicAccessType.Container)

// ...and we register it as an external storage with the Azure SQL Server
// (this will run once, but throw an exception if it already exists)
try Database.initializeExternalBlob connStrSql "https://thegammadata.blob.core.windows.net"
with e -> printfn "FAILED TO INITIALIZE BLOB: %s" e.Message

// Now, generate some random data and run a BULK INSERT into the table
// (this can fairly easily insert a few thousands rows at one time :-))
Database.insertRecords connStrBlob connStrSql "my-temp-blob" "my-table" 0 
  [ let rnd = System.Random()
    for country in ["CZ"; "GB"] do
    for year in 1500 .. 2000 do 
    yield [| country; string year; string (rnd.NextDouble()) |] ]

// Count how many rows we have now!
Database.executeScalarCommand connStrSql "SELECT Count(*) FROM [my-table]"
|> printfn "DATA INSERTED: %A rows"
  

// ------------------------------------------------------------------------------------------------
// Just a dummy REST service...
// ------------------------------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

let app = 
  choose [ 
    path "/" >=>
      returnMembers [
        Member("zz", None, Nested("/"), [], [])
      ]
  ]