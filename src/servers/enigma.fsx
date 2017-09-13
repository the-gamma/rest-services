#if INTERACTIVE
#I "../../packages"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net40/FSharp.Data.dll"
#r "System.Transactions"
#I "../common"
#load "../../packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
#load "serializer.fs" "storage.fs" "database.fs" "pivot.fs"
#else
module GovUk.Airquality
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
let root = "http://localhost:10033"
#else
let root = "https://thegamma-rest-services.azurewebsites.net"
let connStrBlob = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMADATA_BLOBS")
let connStrSql = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMADATA_SQL")
let enigmaApiKey = Environment.GetEnvironmentVariable("CUSTOMCONNSTR_ENIGMA_APIKEY")
#endif

type EnigmaMeta = JsonProvider<const(__SOURCE_DIRECTORY__ + "/enigma/meta.json")>  
type EnigmaData = JsonProvider<const(__SOURCE_DIRECTORY__ + "/enigma/data.json")>  
type TableSchema = JsonProvider<"""[{"name":"col 1", "type":"int"}, {"name":"col 2", "type":"string", "format":"xxx"}]""">

// ------------------------------------------------------------------------------------------------
// Loading the tree structure of data sets for autocomplete
// ------------------------------------------------------------------------------------------------

type Dataset = 
  { Path : string
    Label : string 
    Description : string
    IsDataset : bool
    Children : Lazy<Task<list<Dataset>>> }

let cachedRequest key endpoint url = async {
  let! blob = tryReadBlobAsync connStrBlob "enigma" (endpoint + "/" + url)
  match blob with
  | Some res -> 
      return res
  | None ->
      let! data = Http.AsyncRequestString(sprintf "https://api.enigma.io/v2/%s/%s/%s" endpoint key url)
      do! writeBlobAsync connStrBlob "enigma" (endpoint + "/" + url) data
      return data }

let rec getChildren path = async {
  let! json = cachedRequest enigmaApiKey "meta" path
  let data = EnigmaMeta.Parse json
  let children = 
    [ for i in data.Result.ImmediateNodes -> 
        { Path = i.Datapath.Substring(path.Length + 1)
          IsDataset = false
          Label = i.Label; Description = i.Description
          Children = lazy Async.StartAsTask (getChildren i.Datapath) } ]
  let data = 
    [ for c in data.Result.ChildrenTables do
        let path = c.Datapath.Substring(path.Length + 1)
        if not (path.Contains(".")) then 
          yield { Path = path; Label = c.Label; IsDataset = true;
                  Description = c.Description; Children = lazy Task.FromResult([]) } ]
  return children @ data |> List.sortBy (fun c -> c.Label) }

// Global Enigma data sets
let datasets = 
  [ { Path = "us"
      IsDataset = false
      Label = "United States"
      Description = "Data relating to the United States."
      Children = lazy Async.StartAsTask (getChildren "us") } ]

// Pre-load first level of children
for d in datasets do ignore(d.Children.Force())

/// Find data set with the specified path 
let rec findPath path (datasets:seq<Dataset>) = async {
  match path with 
  | [] -> return Some(datasets)
  | x::xs -> 
      match datasets |> Seq.tryFind (fun d -> d.Path = x) with
      | None -> return None
      | Some d -> 
          let! children = d.Children.Value |> Async.AwaitTask
          return! findPath xs children }


// ------------------------------------------------------------------------------------------------
// Downloading and caching data from Enigma
// ------------------------------------------------------------------------------------------------

open System.Text

let scriptTable table tableType =
  let fields =
    [ for header, typ in tableType ->
        match typ with 
        | Pivot.InferredType.Bool | Pivot.InferredType.OneZero -> header, "bit"
        | Pivot.InferredType.Date _ -> header, "datetimeoffset"
        | Pivot.InferredType.Float -> header, "real" 
        | Pivot.InferredType.Int -> header, "int" 
        | Pivot.InferredType.String | Pivot.InferredType.Any -> header, "ntext" ]
    |> Seq.map (fun (h, t) -> sprintf "[%s] %s NULL" h t)
    |> String.concat ",\n  " 
  sprintf "CREATE TABLE dbo.[%s] (\n  %s\n)\n\n"  table fields

let formatJson = function
  | JsonValue.Boolean true -> "true"
  | JsonValue.Boolean false -> "false"
  | JsonValue.String s -> s
  | JsonValue.Null -> ""
  | JsonValue.Float f -> string f 
  | JsonValue.Number n -> string n
  | (JsonValue.Record _ | JsonValue.Array _) as json -> json.ToString()

let downloadPage page tableName = async {
  let! page = cachedRequest enigmaApiKey "data" (sprintf "%s/page/%d" tableName page)
  return EnigmaData.Parse(page) }

let readRows headers (rows:seq<EnigmaData.Result>) = 
  rows |> Seq.map (fun res ->
    let lookup = 
      match res.JsonValue with 
      | JsonValue.Record flds -> dict flds 
      | _ -> failwith "Expected record cannot infer type"
    headers |> Array.map (fun h -> formatJson (lookup.[h])) )

let inferTableTypes (rows:seq<EnigmaData.Result>) =
  if Seq.isEmpty rows then failwith "No records returned, cannot infer type"
  let headers = 
    match Seq.head(rows).JsonValue with 
    | JsonValue.Record flds -> Array.map fst flds
    | _ -> failwith "Expected JSON record, cannot infer type"
  Pivot.inferTypes headers (readRows headers rows)
  
let cacheDataset full datasetName = async {
  // Table name, depending on whether we want just preview or full data
  let tableName = "enigma-" + datasetName + (if full then "-full" else "-preview")

  // Read first page, create SQL table, bulk insert first page
  printfn "Caching dataset '%s'" datasetName
  let! firstPage = downloadPage 1 datasetName 
  let types = inferTableTypes firstPage.Result
  let headers = Array.map fst types
  let sql = scriptTable tableName types
  Database.executeCommand connStrSql sql
  Database.insertRecords connStrBlob connStrSql "enigma" tableName 0 (readRows headers firstPage.Result)
  printfn "Caching '%s' page %d/%d: stored first page" datasetName 1 firstPage.Info.TotalPages

  // Download and save individual pages of the dataset
  let downloadPage page = async {
    printfn "Caching '%s' page %d/%d: starting" datasetName page firstPage.Info.TotalPages
    let! data = downloadPage page datasetName 
    Database.insertRecords connStrBlob connStrSql "enigma" tableName 0 (readRows headers data.Result)
    printfn "Caching '%s' page %d/%d: done" datasetName page firstPage.Info.TotalPages }    

  // Only download remaining pages if 'full' is requested
  if full then 
    if firstPage.Info.TotalPages > 20 then failwithf "The dataset '%s' is too big." datasetName
    do! [ for page in 2 .. firstPage.Info.TotalPages -> downloadPage page ]
        |> Async.Parallel |> Async.Ignore 
  return types }

/// Make sure we do not start download twice 
type LockMessage = 
  | GetOrAdd of string * AsyncReplyChannel<Choice<Task<unit>, TaskCompletionSource<unit>>> 
  | Remove of string 

let lockAgent = MailboxProcessor.Start(fun inbox -> async {
  let dict = System.Collections.Generic.Dictionary<string, Task<unit>>()
  while true do 
    let! msg = inbox.Receive()
    match msg with 
    | Remove(k) -> dict.Remove(k) |> ignore
    | GetOrAdd(k, repl) when dict.ContainsKey(k) -> 
        repl.Reply(Choice1Of2(dict.[k]))
    | GetOrAdd(k, repl) -> 
        let tcs = TaskCompletionSource<unit>()
        dict.Add(k, tcs.Task)
        repl.Reply(Choice2Of2(tcs)) })

/// Make sure that the given data set is cached in DB and we have schema in a blob
let rec ensureCacheDataset full datasetName = async {
  let blobName = "schema/" + datasetName + (if full then ".full" else "")
  let! blob = tryReadBlobAsync connStrBlob "enigma" ("schema/" + datasetName)
  match blob with 
  | Some schema -> 
      printfn "Reading & parsing schema for %s" datasetName
      let columns = TableSchema.Parse(schema) |> Array.map (fun col ->
        match col.Type with 
        | "string" -> col.Name, Pivot.InferredType.String
        | "bool" -> col.Name, Pivot.InferredType.Bool
        | "date" -> col.Name, Pivot.InferredType.Date(System.Globalization.CultureInfo.GetCultureInfo(defaultArg col.Format ""))
        | "int" -> col.Name, Pivot.InferredType.Int
        | "float" -> col.Name, Pivot.InferredType.Float
        | _ -> col.Name, Pivot.InferredType.String)
      return columns 

  | None ->
      let! lock = lockAgent.PostAndAsyncReply(fun ch -> GetOrAdd(blobName, ch))
      match lock with 
      | Choice1Of2 task -> 
          printfn "Waiting for download completion of %s" datasetName
          do! Async.AwaitTask(task)
          return! ensureCacheDataset full datasetName

      | Choice2Of2 tcs -> 
          try
            printfn "Caching data set for %s" datasetName
            let! columns = cacheDataset full datasetName
            let schema = columns |> Array.map (fun (col, typ) ->
              let typ, fmt = 
                match typ with 
                | Pivot.InferredType.Any | Pivot.InferredType.String -> "string", None
                | Pivot.InferredType.Bool | Pivot.InferredType.OneZero -> "bool", None
                | Pivot.InferredType.Date null -> "date", Some("")
                | Pivot.InferredType.Date clt -> "date", Some(clt.IetfLanguageTag)
                | Pivot.InferredType.Int -> "int", None
                | Pivot.InferredType.Float -> "float", None
              TableSchema.Root(col, typ, fmt))  
            let json = schema |> Array.map (fun j -> j.JsonValue) |> JsonValue.Array
            do! writeBlobAsync connStrBlob "enigma" blobName (json.ToString())  
            return columns
          finally 
            printfn "Caching data set for %s (done)" datasetName
            tcs.TrySetResult(()) |> ignore
            lockAgent.Post(Remove blobName)  }


// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------
(*
let sql = scriptTable "enigma-us.gov.whitehouse.salaries.2016"
Database.initializeExternalBlob connStrSql "https://thegammadata.blob.core.windows.net"

Database.cleanupExternalBlob connStrSql

let tableName = "us.gov.ahrq.grants"
Database.executeCommand connStrSql (sprintf "DROP TABLE [enigma-%s-preview]" tableName)

Database.insertRecords connStrBlob connStrSql "enigma" "enigma-us.gov.whitehouse.salaries.2016" 0 rows
*)

//Database.executeCommand connStrSql "DROP TABLE [enigma-enigma-us.gov.whitehouse.salaries.2016]"



// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

let app = 
  choose [ 
    pathScan "/pivot/%s" (fun id ctx -> async {
      if id |> Seq.forall (fun c -> c = '.' || c = '-' || Char.IsLetterOrDigit(c)) |> not then 
        failwith "Invalid dataset name"
      let! meta = ensureCacheDataset false id
      let table = sprintf "enigma-%s-preview" id
      return! Pivot.handleSqlRequest connStrSql table meta (List.map fst ctx.request.query) ctx })
      (*
      let data = 
        Database.executeReader connStrSql (sprintf "SELECT * FROM [enigma-%s-preview]" id)
          (fun row -> meta |> Array.mapi (fun i (col, typ) ->
            let isNull = row.IsDBNull(i)
            match typ with
            | Pivot.InferredType.Any | Pivot.InferredType.String when isNull -> col, Pivot.Value.String("")
            | _ when isNull -> failwith "Unexpected null value"
            | Pivot.InferredType.Any | Pivot.InferredType.String -> col, Pivot.Value.String(row.GetString(i))
            | Pivot.InferredType.Bool | Pivot.InferredType.OneZero -> col, Pivot.Value.Bool(row.GetBoolean(i))
            | Pivot.InferredType.Date _ -> col, Pivot.Value.Date(row.GetDateTimeOffset(i))
            | Pivot.InferredType.Int -> col, Pivot.Value.Number(float(row.GetInt32(i)))
            | Pivot.InferredType.Float -> col, Pivot.Value.Number(float(row.GetFloat(i))) )) 
      return! Pivot.handleInMemoryRequest meta (data.ToArray()) (List.map fst ctx.request.query) ctx })
      //*)
    pathScan "/%s" (fun id ctx -> async {
      let! ds = findPath (List.ofSeq(id.Split([|'.'|], StringSplitOptions.RemoveEmptyEntries))) datasets
      match ds with 
      | None -> return! ctx |> RequestErrors.BAD_REQUEST "Invalid datapath"
      | Some sets -> 
        return! ctx |> returnMembers [          
          for ds in sets do
            ignore(ds.Children.Force())
            let url = "/" + (if id = "" then ds.Path else id + "." + ds.Path)
            let typ = 
              if ds.IsDataset then Provider("pivot", root + "/enigma/pivot" + url)
              else Nested(url)
            yield Member(ds.Label, None, typ, [], [])
      ] })    
  ]