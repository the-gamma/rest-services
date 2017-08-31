// --------------------------------------------------------------------------------------
// Build script that hosts & auto-reloads servers from the various script files
// --------------------------------------------------------------------------------------

#I "../packages"
#r "FAKE/tools/FakeLib.dll"
#r "FSharp.Compiler.Service/lib/net45/FSharp.Compiler.Service.dll"
#r "Suave/lib/net40/Suave.dll"

open Fake
open System
open System.IO
open Suave
open Suave.Operators
open Suave.Web
open Microsoft.FSharp.Compiler.Interactive.Shell

// --------------------------------------------------------------------------------------
// The following uses FileSystemWatcher to look for changes in 'app.fsx'. When
// the file changes, we run `#load "app.fsx"` using the F# Interactive service
// and then get the `App.app` value (top-level value defined using `let app = ...`).
// The loaded WebPart is then hosted at localhost:10042.
// --------------------------------------------------------------------------------------

let sbOut = new Text.StringBuilder()
let sbErr = new Text.StringBuilder()

let fsiSession =
  let inStream = new StringReader("")
  let outStream = new StringWriter(sbOut)
  let errStream = new StringWriter(sbErr)
  let fsiConfig = FsiEvaluationSession.GetDefaultConfiguration()
  let argv = Array.append [|"/fake/fsi.exe"; "--quiet"; "--noninteractive" |] [||]
  FsiEvaluationSession.Create(fsiConfig, argv, inStream, outStream, errStream)

let reportFsiError (e:exn) =
  traceError "Reloading app.fsx script failed."
  traceError (sprintf "Message: %s\nError: %s" e.Message (sbErr.ToString().Trim()))
  sbErr.Clear() |> ignore

let reloadScript serverFsx =
  try
    let file = Path.GetFileNameWithoutExtension(serverFsx)
    let modul = file.[0].ToString().ToUpper() + file.Substring(1)
    traceImportant <| sprintf "Reloading '%s.fsx' script..." file
    fsiSession.EvalInteraction(sprintf "#load @\"%s\"" serverFsx)
    match fsiSession.EvalExpression(modul + ".app") with
    | Some app -> Some(app.ReflectionValue :?> WebPart)
    | None -> failwithf "Couldn't get 'app' value for sample '%s.fsx'" file
  with e -> reportFsiError e; None

// --------------------------------------------------------------------------------------
// Suave server that redirects all request to currently loaded version
// --------------------------------------------------------------------------------------

let getLocalServerConfig port =
  { defaultConfig with
      homeFolder = Some __SOURCE_DIRECTORY__
      logger = Logging.Loggers.saneDefaultsFor Logging.LogLevel.Debug
      bindings = [ HttpBinding.mkSimple HTTP  "127.0.0.1" port ] }

let servers = System.Collections.Generic.Dictionary<string, WebPart>()

let reloadAppServer (changedFiles: string seq) =
  for file in changedFiles do
    let name = Path.GetFileNameWithoutExtension(file)
    reloadScript file |> Option.iter (fun app -> 
      servers.[name] <- app
      traceImportant "Refreshed server." )

/// Drop the <s> part from http://localhost:123/<s>/something
let dropPrefix part ctx = 
  let u = ctx.request.url
  let local = 
    match List.ofArray (u.LocalPath.Substring(1).Split('/')) with
    | _::rest -> String.concat "/" rest
    | [] -> ""
  let url = System.Uri(u.Scheme + "://" + u.Authority + "/" + local)
  { ctx with request = { ctx.request with url = url }} |> part

// Server that serves pages from the given array
let handlePage s =
  match servers.TryGetValue(s) with
  | true, part -> dropPrefix part
  | _ -> RequestErrors.NOT_FOUND "Page not found"

let app =
  Writers.setHeader  "Access-Control-Allow-Origin" "*"
  >=> Writers.setHeader "Access-Control-Allow-Headers" "content-type,x-cookie"
  >=> choose [ 
    Filters.OPTIONS >=> Successful.OK "CORS approved"              
    Filters.path "/" >=> Successful.OK "Services are running..."
    Filters.pathScan "/%s/%s" (fst >> handlePage)  
    Filters.pathScan "/%s" handlePage ]

let port = 10033
let _, server = startWebServerAsync (getLocalServerConfig port) app

// Start Suave to host it on localhost
let sources = { BaseDirectory = __SOURCE_DIRECTORY__; Includes = [ "servers/*.fsx" ]; Excludes = [] }
reloadAppServer sources
Async.Start(server)

// Watch for changes & reload when server.fsx changes
let watcher = sources |> WatchChanges (Seq.map (fun x -> x.FullPath) >> reloadAppServer)
traceImportant "Waiting for app.fsx edits. Press any key to stop."
System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite)
