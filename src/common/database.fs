module TheGamma.Services.Database
(*
open System.IO
open System.Text
*)
open System
open System.Data
open System.Data.SqlClient
open TheGamma.Services.Storage
(*
open Microsoft.FSharp.Reflection

// ------------------------------------------------------------------------------------------------
// Helpers for storing data to database - given F# record, generate SQL CREATE table scripts
// ------------------------------------------------------------------------------------------------

let tableName name = 
  let chars = 
    [| for c in name do 
        if Char.IsUpper c then yield '-'
        yield Char.ToLower(c) |]
  System.String(chars).Trim('-')
  
let rec getTables prefix typ = seq {
  for fld in FSharpType.GetRecordFields(typ) do
    if FSharpType.IsRecord(fld.PropertyType) then yield! getTables prefix fld.PropertyType
  yield prefix + "-" + tableName typ.Name }
  
let rec scriptTable (sb:StringBuilder) prefix (known:System.Collections.Generic.HashSet<_>) (typ:System.Type) =   
  if not (known.Contains(typ.Name)) then
    known.Add(typ.Name) |> ignore
    let fields =
      [ for fld in FSharpType.GetRecordFields(typ) ->
          let name, typ = 
            if fld.PropertyType = typeof<string> then fld.Name, "nvarchar(1000)"
            elif fld.PropertyType = typeof<int> then fld.Name, "int"
            elif fld.PropertyType = typeof<float> then fld.Name, "float"
            elif fld.PropertyType = typeof<DateTimeOffset> then fld.Name, "datetimeoffset"
            elif fld.PropertyType = typeof<DateTime> then fld.Name, "datetime"
            elif fld.PropertyType = typeof<Guid> then fld.Name, "uniqueidentifier"
            elif FSharpType.IsRecord fld.PropertyType then 
              let idProp = FSharpType.GetRecordFields(fld.PropertyType) |> Seq.find (fun p -> p.Name = "ID")
              let idTyp = 
                if idProp.PropertyType = typeof<int> then "int"
                elif idProp.PropertyType = typeof<Guid> then "uniqueidentifier"
                elif idProp.PropertyType = typeof<string> then "nvarchar(1000)"
                else failwith "Unsupported ID type"
              fld.Name + "ID", idTyp
            else failwithf "Unsupported type: %s" fld.PropertyType.Name
          if FSharpType.IsRecord fld.PropertyType then
            scriptTable sb prefix known fld.PropertyType
          if name = "ID" then sprintf "[ID] %s PRIMARY KEY NOT NULL" typ
          else sprintf "[%s] %s NOT NULL" name typ ]
    let fields = String.concat ",\n  " fields
    sprintf "CREATE TABLE dbo.[%s-%s] (\n  %s\n)\n\n" 
      prefix (tableName typ.Name) fields |> sb.Append |> ignore

let rec scriptTables prefix types = 
  let sb = StringBuilder()  
  let known = System.Collections.Generic.HashSet<_>()
  for typ in types do scriptTable sb prefix known typ
  sb.ToString()
*)
let executeCommand sqlConn sql = 
  use conn = new SqlConnection(sqlConn)
  conn.Open()
  use cmd = new SqlCommand(sql, conn)
  cmd.ExecuteNonQuery() |> ignore

let executeCommandWithTimeout sqlConn timeout sql = 
  use conn = new SqlConnection(sqlConn)
  conn.Open()
  use cmd = new SqlCommand(sql, conn, CommandTimeout=timeout)
  cmd.ExecuteNonQuery() |> ignore

let executeReader sqlConn sql parse = 
  use conn = new SqlConnection(sqlConn)
  conn.Open()
  use cmd = new SqlCommand(sql, conn)
  use rdr = cmd.ExecuteReader() 
  let res = ResizeArray<_>()
  while rdr.Read() do res.Add(parse rdr)
  res
(*

let executeScalarCommand sqlConn sql = 
  use conn = new SqlConnection(sqlConn)
  conn.Open()
  use cmd = new SqlCommand(sql, conn)
  cmd.ExecuteScalar()

let initializeExternalBlob storageAccount = 
  executeCommand 
    ( "CREATE EXTERNAL DATA SOURCE TheGammaStorage "  +
      sprintf "WITH (TYPE = BLOB_STORAGE, LOCATION = '%s')" storageAccount)

let initializeStorage prefix types = 
  let sql = scriptTables prefix types
  executeCommand sql

let cleanupStorage connStr prefix types =
  try executeCommand connStr "DROP EXTERNAL DATA SOURCE TheGammaStorage" 
  with e -> printfn "Could not delete TheGammaStorage: %s"  e.Message
  let tables = types |> Seq.collect (getTables prefix) |> Seq.distinct
  for tab in tables do
    try executeCommand connStr (sprintf "DROP TABLE [%s]" tab)
    with e -> printfn "Could not delete table %s: %s" tab e.Message
      
      *)

let initializeExternalBlob connStrSql storageAccount = 
  executeCommand connStrSql 
    ( "CREATE EXTERNAL DATA SOURCE TheGammaStorage "  +
      sprintf "WITH (TYPE = BLOB_STORAGE, LOCATION = '%s')" storageAccount)

let cleanupExternalBlob connStrSql =
  try executeCommand connStrSql "DROP EXTERNAL DATA SOURCE TheGammaStorage" 
  with e -> failwithf "Could not delete TheGammaStorage: %s" e.Message
      
let insertRecords connStrBlob connStrSql container tableName errors (records:seq<string[]>) =
  let tempName = "blob" + Guid.NewGuid().ToString("N")
  let blob = writeRecordsToBlob connStrBlob container "temp" (tempName + ".txt") records
  let insert = 
    ( sprintf "BULK INSERT [%s] FROM '%s/temp/%s.txt' " tableName container tempName) +
    ( sprintf "WITH (DATA_SOURCE = 'TheGammaStorage', FIELDTERMINATOR = ';', ROWTERMINATOR = '0x0a', TABLOCK, MAXERRORS=%d)" errors)
  executeCommandWithTimeout connStrSql (60 * 5) insert
  blob.Delete()
