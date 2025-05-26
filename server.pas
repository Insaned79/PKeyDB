{$MODE OBJFPC}
unit server;

interface

uses
  SysUtils, fphttpapp, fphttpserver, httpdefs, classes, strutils, fpjson, jsonparser, DateUtils, config, auth, storage;

procedure StartServer(const Conf: TAppConfig);

implementation

type
  TDBStorage = class
    Storage: TStorage;
    destructor Destroy; override;
  end;

  TSimpleServer = class(TFPHTTPServer)
  private
    FDBs: TStringList; // name -> TDBStorage
    FConfig: TAppConfig;
    function GetDB(const DBName: string): TDBStorage;
    function FindDBConfig(const DBName: string): TDatabaseConfig;
  public
    constructor Create(AOwner: TComponent; const Conf: TAppConfig); reintroduce;
    destructor Destroy; override;
    procedure HandleRequest(Sender: TObject; var ARequest: TFPHTTPConnectionRequest; var AResponse: TFPHTTPConnectionResponse);
  end;

destructor TDBStorage.Destroy;
begin
  StorageClose(Storage);
  inherited Destroy;
end;

constructor TSimpleServer.Create(AOwner: TComponent; const Conf: TAppConfig);
begin
  inherited Create(AOwner);
  FDBs := TStringList.Create;
  FDBs.OwnsObjects := True;
  FConfig := Conf;
end;

destructor TSimpleServer.Destroy;
begin
  FDBs.Free;
  inherited Destroy;
end;

function TSimpleServer.GetDB(const DBName: string): TDBStorage;
var idx: Integer;
    dbFile, datFile: string;
    dbStorage: TDBStorage;
    dbConfig: TDatabaseConfig;
begin
  idx := FDBs.IndexOf(DBName);
  if idx < 0 then
  begin
    dbConfig := FindDBConfig(DBName);
    dbFile := dbConfig.Path;
    datFile := ChangeFileExt(dbFile, '.dat');
    dbStorage := TDBStorage.Create;
    if not StorageOpen(dbStorage.Storage, dbFile, datFile) then
      raise Exception.Create('Cannot open storage for DB: ' + DBName);
    FDBs.AddObject(DBName, dbStorage);
    Result := dbStorage;
  end
  else
    Result := TDBStorage(FDBs.Objects[idx]);
end;

function TSimpleServer.FindDBConfig(const DBName: string): TDatabaseConfig;
var i: Integer;
begin
  for i := 0 to High(FConfig.Databases) do
    if FConfig.Databases[i].Name = DBName then
      Exit(FConfig.Databases[i]);
  raise Exception.Create('Database not found: ' + DBName);
end;

procedure WriteJSONResponse(var Resp: TFPHTTPConnectionResponse; const Code: Integer; const S: string);
begin
  Resp.ContentType := 'application/json';
  Resp.Code := Code;
  Resp.Content := S;
end;

function LogTimestamp: string;
begin
  Result := FormatDateTime('yyyy-mm-dd hh:nn:ss', Now);
end;

procedure LogInfo(const S: string);
begin
  WriteLn('[', LogTimestamp, '] [INFO] ', S);
end;

procedure LogError(const S: string);
begin
  WriteLn('[', LogTimestamp, '] [ERROR] ', S);
end;

function BytesToString(const Bytes: TByteArray): string;
begin
  SetString(Result, PAnsiChar(@Bytes[0]), Length(Bytes));
end;

function StringToBytes(const S: string): TByteArray;
var
  i: Integer;
begin
  SetLength(Result, Length(S));
  for i := 1 to Length(S) do
    Result[i-1] := Byte(S[i]);
end;

procedure TSimpleServer.HandleRequest(Sender: TObject; var ARequest: TFPHTTPConnectionRequest; var AResponse: TFPHTTPConnectionResponse);
var
  Path, DBName, Key, Value, Token, Action: string;
  DB: TDBStorage;
  i, slashPos: Integer;
  JSONData: TJSONData;
  JSONObject: TJSONObject;
  Password, JWT: string;
  DBConf: TDatabaseConfig;
  keyBytes, valueBytes: TByteArray;
  keys: TByteArrayDynArray;
begin
  Path := ARequest.URI;
  if AnsiStartsStr('/db/', Path) then
  begin
    slashPos := PosEx('/', Path, 5);
    if slashPos > 0 then
    begin
      DBName := Copy(Path, 5, slashPos - 5);
      Action := Copy(Path, slashPos, Length(Path) - slashPos + 1);
    end
    else
    begin
      DBName := Copy(Path, 5, Length(Path));
      Action := '';
    end;
    if DBName = '' then
    begin
      LogError('No dbname in request: ' + Path);
      WriteJSONResponse(AResponse, 400, '{"error":"No dbname"}');
      Exit;
    end;
    DB := GetDB(DBName);
    Token := ARequest.CustomHeaders.Values['X-Auth-Token'];
    if not ValidateJWT(Token, FConfig.JwtSecret, DBName) then
    begin
      LogError('Unauthorized access to ' + Path);
      WriteJSONResponse(AResponse, 401, '{"error":"Unauthorized"}');
      Exit;
    end;
    if AnsiStartsStr('/set', Action) and (ARequest.Method = 'POST') then
    begin
      if Pos('application/json', LowerCase(ARequest.ContentType)) > 0 then
      begin
        try
          JSONData := GetJSON(ARequest.Content);
          if JSONData.JSONType = jtObject then
          begin
            JSONObject := TJSONObject(JSONData);
            Key := JSONObject.Get('key', '');
            Value := JSONObject.Get('value', '');
          end;
        except
          on E: Exception do
          begin
            LogError('Bad JSON in set: ' + E.Message);
            WriteJSONResponse(AResponse, 400, '{"error":"Bad JSON"}');
            Exit;
          end;
        end;
      end
      else
      begin
        Key := ARequest.ContentFields.Values['key'];
        Value := ARequest.ContentFields.Values['value'];
      end;
      if (Key = '') or (Value = '') then
      begin
        LogError('Bad request: empty key or value');
        WriteJSONResponse(AResponse, 400, '{"error":"Bad request"}');
        Exit;
      end;
      keyBytes := StringToBytes(Key);
      valueBytes := StringToBytes(Value);
      if not StorageSet(DB.Storage, keyBytes, Length(keyBytes), valueBytes) then
      begin
        WriteJSONResponse(AResponse, 500, '{"error":"Set failed"}');
        Exit;
      end;
      LogInfo('SET ' + DBName + ': ' + Key + ' = ' + Value);
      WriteJSONResponse(AResponse, 200, '{"result":"ok"}');
      Exit;
    end
    else if AnsiStartsStr('/get', Action) and (ARequest.Method = 'GET') then
    begin
      Key := ARequest.QueryFields.Values['key'];
      if (Key = '') then
      begin
        LogError('Bad request: empty key in get');
        WriteJSONResponse(AResponse, 400, '{"error":"Bad request"}');
        Exit;
      end;
      keyBytes := StringToBytes(Key);
      if not StorageGet(DB.Storage, keyBytes, Length(keyBytes), valueBytes) then
      begin
        LogError('Not found: ' + Key + ' in ' + DBName);
        WriteJSONResponse(AResponse, 404, '{"error":"Not found"}');
        Exit;
      end;
      Value := BytesToString(valueBytes);
      LogInfo('GET ' + DBName + ': ' + Key + ' = ' + Value);
      WriteJSONResponse(AResponse, 200, Format('{"key":"%s","value":"%s"}', [Key, Value]));
      Exit;
    end
    else if AnsiStartsStr('/del', Action) and (ARequest.Method = 'DELETE') then
    begin
      Key := ARequest.QueryFields.Values['key'];
      if (Key = '') then
      begin
        LogError('Bad request: empty key in del');
        WriteJSONResponse(AResponse, 400, '{"error":"Bad request"}');
        Exit;
      end;
      keyBytes := StringToBytes(Key);
      if not StorageDelete(DB.Storage, keyBytes, Length(keyBytes)) then
      begin
        LogError('Not found (del): ' + Key + ' in ' + DBName);
        WriteJSONResponse(AResponse, 404, '{"error":"Not found"}');
        Exit;
      end;
      LogInfo('DEL ' + DBName + ': ' + Key);
      WriteJSONResponse(AResponse, 200, '{"result":"deleted"}');
      Exit;
    end
    else if AnsiStartsStr('/list', Action) and (ARequest.Method = 'GET') then
    begin
      LogInfo('LIST ' + DBName);
      keys := StorageList(DB.Storage);
      AResponse.ContentType := 'application/json';
      AResponse.Code := 200;
      AResponse.Content := '{"keys":[';
      for i := 0 to High(keys) do
      begin
        if i > 0 then AResponse.Content := AResponse.Content + ',';
        AResponse.Content := AResponse.Content + '"' + BytesToString(keys[i]) + '"';
      end;
      AResponse.Content := AResponse.Content + ']}';
      Exit;
    end
    else if AnsiStartsStr('/clear', Action) and (ARequest.Method = 'POST') then
    begin
      StorageClear(DB.Storage);
      LogInfo('CLEAR ' + DBName);
      WriteJSONResponse(AResponse, 200, '{"result":"cleared"}');
      Exit;
    end;
    LogError('Unknown endpoint: ' + Path);
    WriteJSONResponse(AResponse, 404, '{"error":"Unknown endpoint"}');
    Exit;
  end
  else if Path = '/auth' then
  begin
    LogInfo('AUTH request');
    if (ARequest.Method <> 'POST') or (Pos('application/json', LowerCase(ARequest.ContentType)) = 0) then
    begin
      WriteJSONResponse(AResponse, 400, '{"error":"Bad request"}');
      Exit;
    end;
    try
      JSONData := GetJSON(ARequest.Content);
      if (JSONData.JSONType = jtObject) then
      begin
        JSONObject := TJSONObject(JSONData);
        DBName := JSONObject.Get('dbname', '');
        Password := JSONObject.Get('password', '');
        if (DBName = '') or (Password = '') then
        begin
          WriteJSONResponse(AResponse, 400, '{"error":"Bad request"}');
          Exit;
        end;
        try
          DBConf := FindDBConfig(DBName);
        except
          on E: Exception do
          begin
            WriteJSONResponse(AResponse, 404, '{"error":"Not found"}');
            Exit;
          end;
        end;
        if not CheckPassword(DBConf, Password) then
        begin
          WriteJSONResponse(AResponse, 401, '{"error":"Invalid credentials"}');
          Exit;
        end;
        JWT := GenerateJWT(DBName, FConfig.JwtSecret, FConfig.JwtExpiry);
        WriteJSONResponse(AResponse, 200, '{"token":"' + JWT + '"}');
        Exit;
      end;
    except
      on E: Exception do
      begin
        WriteJSONResponse(AResponse, 400, '{"error":"Bad JSON"}');
        Exit;
      end;
    end;
    WriteJSONResponse(AResponse, 400, '{"error":"Bad request"}');
    Exit;
  end
  else
  begin
    LogError('Not found: ' + Path);
    WriteJSONResponse(AResponse, 404, '{"error":"Not found"}');
    Exit;
  end;
end;

procedure StartServer(const Conf: TAppConfig);
var
  Server: TSimpleServer;
begin
  Server := TSimpleServer.Create(nil, Conf);
  try
    Server.OnRequest := @Server.HandleRequest;
    Server.Threaded := False;
    {$IFDEF FPC_HAS_FEATURE_ADDRESS}
    Server.Address := Conf.Address;
    {$ENDIF}
    Server.Port := Conf.Port;
    WriteLn('Starting HTTP server at ', Conf.Address, ':', Conf.Port);
    Server.Active := True;
    while Server.Active do
      Sleep(1000);
  finally
    Server.Free;
  end;
end;

end. 