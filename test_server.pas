{$mode objfpc}
program test_server;

uses
  SysUtils, fpcunit, testregistry, fphttpclient, fpjson, jsonparser, Classes, server, console_testrunner;

type
  TServerTest = class(TTestCase)
  private
    FPort: Integer;
    FToken: string;
    procedure StartTestServer;
    procedure StopTestServer;
    function BaseURL: string;
    function HTTPPostJSON(const URL, JSON: string; const Token: string = ''): string;
    function HTTPGet(const URL: string; const Token: string = ''): string;
    function HTTPDelete(const URL: string; const Token: string = ''): string;
  protected
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure TestAuth;
    procedure TestSetGet;
    procedure TestDelete;
    procedure TestList;
    procedure TestClear;
    procedure TestErrors;
  end;

type
  TServerRunner = class(TThread)
  private
    FAddress: string;
    FPort: Integer;
  protected
    procedure Execute; override;
  public
    constructor Create(const Address: string; Port: Integer);
  end;

constructor TServerRunner.Create(const Address: string; Port: Integer);
begin
  inherited Create(False);
  FAddress := Address;
  FPort := Port;
  FreeOnTerminate := False;
end;

procedure TServerRunner.Execute;
begin
  StartServer(FAddress, FPort);
end;

var
  ServerThread: TServerRunner = nil;

procedure TServerTest.StartTestServer;
begin
  ServerThread := TServerRunner.Create('127.0.0.1', FPort);
  Sleep(500); // Дать серверу время стартовать
end;

procedure TServerTest.StopTestServer;
begin
  if Assigned(ServerThread) then
    ServerThread.Terminate;
end;

function TServerTest.BaseURL: string;
begin
  Result := Format('http://127.0.0.1:%d', [FPort]);
end;

function TServerTest.HTTPPostJSON(const URL, JSON: string; const Token: string): string;
var
  Client: TFPHTTPClient;
  Stream: TStringStream;
begin
  Client := TFPHTTPClient.Create(nil);
  Stream := TStringStream.Create('');
  try
    Client.AddHeader('Content-Type', 'application/json');
    if Token <> '' then
      Client.AddHeader('Authorization', 'Bearer ' + Token);
    Client.RequestBody := TStringStream.Create(JSON);
    Client.Post(URL, Stream);
    Result := Stream.DataString;
  finally
    Stream.Free;
    Client.Free;
  end;
end;

function TServerTest.HTTPGet(const URL: string; const Token: string): string;
var
  Client: TFPHTTPClient;
begin
  Client := TFPHTTPClient.Create(nil);
  try
    if Token <> '' then
      Client.AddHeader('Authorization', 'Bearer ' + Token);
    Result := Client.Get(URL);
  finally
    Client.Free;
  end;
end;

function TServerTest.HTTPDelete(const URL: string; const Token: string): string;
var
  Client: TFPHTTPClient;
begin
  Client := TFPHTTPClient.Create(nil);
  try
    if Token <> '' then
      Client.AddHeader('Authorization', 'Bearer ' + Token);
    Result := Client.Delete(URL);
  finally
    Client.Free;
  end;
end;

procedure TServerTest.SetUp;
begin
  Randomize;
  FPort := 10000 + Random(5000);
  StartTestServer;
end;

procedure TServerTest.TearDown;
begin
  StopTestServer;
end;

procedure TServerTest.TestAuth;
var
  Resp: string;
  JSON: TJSONData;
begin
  Resp := HTTPPostJSON(BaseURL + '/auth', '{"dbname":"test","password":"any"}');
  JSON := GetJSON(Resp);
  AssertEquals('testtoken', JSON.FindPath('token').AsString);
  FToken := JSON.FindPath('token').AsString;
end;

procedure TServerTest.TestSetGet;
var
  Resp: string;
  JSON: TJSONData;
begin
  TestAuth;
  Resp := HTTPPostJSON(BaseURL + '/db/test/set', '{"key":"foo","value":"bar"}', FToken);
  JSON := GetJSON(Resp);
  AssertEquals('ok', JSON.FindPath('result').AsString);

  Resp := HTTPGet(BaseURL + '/db/test/get?key=foo', FToken);
  JSON := GetJSON(Resp);
  AssertEquals('foo', JSON.FindPath('key').AsString);
  AssertEquals('bar', JSON.FindPath('value').AsString);
end;

procedure TServerTest.TestDelete;
var
  Resp: string;
  JSON: TJSONData;
begin
  TestSetGet;
  Resp := HTTPDelete(BaseURL + '/db/test/del?key=foo', FToken);
  JSON := GetJSON(Resp);
  AssertEquals('deleted', JSON.FindPath('result').AsString);
end;

procedure TServerTest.TestList;
var
  Resp: string;
  JSON: TJSONData;
begin
  TestSetGet;
  Resp := HTTPGet(BaseURL + '/db/test/list', FToken);
  JSON := GetJSON(Resp);
  AssertTrue(JSON.FindPath('keys').JSONType = jtArray);
end;

procedure TServerTest.TestClear;
var
  Resp: string;
  JSON: TJSONData;
begin
  TestSetGet;
  Resp := HTTPPostJSON(BaseURL + '/db/test/clear', '{}', FToken);
  JSON := GetJSON(Resp);
  AssertEquals('cleared', JSON.FindPath('result').AsString);
end;

procedure TServerTest.TestErrors;
var
  Resp: string;
  JSON: TJSONData;
begin
  // Без токена
  Resp := HTTPGet(BaseURL + '/db/test/get?key=foo');
  JSON := GetJSON(Resp);
  AssertEquals('Unauthorized', JSON.FindPath('error').AsString);

  // Неизвестный ключ
  TestAuth;
  Resp := HTTPGet(BaseURL + '/db/test/get?key=notfound', FToken);
  JSON := GetJSON(Resp);
  AssertEquals('Not found', JSON.FindPath('error').AsString);
end;

begin
  RegisterTest(TServerTest);
  RunRegisteredTests;
end. 