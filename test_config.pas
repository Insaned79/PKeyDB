{$MODE OBJFPC}
program test_config;

uses
  SysUtils, config;

procedure AssertEqual(const Msg: string; const A, B: string);
begin
  if A <> B then
    WriteLn('  FAIL: ', Msg, ' (expected: ', B, ', got: ', A, ')')
  else
    WriteLn('  OK: ', Msg);
end;

procedure AssertEqualInt(const Msg: string; const A, B: Int64);
begin
  if A <> B then
    WriteLn('  FAIL: ', Msg, ' (expected: ', B, ', got: ', A, ')')
  else
    WriteLn('  OK: ', Msg);
end;

procedure TestLoadConfigSuccess;
var
  Conf: TAppConfig;
begin
  WriteLn('Test: LoadConfig with valid file...');
  try
    Conf := LoadConfig('pkeydb.ini');
    WriteLn('  OK: Config loaded');
    PrintConfig(Conf);
    AssertEqual('Address', Conf.Address, '127.0.0.1');
    AssertEqualInt('Port', Conf.Port, 8080);
    AssertEqual('jwt_secret', Conf.JwtSecret, 'supersecretkey');
    AssertEqualInt('jwt_expiry', Conf.JwtExpiry, 60);
    AssertEqualInt('Databases count', Length(Conf.Databases), 2);
    if Length(Conf.Databases) > 0 then
    begin
      AssertEqual('db1 name', Conf.Databases[0].Name, 'db1');
      AssertEqual('db1 path', Conf.Databases[0].Path, 'data/db1.db');
      AssertEqual('db1 password', Conf.Databases[0].Password, 'secret1');
      AssertEqualInt('db1 limit', Conf.Databases[0].Limit, 10485760);
      AssertEqual('db1 limit_action', Conf.Databases[0].LimitAction, 'rotate');
    end;
    if Length(Conf.Databases) > 1 then
    begin
      AssertEqual('db2 name', Conf.Databases[1].Name, 'db2');
      AssertEqual('db2 path', Conf.Databases[1].Path, 'data/db2.db');
      AssertEqual('db2 password', Conf.Databases[1].Password, 'secret2');
      AssertEqualInt('db2 limit', Conf.Databases[1].Limit, -1);
      AssertEqual('db2 limit_action', Conf.Databases[1].LimitAction, 'error');
    end;
  except
    on E: Exception do
      WriteLn('  FAIL: Exception: ', E.Message);
  end;
end;

procedure TestLoadConfigNotFound;
begin
  WriteLn('Test: LoadConfig with missing file...');
  try
    LoadConfig('no_such_file.ini');
    WriteLn('  FAIL: No exception thrown');
  except
    on E: Exception do
      WriteLn('  OK: Exception: ', E.Message);
  end;
end;

begin
  TestLoadConfigSuccess;
  TestLoadConfigNotFound;
end. 