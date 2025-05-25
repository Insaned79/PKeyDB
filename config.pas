{$MODE OBJFPC}
unit config;

interface

uses
  SysUtils;

type
  TDatabaseConfig = record
    Name: string;
    Path: string;
    Password: string;
    Limit: Int64; // -1 if no limit
    LimitAction: string; // 'error' или 'rotate'
  end;

  TAppConfig = record
    Address: string;
    Port: Integer;
    JwtSecret: string;
    JwtExpiry: Integer;
    Databases: array of TDatabaseConfig;
  end;

function LoadConfig(const FileName: string): TAppConfig;
procedure PrintConfig(const Conf: TAppConfig);

implementation

uses
  IniFiles, StrUtils, Classes;

function ParseLimit(const S: string): Int64;
begin
  if LowerCase(S) = 'none' then
    Result := -1
  else
    Result := StrToInt64Def(S, -1);
end;

function LoadConfig(const FileName: string): TAppConfig;
var
  Ini: TIniFile;
  ResultConfig: TAppConfig;
  DBNames: TStringList;
  i: Integer;
  Key, DBName: string;
  DB: TDatabaseConfig;
  AllKeys: TStringList;
begin
  if not FileExists(FileName) then
    raise Exception.Create('Config file not found: ' + FileName);

  Ini := TIniFile.Create(FileName);
  try
    ResultConfig.Address := Ini.ReadString('server', 'address', '0.0.0.0');
    ResultConfig.Port := Ini.ReadInteger('server', 'port', 8080);
    ResultConfig.JwtSecret := Ini.ReadString('server', 'jwt_secret', '');
    ResultConfig.JwtExpiry := Ini.ReadInteger('server', 'jwt_expiry', 60);

    // Parse databases
    AllKeys := TStringList.Create;
    DBNames := TStringList.Create;
    try
      Ini.ReadSection('databases', AllKeys);
      // Extract unique db names from keys like db1.path, db2.password, etc.
      for i := 0 to AllKeys.Count - 1 do
      begin
        Key := AllKeys[i];
        if Pos('.', Key) > 0 then
        begin
          DBName := LeftStr(Key, Pos('.', Key) - 1);
          if DBNames.IndexOf(DBName) = -1 then
            DBNames.Add(DBName);
        end;
      end;
      SetLength(ResultConfig.Databases, DBNames.Count);
      for i := 0 to DBNames.Count - 1 do
      begin
        DB.Name := DBNames[i];
        DB.Path := Ini.ReadString('databases', DB.Name + '.path', '');
        DB.Password := Ini.ReadString('databases', DB.Name + '.password', '');
        DB.Limit := ParseLimit(Ini.ReadString('databases', DB.Name + '.limit', 'none'));
        DB.LimitAction := Ini.ReadString('databases', DB.Name + '.limit_action', 'error');
        ResultConfig.Databases[i] := DB;
      end;
    finally
      DBNames.Free;
      AllKeys.Free;
    end;
  finally
    Ini.Free;
  end;
  LoadConfig := ResultConfig;
end;

procedure PrintConfig(const Conf: TAppConfig);
var
  i: Integer;
begin
  WriteLn('Server: ', Conf.Address, ':', Conf.Port);
  if Conf.JwtSecret <> '' then
    WriteLn('JWT secret: [set, length=', Length(Conf.JwtSecret), ']')
  else
    WriteLn('JWT secret: [not set]');
  WriteLn('JWT expiry: ', Conf.JwtExpiry, ' min');
  WriteLn('Databases: ', Length(Conf.Databases));
  for i := 0 to High(Conf.Databases) do
  begin
    WriteLn('  [', Conf.Databases[i].Name, ']');
    WriteLn('    Path: ', Conf.Databases[i].Path);
    // Password is not logged for security reasons
    if Conf.Databases[i].Limit < 0 then
      WriteLn('    Limit: none')
    else
      WriteLn('    Limit: ', Conf.Databases[i].Limit, ' bytes');
    WriteLn('    LimitAction: ', Conf.Databases[i].LimitAction);
  end;
end;

end. 