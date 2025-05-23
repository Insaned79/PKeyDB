{$MODE OBJFPC}
program PKeyDB;

uses
  SysUtils, config, server;

var
  ConfigPath: string;
  Conf: TAppConfig;
begin
  if ParamCount > 0 then
    ConfigPath := ParamStr(1)
  else
    ConfigPath := 'pkeydb.ini';

  try
    Conf := LoadConfig(ConfigPath);
    WriteLn('Config loaded successfully:');
    PrintConfig(Conf);
    StartServer(Conf.Address, Conf.Port);
  except
    on E: Exception do
    begin
      WriteLn(StdErr, 'Config loading error: ', E.Message);
      Halt(1);
    end;
  end;
end. 