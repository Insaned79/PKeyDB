{$MODE OBJFPC}
unit auth;

interface

uses
  SysUtils, config;

function CheckPassword(const DB: TDatabaseConfig; const Password: string): Boolean;
function GenerateJWT(const DBName, Secret: string; ExpiryMinutes: Integer): string;
function ValidateJWT(const Token, Secret, DBName: string): Boolean;

implementation

uses
  HlpHashFactory, HlpConverters, HlpIHash, HlpIHashResult, HlpHashResult, LazJWT, DateUtils, fpjson;

function SHA256Hex(const S: string): string;
var
  hash: IHash;
  hashResult: IHashResult;
begin
  hash := THashFactory.TCrypto.CreateSHA2_256;
  hashResult := hash.ComputeString(S, TEncoding.UTF8);
  Result := LowerCase(hashResult.ToString());
end;

function CheckPassword(const DB: TDatabaseConfig; const Password: string): Boolean;
begin
  // DB.Password должен быть hex-строкой SHA256
  Result := SHA256Hex(Password) = LowerCase(DB.Password);
end;

function GenerateJWT(const DBName, Secret: string; ExpiryMinutes: Integer): string;
var
  jwt: ILazJWT;
  nowUnix, expUnix: Int64;
  jti: string;
begin
  nowUnix := DateTimeToUnix(Now);
  expUnix := DateTimeToUnix(IncMinute(Now, ExpiryMinutes));
  jti := IntToHex(Random(MaxInt), 8) + IntToHex(Random(MaxInt), 8);
  jwt := TLazJWT.New
    .SecretJWT(Secret)
    .Iss('PKeyDB')
    .Sub(DBName)
    .Iat(nowUnix)
    .Exp(expUnix)
    .JTI(jti);
  Result := jwt.Token;
end;

function ValidateJWT(const Token, Secret, DBName: string): Boolean;
var
  jwt: ILazJWT;
  payload: TJSONObject;
  nowUnix: Int64;
begin
  Result := False;
  if Token = '' then Exit;
  jwt := TLazJWT.New
    .SecretJWT(Secret)
    .Token(Token);
  try
    jwt.ValidateToken;
  except
    Exit;
  end;
  payload := GetJSON(jwt.PayLoad()) as TJSONObject;
  nowUnix := DateTimeToUnix(Now);
  // Проверяем exp, sub, iss
  if (payload.Get('iss', '') <> 'PKeyDB') then Exit;
  if (payload.Get('sub', '') <> DBName) then Exit;
  if (payload.Get('exp', 0) < nowUnix) then Exit;
  Result := True;
end;

end. 