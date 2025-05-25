{$MODE OBJFPC}{$H+}
unit storage;

interface

uses
  SysUtils, Classes, Unix;

const
  MAX_KEY_SIZE = 128;
  DEFAULT_BUCKET_SIZE = 4096; // 4 КБ
  MAX_RECORDS_PER_BUCKET = 2; // для теста split/grow

// --- Структуры ---

type
  QWordArray = array of QWord;
  TByteArray = array of Byte;

  // Запись в бакете
  PBucketRecord = ^TBucketRecord;
  TBucketRecord = packed record
    key_hash: QWord;
    key_size: Word;
    key_data: array[0..MAX_KEY_SIZE-1] of Byte;
    data_offset: QWord;
    data_size: DWord;
    deleted: Byte;
    timestamp: QWord; // время вставки/обновления
  end;

  // Бакет
  PBucket = ^TBucket;
  TBucket = packed record
    local_depth: Byte;
    num_records: Word;
    records: array of TBucketRecord;
  end;

  // Директория
  PDirectory = ^TDirectory;
  TDirectory = packed record
    global_depth: Byte;
    bucket_offsets: QWordArray;
  end;

  TSplitResult = record
    new_bucket: PBucket;
    new_indexes: array of Integer;
  end;

// --- Динамический массив бакетов ---
type
  TBucketPtrArray = array of PBucket;

// --- Структура всей базы ---
type
  TStorage = record
    idx_filename: string;
    dat_filename: string;
    idx_file: File;
    dat_file: File;
    directory: TDirectory;
    buckets: TBucketPtrArray;
    is_open: Boolean;
  end;

// --- Прототипы функций (заглушки) ---
function InitDirectory(depth: Byte): TDirectory;
function InitBucket(local_depth: Byte): TBucket;
procedure SaveDirectoryToFile(const dir: TDirectory; const filename: string);
function LoadDirectoryFromFile(const filename: string): TDirectory;
procedure SaveBucketToFile(const bucket: TBucket; const filename: string);
function LoadBucketFromFile(const filename: string): TBucket;
function DirectoryIndexByHash(hash: QWord; global_depth: Byte): Integer;
function FindRecordInBucket(const bucket: PBucket; key_hash: QWord; const key_data: array of Byte; key_size: Word): Integer;
function AddRecordToBucket(bucket: PBucket; const rec: TBucketRecord; max_records: Integer): Boolean;
function DeleteRecordInBucket(bucket: PBucket; key_hash: QWord; const key_data: array of Byte; key_size: Word): Boolean;
function SplitBucket(bucket: PBucket; local_depth: Byte; max_records: Integer): TSplitResult;
procedure GrowDirectoryIfNeeded(var dir: TDirectory; new_local_depth: Byte);
function InsertWithSplit(var dir: TDirectory; var buckets: TBucketPtrArray; const rec: TBucketRecord; max_records: Integer): Boolean;

// --- Интерфейс для работы с базой ---
function StorageOpen(var storage: TStorage; const idx_filename, dat_filename: string): Boolean;
procedure StorageClose(var storage: TStorage);

// --- Интерфейс для работы с данными ---
function StorageGet(var storage: TStorage; const key_data: array of Byte; key_size: Word; out value: TByteArray): Boolean;
function StorageSet(var storage: TStorage; const key_data: array of Byte; key_size: Word; const value: TByteArray): Boolean;
function StorageDelete(var storage: TStorage; const key_data: array of Byte; key_size: Word): Boolean;

function HashKey(const key_data: array of Byte; key_size: Word): QWord;

procedure CompactBuckets(var dir: TDirectory; var buckets: TBucketPtrArray);

type
  TByteArrayDynArray = array of TByteArray;

function StorageList(var storage: TStorage): TByteArrayDynArray;

function StorageGetTotalSize(var storage: TStorage): QWord;

function GetUnixTime: QWord;

function GetFileSizeByName(const fname: string): QWord;

function StorageFindOldestKey(var storage: TStorage; out key: TByteArray): Boolean;

function StorageDeleteOldest(var storage: TStorage): Boolean;

procedure StorageCompactData(var storage: TStorage);

implementation

function InitDirectory(depth: Byte): TDirectory;
begin
  Result.global_depth := depth;
  SetLength(Result.bucket_offsets, 1 shl depth);
  FillChar(Result.bucket_offsets[0], Length(Result.bucket_offsets)*SizeOf(QWord), 0);
end;

function InitBucket(local_depth: Byte): TBucket;
begin
  Result.local_depth := local_depth;
  Result.num_records := 0;
  SetLength(Result.records, 0);
end;

procedure SaveDirectoryToFile(const dir: TDirectory; const filename: string);
var
  f: File;
  n: LongInt;
begin
  AssignFile(f, filename);
  Rewrite(f, 1);
  try
    BlockWrite(f, dir.global_depth, SizeOf(dir.global_depth));
    n := Length(dir.bucket_offsets);
    BlockWrite(f, n, SizeOf(n));
    if n > 0 then
      BlockWrite(f, dir.bucket_offsets[0], n * SizeOf(QWord));
  finally
    CloseFile(f);
  end;
end;

function LoadDirectoryFromFile(const filename: string): TDirectory;
var
  f: File;
  n: LongInt;
begin
  AssignFile(f, filename);
  Reset(f, 1);
  try
    BlockRead(f, Result.global_depth, SizeOf(Result.global_depth));
    BlockRead(f, n, SizeOf(n));
    SetLength(Result.bucket_offsets, n);
    if n > 0 then
      BlockRead(f, Result.bucket_offsets[0], n * SizeOf(QWord));
  finally
    CloseFile(f);
  end;
end;

procedure SaveBucketToFile(const bucket: TBucket; const filename: string);
var
  f: File;
  i: Integer;
  rec: TBucketRecord;
begin
  AssignFile(f, filename);
  Rewrite(f, 1);
  try
    BlockWrite(f, bucket.local_depth, SizeOf(bucket.local_depth));
    BlockWrite(f, bucket.num_records, SizeOf(bucket.num_records));
    for i := 0 to bucket.num_records-1 do
    begin
      rec := bucket.records[i];
      BlockWrite(f, rec.key_hash, SizeOf(rec.key_hash));
      BlockWrite(f, rec.key_size, SizeOf(rec.key_size));
      BlockWrite(f, rec.key_data, MAX_KEY_SIZE);
      BlockWrite(f, rec.data_offset, SizeOf(rec.data_offset));
      BlockWrite(f, rec.data_size, SizeOf(rec.data_size));
      BlockWrite(f, rec.deleted, SizeOf(rec.deleted));
      BlockWrite(f, rec.timestamp, SizeOf(rec.timestamp));
    end;
  finally
    CloseFile(f);
  end;
end;

function LoadBucketFromFile(const filename: string): TBucket;
var
  f: File;
  i: Integer;
  rec: TBucketRecord;
begin
  AssignFile(f, filename);
  Reset(f, 1);
  try
    BlockRead(f, Result.local_depth, SizeOf(Result.local_depth));
    BlockRead(f, Result.num_records, SizeOf(Result.num_records));
    SetLength(Result.records, Result.num_records);
    for i := 0 to Result.num_records-1 do
    begin
      BlockRead(f, rec.key_hash, SizeOf(rec.key_hash));
      BlockRead(f, rec.key_size, SizeOf(rec.key_size));
      BlockRead(f, rec.key_data, MAX_KEY_SIZE);
      BlockRead(f, rec.data_offset, SizeOf(rec.data_offset));
      BlockRead(f, rec.data_size, SizeOf(rec.data_size));
      BlockRead(f, rec.deleted, SizeOf(rec.deleted));
      BlockRead(f, rec.timestamp, SizeOf(rec.timestamp));
      Result.records[i] := rec;
    end;
  finally
    CloseFile(f);
  end;
end;

function DirectoryIndexByHash(hash: QWord; global_depth: Byte): Integer;
begin
  // Берём global_depth младших бит хеша
  Result := Integer(hash and ((1 shl global_depth) - 1));
end;

function FindRecordInBucket(const bucket: PBucket; key_hash: QWord; const key_data: array of Byte; key_size: Word): Integer;
var
  i, j: Integer;
  match: Boolean;
begin
  for i := 0 to bucket^.num_records-1 do
    if (bucket^.records[i].key_hash = key_hash) and (bucket^.records[i].key_size = key_size) then
    begin
      match := True;
      for j := 0 to key_size-1 do
        if bucket^.records[i].key_data[j] <> key_data[j] then
        begin
          match := False;
          Break;
        end;
      if match and (bucket^.records[i].deleted = 0) then
      begin
        Result := i;
        Exit;
      end;
    end;
  Result := -1;
end;

function AddRecordToBucket(bucket: PBucket; const rec: TBucketRecord; max_records: Integer): Boolean;
var
  i: Integer;
begin
  if FindRecordInBucket(bucket, rec.key_hash, rec.key_data, rec.key_size) >= 0 then
  begin
    Result := False; // дубликат
    Exit;
  end;
  // Пытаемся найти "дырку" (удалённую запись)
  for i := 0 to bucket^.num_records-1 do
    if bucket^.records[i].deleted = 1 then
    begin
      bucket^.records[i] := rec;
      Result := True;
      Exit;
    end;
  // Если нет дырок — добавляем в конец, если не переполнено
  if bucket^.num_records >= max_records then
  begin
    Result := False;
    Exit;
  end;
  SetLength(bucket^.records, bucket^.num_records + 1);
  bucket^.records[bucket^.num_records] := rec;
  Inc(bucket^.num_records);
  Result := True;
end;

function DeleteRecordInBucket(bucket: PBucket; key_hash: QWord; const key_data: array of Byte; key_size: Word): Boolean;
var
  idx: Integer;
begin
  idx := FindRecordInBucket(bucket, key_hash, key_data, key_size);
  if idx >= 0 then
  begin
    bucket^.records[idx].deleted := 1;
    Result := True;
  end
  else
    Result := False;
end;

function SplitBucket(bucket: PBucket; local_depth: Byte; max_records: Integer): TSplitResult;
var
  i, n, mask, idx: Integer;
  recs_keep, recs_move: array of TBucketRecord;
begin
  bucket^.local_depth := local_depth;
  mask := 1 shl (local_depth - 1);
  SetLength(recs_keep, 0);
  SetLength(recs_move, 0);
  for i := 0 to bucket^.num_records-1 do
    if (bucket^.records[i].deleted = 0) and ((bucket^.records[i].key_hash and mask) <> 0) then
    begin
      idx := Length(recs_move);
      SetLength(recs_move, idx+1);
      recs_move[idx] := bucket^.records[i];
    end
    else
    begin
      idx := Length(recs_keep);
      SetLength(recs_keep, idx+1);
      recs_keep[idx] := bucket^.records[i];
    end;
  bucket^.num_records := Length(recs_keep);
  SetLength(bucket^.records, bucket^.num_records);
  for i := 0 to High(recs_keep) do
    bucket^.records[i] := recs_keep[i];
  New(Result.new_bucket);
  Result.new_bucket^ := InitBucket(local_depth);
  Result.new_bucket^.num_records := Length(recs_move);
  SetLength(Result.new_bucket^.records, Result.new_bucket^.num_records);
  for i := 0 to High(recs_move) do
    Result.new_bucket^.records[i] := recs_move[i];
  n := 1 shl local_depth;
  SetLength(Result.new_indexes, 0);
  for i := 0 to n-1 do
    if (i and mask) <> 0 then
    begin
      idx := Length(Result.new_indexes);
      SetLength(Result.new_indexes, idx+1);
      Result.new_indexes[idx] := i;
    end;
end;

procedure GrowDirectoryIfNeeded(var dir: TDirectory; new_local_depth: Byte);
var
  old_size, i: Integer;
begin
  if new_local_depth > dir.global_depth then
  begin
    old_size := Length(dir.bucket_offsets);
    SetLength(dir.bucket_offsets, old_size * 2);
    for i := 0 to old_size-1 do
      dir.bucket_offsets[i + old_size] := dir.bucket_offsets[i];
    Inc(dir.global_depth);
  end;
end;

function InsertWithSplit(var dir: TDirectory; var buckets: TBucketPtrArray; const rec: TBucketRecord; max_records: Integer): Boolean;
var
  idx, bidx, i, new_bidx: Integer;
  split_res: TSplitResult;
  local_depth: Byte;
begin
  idx := DirectoryIndexByHash(rec.key_hash, dir.global_depth);
  bidx := dir.bucket_offsets[idx];
  if AddRecordToBucket(buckets[bidx], rec, max_records) then
  begin
    Result := True;
    Exit;
  end;
  local_depth := buckets[bidx]^.local_depth + 1;
  split_res := SplitBucket(buckets[bidx], local_depth, max_records);
  new_bidx := Length(buckets);
  SetLength(buckets, new_bidx + 1);
  buckets[new_bidx] := split_res.new_bucket;
  GrowDirectoryIfNeeded(dir, local_depth);
  for i := 0 to High(split_res.new_indexes) do
    if dir.bucket_offsets[split_res.new_indexes[i]] = bidx then
      dir.bucket_offsets[split_res.new_indexes[i]] := new_bidx;
  idx := DirectoryIndexByHash(rec.key_hash, dir.global_depth);
  bidx := dir.bucket_offsets[idx];
  Result := AddRecordToBucket(buckets[bidx], rec, max_records);
end;

function StorageOpen(var storage: TStorage; const idx_filename, dat_filename: string): Boolean;
var
  idx_exists, dat_exists: Boolean;
  dir: TDirectory;
  bucket: TBucket;
  f: File;
  i, n, bidx: Integer;
  bucket_files: array of Integer;
begin
  storage.idx_filename := idx_filename;
  storage.dat_filename := dat_filename;
  storage.is_open := False;
  idx_exists := FileExists(idx_filename);
  dat_exists := FileExists(dat_filename);
  if not idx_exists then
  begin
    // Новый индекс: директория глубины 1, один бакет
    dir := InitDirectory(1);
    dir.bucket_offsets[0] := 0;
    dir.bucket_offsets[1] := 0;
    SaveDirectoryToFile(dir, idx_filename);
    // Новый бакет
    bucket := InitBucket(1);
    SaveBucketToFile(bucket, idx_filename + '.bucket0');
    // Пустой файл данных
    AssignFile(f, dat_filename);
    Rewrite(f, 1);
    CloseFile(f);
  end;
  // Открываем файлы (на запись/чтение)
  AssignFile(storage.idx_file, idx_filename);
  {$I-} Reset(storage.idx_file, 1); {$I+}
  if IOResult <> 0 then Exit(False);
  AssignFile(storage.dat_file, dat_filename);
  {$I-} Reset(storage.dat_file, 1); {$I+}
  if IOResult <> 0 then
  begin
    CloseFile(storage.idx_file);
    Exit(False);
  end;
  storage.is_open := True;
  // Загружаем directory
  storage.directory := LoadDirectoryFromFile(idx_filename);
  // Загружаем бакеты (уникальные индексы)
  n := 0;
  SetLength(bucket_files, 0);
  for i := 0 to High(storage.directory.bucket_offsets) do
  begin
    bidx := storage.directory.bucket_offsets[i];
    if (bidx >= n) then
    begin
      n := bidx + 1;
      SetLength(bucket_files, n);
    end;
    bucket_files[bidx] := 1;
  end;
  SetLength(storage.buckets, n);
  for i := 0 to n-1 do
  begin
    New(storage.buckets[i]);
    storage.buckets[i]^ := LoadBucketFromFile(idx_filename + '.bucket' + IntToStr(i));
  end;
  CompactBuckets(storage.directory, storage.buckets);
  Result := True;
end;

procedure StorageClose(var storage: TStorage);
var
  i: Integer;
  fname: string;
begin
  if not storage.is_open then Exit;
  CompactBuckets(storage.directory, storage.buckets);
  // Сохраняем директорию
  SaveDirectoryToFile(storage.directory, storage.idx_filename);
  // Сохраняем все бакеты
  for i := 0 to High(storage.buckets) do
  begin
    fname := storage.idx_filename + '.bucket' + IntToStr(i);
    SaveBucketToFile(storage.buckets[i]^, fname);
    Dispose(storage.buckets[i]);
  end;
  SetLength(storage.buckets, 0);
  // Закрываем файлы
  CloseFile(storage.idx_file);
  CloseFile(storage.dat_file);
  storage.is_open := False;
end;

function HashKey(const key_data: array of Byte; key_size: Word): QWord;
var
  i: Integer;
begin
  // FNV-1a 64-bit
  Result := $CBF29CE484222325;
  for i := 0 to key_size-1 do
  begin
    Result := Result xor key_data[i];
    Result := Result * $100000001B3;
  end;
end;

function StorageGet(var storage: TStorage; const key_data: array of Byte; key_size: Word; out value: TByteArray): Boolean;
var
  key_hash: QWord;
  idx, bidx, rec_idx: Integer;
  bucket: PBucket;
  rec: TBucketRecord;
begin
  Result := False;
  key_hash := HashKey(key_data, key_size);
  idx := DirectoryIndexByHash(key_hash, storage.directory.global_depth);
  bidx := storage.directory.bucket_offsets[idx];
  bucket := storage.buckets[bidx];
  rec_idx := FindRecordInBucket(bucket, key_hash, key_data, key_size);
  if rec_idx < 0 then Exit(False);
  rec := bucket^.records[rec_idx];
  if rec.deleted <> 0 then Exit(False);
  // Читаем value из .dat
  SetLength(value, rec.data_size);
  Seek(storage.dat_file, rec.data_offset+1); // Seek 1-based
  BlockRead(storage.dat_file, value[0], rec.data_size);
  Result := True;
end;

function GetUnixTime: QWord;
begin
  Result := QWord(Round((Now - EncodeDate(1970,1,1)) * 86400));
end;

function StorageSet(var storage: TStorage; const key_data: array of Byte; key_size: Word; const value: TByteArray): Boolean;
var
  key_hash: QWord;
  idx, bidx, rec_idx: Integer;
  bucket: PBucket;
  rec: TBucketRecord;
  offset: QWord;
begin
  key_hash := HashKey(key_data, key_size);
  idx := DirectoryIndexByHash(key_hash, storage.directory.global_depth);
  bidx := storage.directory.bucket_offsets[idx];
  bucket := storage.buckets[bidx];
  rec_idx := FindRecordInBucket(bucket, key_hash, key_data, key_size);
  if rec_idx >= 0 then
  begin
    Seek(storage.dat_file, 0);
    offset := FileSize(storage.dat_file);
    Seek(storage.dat_file, offset+1);
    BlockWrite(storage.dat_file, value[0], Length(value));
    bucket^.records[rec_idx].data_offset := offset;
    bucket^.records[rec_idx].data_size := Length(value);
    bucket^.records[rec_idx].deleted := 0;
    bucket^.records[rec_idx].timestamp := GetUnixTime;
    Result := True;
    Exit;
  end;
  // Записываем value в конец .dat
  Seek(storage.dat_file, 0);
  offset := FileSize(storage.dat_file);
  Seek(storage.dat_file, offset+1);
  BlockWrite(storage.dat_file, value[0], Length(value));
  // Формируем запись
  rec.key_hash := key_hash;
  rec.key_size := key_size;
  FillChar(rec.key_data, MAX_KEY_SIZE, 0);
  Move(key_data[0], rec.key_data[0], key_size);
  rec.data_offset := offset;
  rec.data_size := Length(value);
  rec.deleted := 0;
  rec.timestamp := GetUnixTime;
  // Пытаемся добавить в бакет
  if AddRecordToBucket(bucket, rec, MAX_RECORDS_PER_BUCKET) then
  begin
    Result := True;
    Exit;
  end;
  // Split/grow
  if not InsertWithSplit(storage.directory, storage.buckets, rec, MAX_RECORDS_PER_BUCKET) then
  begin
    Result := False;
    Exit;
  end;
  CompactBuckets(storage.directory, storage.buckets);
  Result := True;
end;

function StorageDelete(var storage: TStorage; const key_data: array of Byte; key_size: Word): Boolean;
var
  key_hash: QWord;
  idx, bidx, rec_idx: Integer;
  bucket: PBucket;
begin
  key_hash := HashKey(key_data, key_size);
  idx := DirectoryIndexByHash(key_hash, storage.directory.global_depth);
  bidx := storage.directory.bucket_offsets[idx];
  bucket := storage.buckets[bidx];
  rec_idx := FindRecordInBucket(bucket, key_hash, key_data, key_size);
  if rec_idx < 0 then
  begin
    Result := False;
    Exit;
  end;
  bucket^.records[rec_idx].deleted := 1;
  Result := True;
end;

procedure CompactBuckets(var dir: TDirectory; var buckets: TBucketPtrArray);
var
  i, n, old_idx, new_idx: Integer;
  old_to_new: array of Integer;
  used: array of Boolean;
  new_buckets: TBucketPtrArray;
begin
  n := 0;
  SetLength(used, Length(buckets));
  for i := 0 to High(used) do used[i] := False;
  // Отмечаем используемые бакеты
  for i := 0 to High(dir.bucket_offsets) do
    used[dir.bucket_offsets[i]] := True;
  // Строим отображение old_idx -> new_idx
  SetLength(old_to_new, Length(buckets));
  for i := 0 to High(buckets) do
    if used[i] then
    begin
      old_to_new[i] := n;
      Inc(n);
    end
    else
      old_to_new[i] := -1;
  // Пересобираем массив бакетов
  SetLength(new_buckets, n);
  for i := 0 to High(buckets) do
    if used[i] then
      new_buckets[old_to_new[i]] := buckets[i];
  // Обновляем bucket_offsets
  for i := 0 to High(dir.bucket_offsets) do
    dir.bucket_offsets[i] := old_to_new[dir.bucket_offsets[i]];
  buckets := new_buckets;
end;

function StorageList(var storage: TStorage): TByteArrayDynArray;
var
  i, j, n: Integer;
  res: TByteArrayDynArray;
  bucket: PBucket;
  rec: TBucketRecord;
  key: TByteArray;
begin
  n := 0;
  SetLength(res, 0);
  for i := 0 to High(storage.buckets) do
  begin
    bucket := storage.buckets[i];
    for j := 0 to bucket^.num_records-1 do
    begin
      rec := bucket^.records[j];
      if rec.deleted = 0 then
      begin
        SetLength(key, rec.key_size);
        Move(rec.key_data[0], key[0], rec.key_size);
        SetLength(res, n+1);
        res[n] := key;
        Inc(n);
      end;
    end;
  end;
  Result := res;
end;

function StorageGetTotalSize(var storage: TStorage): QWord;
begin
  // Размер .dat + .idx
  Result := 0;
  Result := Result + GetFileSizeByName(storage.dat_filename);
  Result := Result + GetFileSizeByName(storage.idx_filename);
end;

function GetFileSizeByName(const fname: string): QWord;
var
  f: File;
  sz: Int64;
begin
  Result := 0;
  if not FileExists(fname) then Exit;
  AssignFile(f, fname);
  {$I-} Reset(f, 1); {$I+}
  if IOResult <> 0 then Exit;
  sz := FileSize(f);
  CloseFile(f);
  if sz >= 0 then Result := QWord(sz);
end;

function StorageFindOldestKey(var storage: TStorage; out key: TByteArray): Boolean;
var
  i, j: Integer;
  min_ts: QWord;
  found: Boolean;
  rec: TBucketRecord;
begin
  min_ts := High(QWord);
  found := False;
  SetLength(key, 0);
  for i := 0 to High(storage.buckets) do
    for j := 0 to storage.buckets[i]^.num_records-1 do
    begin
      rec := storage.buckets[i]^.records[j];
      if (rec.deleted = 0) and (rec.timestamp < min_ts) then
      begin
        min_ts := rec.timestamp;
        SetLength(key, rec.key_size);
        Move(rec.key_data[0], key[0], rec.key_size);
        found := True;
      end;
    end;
  Result := found;
end;

function StorageDeleteOldest(var storage: TStorage): Boolean;
var
  i, j, min_i, min_j: Integer;
  min_ts: QWord;
  found: Boolean;
begin
  min_ts := High(QWord);
  found := False;
  min_i := -1; min_j := -1;
  for i := 0 to High(storage.buckets) do
    for j := 0 to storage.buckets[i]^.num_records-1 do
      if (storage.buckets[i]^.records[j].deleted = 0) and (storage.buckets[i]^.records[j].timestamp < min_ts) then
      begin
        min_ts := storage.buckets[i]^.records[j].timestamp;
        min_i := i; min_j := j;
        found := True;
      end;
  if found then
  begin
    storage.buckets[min_i]^.records[min_j].deleted := 1;
    Result := True;
  end
  else
    Result := False;
end;

procedure StorageCompactData(var storage: TStorage);
var
  tmp_filename: string;
  tmp_file: File;
  new_offset: QWord;
  i, j: Integer;
  value: TByteArray;
begin
  if not storage.is_open then Exit;
  tmp_filename := storage.dat_filename + '.compact';
  AssignFile(tmp_file, tmp_filename);
  Rewrite(tmp_file, 1);
  new_offset := 0;
  // Копируем все актуальные значения
  for i := 0 to High(storage.buckets) do
    for j := 0 to storage.buckets[i]^.num_records-1 do
      with storage.buckets[i]^.records[j] do
        if deleted = 0 then
        begin
          // Читаем value из старого .dat
          SetLength(value, data_size);
          Seek(storage.dat_file, data_offset+1);
          BlockRead(storage.dat_file, value[0], data_size);
          // Записываем в новый .dat
          Seek(tmp_file, new_offset+1);
          BlockWrite(tmp_file, value[0], data_size);
          // Обновляем data_offset
          data_offset := new_offset;
          new_offset := new_offset + data_size;
        end;
  CloseFile(tmp_file);
  // Закрываем старый .dat
  CloseFile(storage.dat_file);
  // Заменяем .dat-файл
  DeleteFile(storage.dat_filename);
  RenameFile(tmp_filename, storage.dat_filename);
  // Переоткрываем .dat
  AssignFile(storage.dat_file, storage.dat_filename);
  Reset(storage.dat_file, 1);
  // Сохраняем индексы
  SaveDirectoryToFile(storage.directory, storage.idx_filename);
  for i := 0 to High(storage.buckets) do
    SaveBucketToFile(storage.buckets[i]^, storage.idx_filename + '.bucket' + IntToStr(i));
end;

end. 