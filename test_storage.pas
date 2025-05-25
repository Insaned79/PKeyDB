{$MODE OBJFPC}{$H+}
program test_storage;

uses
  SysUtils, storage;

procedure TestStorageOpenClose;
var
  storage: TStorage;
begin
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen не вернул True');
    Halt(1);
  end;
  if storage.idx_filename <> 'test.idx' then
  begin
    WriteLn('Ошибка: idx_filename не установлен');
    Halt(1);
  end;
  if storage.dat_filename <> 'test.dat' then
  begin
    WriteLn('Ошибка: dat_filename не установлен');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageOpen/Close: OK');
end;

procedure TestStoragePersistence;
var
  storage: TStorage;
  n: Integer;
begin
  // Открываем базу, добавляем бакет
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (persistence)');
    Halt(1);
  end;
  n := Length(storage.buckets);
  // Добавляем новый бакет вручную
  SetLength(storage.buckets, n+1);
  New(storage.buckets[n]);
  storage.buckets[n]^ := InitBucket(1);
  // Обновляем directory: пусть последний offset указывает на новый бакет
  if Length(storage.directory.bucket_offsets) > 0 then
    storage.directory.bucket_offsets[High(storage.directory.bucket_offsets)] := n;
  StorageClose(storage);
  // Снова открываем и проверяем
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (persistence 2)');
    Halt(1);
  end;
  if Length(storage.buckets) <> n+1 then
  begin
    WriteLn('Ошибка: бакет не сохранился');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StoragePersistence: OK');
end;

procedure TestStorageGet;
var
  storage: TStorage;
  key, wrong_key: TByteArray;
  value, out_value: TByteArray;
  rec: TBucketRecord;
  pos: QWord;
  idx, bidx: Integer;
  f: File;
begin
  // Удаляем старые файлы для чистоты теста
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  // Открываем базу
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (get)');
    Halt(1);
  end;
  // Готовим ключ и значение
  SetLength(key, 3); key[0]:=1; key[1]:=2; key[2]:=3;
  SetLength(value, 4); value[0]:=10; value[1]:=20; value[2]:=30; value[3]:=40;
  // Используем StorageSet для добавления
  if not StorageSet(storage, key, 3, value) then
  begin
    WriteLn('Ошибка: StorageSet не смог добавить ключ');
    Halt(1);
  end;
  // Проверяем обновление значения
  value[0]:=99; value[1]:=88; value[2]:=77; value[3]:=66;
  if not StorageSet(storage, key, 3, value) then
  begin
    WriteLn('Ошибка: StorageSet не смог обновить ключ');
    Halt(1);
  end;
  StorageClose(storage);
  // Проверяем StorageGet
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (get2)');
    Halt(1);
  end;
  if not StorageGet(storage, key, 3, out_value) then
  begin
    WriteLn('Ошибка: StorageGet не нашёл существующий ключ');
    Halt(1);
  end;
  if (Length(out_value) <> 4) or (out_value[0]<>99) or (out_value[1]<>88) or (out_value[2]<>77) or (out_value[3]<>66) then
  begin
    WriteLn('Ошибка: StorageGet вернул неверное значение после обновления');
    Halt(1);
  end;
  // Проверяем несуществующий ключ
  SetLength(wrong_key, 3); wrong_key[0]:=9; wrong_key[1]:=8; wrong_key[2]:=7;
  if StorageGet(storage, wrong_key, 3, out_value) then
  begin
    WriteLn('Ошибка: StorageGet нашёл несуществующий ключ');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageGet: OK');
end;

procedure TestStorageSplitGrow;
var
  storage: TStorage;
  key: TByteArray;
  value, out_value: TByteArray;
  i: Integer;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (split)');
    Halt(1);
  end;
  SetLength(key, 2);
  SetLength(value, 2);
  // Вставляем 10 разных ключей
  for i := 0 to 9 do
  begin
    key[0] := i; key[1] := 42;
    value[0] := i; value[1] := 99;
    if not StorageSet(storage, key, 2, value) then
    begin
      WriteLn('Ошибка: StorageSet split/grow при вставке ', i);
      Halt(1);
    end;
  end;
  StorageClose(storage);
  // Проверяем, что все значения читаются
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (split2)');
    Halt(1);
  end;
  for i := 0 to 9 do
  begin
    key[0] := i; key[1] := 42;
    if not StorageGet(storage, key, 2, out_value) then
    begin
      WriteLn('Ошибка: StorageGet split/grow не нашёл ключ ', i);
      Halt(1);
    end;
    if (Length(out_value) <> 2) or (out_value[0] <> i) or (out_value[1] <> 99) then
    begin
      WriteLn('Ошибка: StorageGet split/grow неверное значение для ', i);
      Halt(1);
    end;
  end;
  StorageClose(storage);
  WriteLn('StorageSplitGrow: OK');
end;

procedure TestStorageDelete;
var
  storage: TStorage;
  key: TByteArray;
  value, out_value: TByteArray;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (del)');
    Halt(1);
  end;
  SetLength(key, 2); key[0]:=42; key[1]:=99;
  SetLength(value, 3); value[0]:=1; value[1]:=2; value[2]:=3;
  if not StorageSet(storage, key, 2, value) then
  begin
    WriteLn('Ошибка: StorageSet (del)');
    Halt(1);
  end;
  if not StorageDelete(storage, key, 2) then
  begin
    WriteLn('Ошибка: StorageDelete не удалил ключ');
    Halt(1);
  end;
  if StorageGet(storage, key, 2, out_value) then
  begin
    WriteLn('Ошибка: StorageGet нашёл удалённый ключ');
    Halt(1);
  end;
  // Повторно добавляем тот же ключ
  value[0]:=7; value[1]:=8; value[2]:=9;
  if not StorageSet(storage, key, 2, value) then
  begin
    WriteLn('Ошибка: StorageSet не смог добавить после удаления');
    Halt(1);
  end;
  if not StorageGet(storage, key, 2, out_value) then
  begin
    WriteLn('Ошибка: StorageGet не нашёл ключ после повторного добавления');
    Halt(1);
  end;
  if (Length(out_value) <> 3) or (out_value[0]<>7) or (out_value[1]<>8) or (out_value[2]<>9) then
  begin
    WriteLn('Ошибка: StorageGet вернул неверное значение после повторного добавления');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageDelete: OK');
end;

procedure TestStorageList;
var
  storage: TStorage;
  key: TByteArray;
  value: TByteArray;
  keys: array of TByteArray;
  i, j, found: Integer;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (list)');
    Halt(1);
  end;
  SetLength(key, 2);
  SetLength(value, 1); value[0]:=1;
  // Вставляем 5 ключей
  for i := 1 to 5 do
  begin
    key[0] := i; key[1] := 42;
    if not StorageSet(storage, key, 2, value) then
    begin
      WriteLn('Ошибка: StorageSet (list)');
      Halt(1);
    end;
  end;
  // Удаляем ключ [3,42]
  key[0]:=3; key[1]:=42;
  if not StorageDelete(storage, key, 2) then
  begin
    WriteLn('Ошибка: StorageDelete (list)');
    Halt(1);
  end;
  keys := StorageList(storage);
  if Length(keys) <> 4 then
  begin
    WriteLn('Ошибка: StorageList вернул неверное количество ключей');
    Halt(1);
  end;
  // Проверяем, что [3,42] отсутствует, остальные есть
  for i := 1 to 5 do
  begin
    found := 0;
    for j := 0 to High(keys) do
      if (Length(keys[j])=2) and (keys[j][0]=i) and (keys[j][1]=42) then
        found := 1;
    if (i=3) and (found=1) then
    begin
      WriteLn('Ошибка: StorageList вернул удалённый ключ');
      Halt(1);
    end;
    if (i<>3) and (found=0) then
    begin
      WriteLn('Ошибка: StorageList не вернул существующий ключ');
      Halt(1);
    end;
  end;
  StorageClose(storage);
  WriteLn('StorageList: OK');
end;

procedure TestInitDirectory;
var
  dir: TDirectory;
  i: Integer;
begin
  dir := InitDirectory(3); // 8 бакетов
  if dir.global_depth <> 3 then
  begin
    WriteLn('Ошибка: неверная global_depth');
    Halt(1);
  end;
  if Length(dir.bucket_offsets) <> 8 then
  begin
    WriteLn('Ошибка: неверный размер bucket_offsets');
    Halt(1);
  end;
  for i := 0 to High(dir.bucket_offsets) do
    if dir.bucket_offsets[i] <> 0 then
    begin
      WriteLn('Ошибка: bucket_offsets не инициализированы нулями');
      Halt(1);
    end;
  WriteLn('InitDirectory: OK');
end;

procedure TestInitBucket;
var
  bucket: PBucket;
begin
  New(bucket);
  bucket^ := InitBucket(2);
  if bucket^.local_depth <> 2 then
  begin
    WriteLn('Ошибка: неверная local_depth');
    Halt(1);
  end;
  if bucket^.num_records <> 0 then
  begin
    WriteLn('Ошибка: неверное количество записей');
    Halt(1);
  end;
  if Length(bucket^.records) <> 0 then
  begin
    WriteLn('Ошибка: records должен быть пуст');
    Halt(1);
  end;
  WriteLn('InitBucket: OK');
  Dispose(bucket);
end;

procedure TestDirectorySerialization;
var
  dir1, dir2: TDirectory;
  i: Integer;
  fname: string;
begin
  dir1 := InitDirectory(4); // 16 бакетов
  for i := 0 to High(dir1.bucket_offsets) do
    dir1.bucket_offsets[i] := i * 10;
  fname := 'test_dir.bin';
  SaveDirectoryToFile(dir1, fname);
  dir2 := LoadDirectoryFromFile(fname);
  if dir2.global_depth <> dir1.global_depth then
  begin
    WriteLn('Ошибка: global_depth не совпадает после загрузки');
    Halt(1);
  end;
  if Length(dir2.bucket_offsets) <> Length(dir1.bucket_offsets) then
  begin
    WriteLn('Ошибка: размер bucket_offsets не совпадает после загрузки');
    Halt(1);
  end;
  for i := 0 to High(dir1.bucket_offsets) do
    if dir2.bucket_offsets[i] <> dir1.bucket_offsets[i] then
    begin
      WriteLn('Ошибка: bucket_offsets['+IntToStr(i)+'] не совпадает');
      Halt(1);
    end;
  WriteLn('Directory serialization: OK');
  // DeleteFile(fname);
end;

procedure TestBucketSerialization;
var
  bucket1, bucket2: TBucket;
  i: Integer;
  fname: string;
begin
  bucket1 := InitBucket(5);
  bucket1.num_records := 3;
  SetLength(bucket1.records, 3);
  for i := 0 to 2 do
  begin
    bucket1.records[i].key_hash := 100 + i;
    bucket1.records[i].key_size := 4;
    bucket1.records[i].key_data[0] := Byte(65 + i); // 'A', 'B', 'C'
    bucket1.records[i].key_data[1] := 0;
    bucket1.records[i].key_data[2] := 0;
    bucket1.records[i].key_data[3] := 0;
    bucket1.records[i].data_offset := 1000 + i * 10;
    bucket1.records[i].data_size := 10 + i;
    bucket1.records[i].deleted := Byte(i mod 2);
  end;
  fname := 'test_bucket.bin';
  SaveBucketToFile(bucket1, fname);
  bucket2 := LoadBucketFromFile(fname);
  if bucket2.local_depth <> bucket1.local_depth then
  begin
    WriteLn('Ошибка: local_depth не совпадает после загрузки');
    Halt(1);
  end;
  if bucket2.num_records <> bucket1.num_records then
  begin
    WriteLn('Ошибка: num_records не совпадает после загрузки');
    Halt(1);
  end;
  for i := 0 to bucket1.num_records-1 do
  begin
    if bucket2.records[i].key_hash <> bucket1.records[i].key_hash then
    begin
      WriteLn('Ошибка: key_hash не совпадает для записи ', i);
      Halt(1);
    end;
    if bucket2.records[i].key_size <> bucket1.records[i].key_size then
    begin
      WriteLn('Ошибка: key_size не совпадает для записи ', i);
      Halt(1);
    end;
    if bucket2.records[i].key_data[0] <> bucket1.records[i].key_data[0] then
    begin
      WriteLn('Ошибка: key_data[0] не совпадает для записи ', i);
      Halt(1);
    end;
    if bucket2.records[i].data_offset <> bucket1.records[i].data_offset then
    begin
      WriteLn('Ошибка: data_offset не совпадает для записи ', i);
      Halt(1);
    end;
    if bucket2.records[i].data_size <> bucket1.records[i].data_size then
    begin
      WriteLn('Ошибка: data_size не совпадает для записи ', i);
      Halt(1);
    end;
    if bucket2.records[i].deleted <> bucket1.records[i].deleted then
    begin
      WriteLn('Ошибка: deleted не совпадает для записи ', i);
      Halt(1);
    end;
  end;
  WriteLn('Bucket serialization: OK');
  // DeleteFile(fname);
end;

procedure TestDirectoryIndexByHash;
begin
  if DirectoryIndexByHash($0F, 4) <> $0F then
  begin
    WriteLn('Ошибка: DirectoryIndexByHash($0F, 4)');
    Halt(1);
  end;
  if DirectoryIndexByHash($10, 4) <> 0 then
  begin
    WriteLn('Ошибка: DirectoryIndexByHash($10, 4)');
    Halt(1);
  end;
  if DirectoryIndexByHash($1234, 8) <> $34 then
  begin
    WriteLn('Ошибка: DirectoryIndexByHash($1234, 8)');
    Halt(1);
  end;
  WriteLn('DirectoryIndexByHash: OK');
end;

procedure TestFindAndAddRecordToBucket;
var
  bucket: PBucket;
  rec: TBucketRecord;
  idx: Integer;
  max_records: Integer;
begin
  New(bucket);
  bucket^ := InitBucket(1);
  max_records := 2;
  // Добавляем первую запись
  rec.key_hash := 123;
  rec.key_size := 3;
  rec.key_data[0] := 1; rec.key_data[1] := 2; rec.key_data[2] := 3;
  rec.data_offset := 100;
  rec.data_size := 10;
  rec.deleted := 0;
  if not AddRecordToBucket(bucket, rec, max_records) then
  begin
    WriteLn('Ошибка: не удалось добавить первую запись');
    Halt(1);
  end;
  // Поиск существующей
  idx := FindRecordInBucket(bucket, 123, rec.key_data, 3);
  if idx <> 0 then
  begin
    WriteLn('Ошибка: не найдено добавленное значение');
    Halt(1);
  end;
  // Поиск несуществующей
  rec.key_data[0] := 9;
  idx := FindRecordInBucket(bucket, 123, rec.key_data, 3);
  if idx <> -1 then
  begin
    WriteLn('Ошибка: найдено несуществующее значение');
    Halt(1);
  end;
  // Добавление дубликата
  rec.key_data[0] := 1;
  if AddRecordToBucket(bucket, bucket^.records[0], max_records) then
  begin
    WriteLn('Ошибка: добавлен дубликат');
    Halt(1);
  end;
  // Добавление второй записи
  rec.key_hash := 124;
  rec.key_data[0] := 4; rec.key_data[1] := 5; rec.key_data[2] := 6;
  if not AddRecordToBucket(bucket, rec, max_records) then
  begin
    WriteLn('Ошибка: не удалось добавить вторую запись');
    Halt(1);
  end;
  // Переполнение
  rec.key_hash := 125;
  if AddRecordToBucket(bucket, rec, max_records) then
  begin
    WriteLn('Ошибка: добавлена запись при переполнении');
    Halt(1);
  end;
  WriteLn('Find/AddRecordToBucket: OK');
  Dispose(bucket);
end;

procedure TestDeleteRecordInBucket;
var
  bucket: PBucket;
  rec: TBucketRecord;
  idx: Integer;
  max_records: Integer;
begin
  New(bucket);
  bucket^ := InitBucket(1);
  max_records := 2;
  // Добавляем две записи
  rec.key_hash := 111;
  rec.key_size := 2;
  rec.key_data[0] := 10; rec.key_data[1] := 20;
  rec.data_offset := 100;
  rec.data_size := 10;
  rec.deleted := 0;
  AddRecordToBucket(bucket, rec, max_records);
  rec.key_hash := 112;
  rec.key_data[0] := 30; rec.key_data[1] := 40;
  AddRecordToBucket(bucket, rec, max_records);
  // Удаляем первую
  if not DeleteRecordInBucket(bucket, 111, [10,20], 2) then
  begin
    WriteLn('Ошибка: не удалось удалить существующую запись');
    Halt(1);
  end;
  // Проверяем, что она не находится
  idx := FindRecordInBucket(bucket, 111, [10,20], 2);
  if idx <> -1 then
  begin
    WriteLn('Ошибка: удалённая запись всё ещё находится');
    Halt(1);
  end;
  // Добавляем новую запись (на место удалённой)
  rec.key_hash := 113;
  rec.key_data[0] := 50; rec.key_data[1] := 60;
  rec.deleted := 0;
  if not AddRecordToBucket(bucket, rec, max_records) then
  begin
    WriteLn('Ошибка: не удалось добавить запись после удаления');
    Halt(1);
  end;
  WriteLn('DeleteRecordInBucket: OK');
  Dispose(bucket);
end;

procedure TestSplitBucketAndGrowDirectory;
var
  bucket, new_bucket: PBucket;
  rec: TBucketRecord;
  i, idx: Integer;
  res: TSplitResult;
  dir: TDirectory;
  max_records: Integer;
begin
  max_records := 4;
  New(bucket);
  bucket^ := InitBucket(2); // local_depth=2
  // Ключи: 0b00, 0b01, 0b10, 0b11 (по 2 младших бита)
  for i := 0 to 3 do
  begin
    rec.key_hash := i;
    rec.key_size := 1;
    rec.key_data[0] := i;
    rec.data_offset := 100 + i;
    rec.data_size := 10;
    rec.deleted := 0;
    if not AddRecordToBucket(bucket, rec, max_records) then
    begin
      WriteLn('Ошибка: не удалось добавить запись ', i);
      Halt(1);
    end;
  end;
  // Split по local_depth=3
  res := SplitBucket(bucket, 3, max_records);
  new_bucket := res.new_bucket;
  // Проверяем распределение: mask=0b100, key_hash=4,5,6,7 должны попасть в new_bucket, остальные остаться
  for i := 0 to 3 do
  begin
    idx := FindRecordInBucket(bucket, i, [i], 1);
    if ((i and 4) = 0) and (idx = -1) then
    begin
      WriteLn('Ошибка: запись ', i, ' должна остаться в старом бакете');
      Halt(1);
    end;
    idx := FindRecordInBucket(new_bucket, i, [i], 1);
    if ((i and 4) <> 0) and (idx = -1) then
    begin
      WriteLn('Ошибка: запись ', i, ' должна быть в новом бакете');
      Halt(1);
    end;
  end;
  // Проверяем GrowDirectoryIfNeeded
  dir := InitDirectory(2); // global_depth=2, 4 бакета
  dir.bucket_offsets[0] := 100;
  dir.bucket_offsets[1] := 200;
  dir.bucket_offsets[2] := 300;
  dir.bucket_offsets[3] := 400;
  GrowDirectoryIfNeeded(dir, 3);
  if dir.global_depth <> 3 then
  begin
    WriteLn('Ошибка: global_depth не увеличился');
    Halt(1);
  end;
  if Length(dir.bucket_offsets) <> 8 then
  begin
    WriteLn('Ошибка: bucket_offsets не удвоился');
    Halt(1);
  end;
  for i := 0 to 3 do
    if (dir.bucket_offsets[i] <> dir.bucket_offsets[i+4]) then
    begin
      WriteLn('Ошибка: bucket_offsets копируются неверно');
      Halt(1);
    end;
  WriteLn('SplitBucket/GrowDirectory: OK');
  Dispose(bucket);
  Dispose(new_bucket);
end;

procedure TestInsertWithSplit;
var
  dir: TDirectory;
  buckets: TBucketPtrArray;
  rec: TBucketRecord;
  i, idx, bidx, max_records: Integer;
begin
  max_records := 2;
  dir := InitDirectory(1); // 2 бакета
  SetLength(buckets, 2);
  New(buckets[0]);
  buckets[0]^ := InitBucket(1);
  New(buckets[1]);
  buckets[1]^ := InitBucket(1);
  dir.bucket_offsets[0] := 0;
  dir.bucket_offsets[1] := 1;
  // Вставляем 6 разных ключей (split гарантирован)
  for i := 0 to 5 do
  begin
    rec.key_hash := i;
    rec.key_size := 1;
    rec.key_data[0] := i;
    rec.data_offset := 100 + i;
    rec.data_size := 10;
    rec.deleted := 0;
    if not InsertWithSplit(dir, buckets, rec, max_records) then
    begin
      WriteLn('Ошибка: InsertWithSplit не смог вставить ', i);
      Halt(1);
    end;
  end;
  // Проверяем, что все ключи найдены в своих бакетах
  for i := 0 to 5 do
  begin
    idx := DirectoryIndexByHash(i, dir.global_depth);
    bidx := dir.bucket_offsets[idx];
    if FindRecordInBucket(buckets[bidx], i, [i], 1) = -1 then
    begin
      WriteLn('Ошибка: ключ ', i, ' не найден после split');
      Halt(1);
    end;
  end;
  if dir.global_depth < 2 then
  begin
    WriteLn('Ошибка: global_depth не увеличился после split');
    Halt(1);
  end;
  WriteLn('InsertWithSplit: OK');
end;

procedure TestStorageTimestamp;
var
  storage: TStorage;
  key: TByteArray;
  value: TByteArray;
  ts1, ts2: QWord;
  i: Integer;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (timestamp)');
    Halt(1);
  end;
  SetLength(key, 1);
  SetLength(value, 1); value[0]:=1;
  // Вставляем первый ключ
  key[0] := 1;
  if not StorageSet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageSet (timestamp 1)');
    Halt(1);
  end;
  ts1 := storage.buckets[0]^.records[0].timestamp;
  Sleep(1100); // Ждём 1.1 секунды
  // Вставляем второй ключ
  key[0] := 2;
  if not StorageSet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageSet (timestamp 2)');
    Halt(1);
  end;
  // Ищем timestamp второго ключа
  ts2 := 0;
  for i := 0 to storage.buckets[0]^.num_records-1 do
    if storage.buckets[0]^.records[i].key_data[0]=2 then
      ts2 := storage.buckets[0]^.records[i].timestamp;
  if (ts2 <= ts1) or (ts2 = 0) then
  begin
    WriteLn('Ошибка: timestamp не увеличивается');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageTimestamp: OK');
end;

procedure TestStorageFindOldestKey;
var
  storage: TStorage;
  key, oldest: TByteArray;
  value: TByteArray;
  i: Integer;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (findoldest)');
    Halt(1);
  end;
  SetLength(key, 1);
  SetLength(value, 1); value[0]:=1;
  // Вставляем три ключа с задержкой
  for i := 1 to 3 do
  begin
    key[0] := i;
    if not StorageSet(storage, key, 1, value) then
    begin
      WriteLn('Ошибка: StorageSet (findoldest ', i, ')');
      Halt(1);
    end;
    Sleep(500); // 0.5 секунды между вставками
  end;
  // Проверяем, что найден самый первый ключ
  if not StorageFindOldestKey(storage, oldest) then
  begin
    WriteLn('Ошибка: StorageFindOldestKey не нашёл ключ');
    Halt(1);
  end;
  if (Length(oldest) <> 1) or (oldest[0] <> 1) then
  begin
    WriteLn('Ошибка: StorageFindOldestKey вернул неверный ключ');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageFindOldestKey: OK');
end;

procedure TestStorageDeleteOldest;
var
  storage: TStorage;
  key, oldest: TByteArray;
  value: TByteArray;
  i: Integer;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (deleteoldest)');
    Halt(1);
  end;
  SetLength(key, 1);
  SetLength(value, 1); value[0]:=1;
  // Вставляем три ключа с задержкой
  for i := 1 to 3 do
  begin
    key[0] := i;
    if not StorageSet(storage, key, 1, value) then
    begin
      WriteLn('Ошибка: StorageSet (deleteoldest ', i, ')');
      Halt(1);
    end;
    Sleep(500); // 0.5 секунды между вставками
  end;
  // Удаляем старейший ключ
  if not StorageDeleteOldest(storage) then
  begin
    WriteLn('Ошибка: StorageDeleteOldest не удалил ключ');
    Halt(1);
  end;
  // Проверяем, что ключ 1 отсутствует, а 2 и 3 есть
  key[0] := 1;
  if StorageGet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageDeleteOldest не удалил старейший ключ');
    Halt(1);
  end;
  key[0] := 2;
  if not StorageGet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageDeleteOldest удалил не тот ключ (2)');
    Halt(1);
  end;
  key[0] := 3;
  if not StorageGet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageDeleteOldest удалил не тот ключ (3)');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageDeleteOldest: OK');
end;

procedure TestStorageCompactData;
var
  storage: TStorage;
  key: TByteArray;
  value: TByteArray;
  size_before, size_after: QWord;
  i: Integer;
begin
  DeleteFile('test.idx');
  DeleteFile('test.idx.bucket0');
  DeleteFile('test.dat');
  if not StorageOpen(storage, 'test.idx', 'test.dat') then
  begin
    WriteLn('Ошибка: StorageOpen (compact)');
    Halt(1);
  end;
  SetLength(key, 1);
  SetLength(value, 10); for i := 0 to 9 do value[i] := i;
  // Вставляем три ключа
  for i := 1 to 3 do
  begin
    key[0] := i;
    if not StorageSet(storage, key, 1, value) then
    begin
      WriteLn('Ошибка: StorageSet (compact ', i, ')');
      Halt(1);
    end;
  end;
  // Удаляем второй ключ
  key[0] := 2;
  if not StorageDelete(storage, key, 1) then
  begin
    WriteLn('Ошибка: StorageDelete (compact)');
    Halt(1);
  end;
  size_before := GetFileSizeByName('test.dat');
  StorageCompactData(storage);
  size_after := GetFileSizeByName('test.dat');
  if size_after >= size_before then
  begin
    WriteLn('Ошибка: StorageCompactData не уменьшил размер .dat');
    Halt(1);
  end;
  // Проверяем, что ключи 1 и 3 доступны, а 2 — нет
  key[0] := 1;
  if not StorageGet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageCompactData потерял ключ 1');
    Halt(1);
  end;
  key[0] := 2;
  if StorageGet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageCompactData не удалил ключ 2');
    Halt(1);
  end;
  key[0] := 3;
  if not StorageGet(storage, key, 1, value) then
  begin
    WriteLn('Ошибка: StorageCompactData потерял ключ 3');
    Halt(1);
  end;
  StorageClose(storage);
  WriteLn('StorageCompactData: OK');
end;

begin
  TestStorageOpenClose;
  TestStoragePersistence;
  TestStorageGet;
  TestStorageSplitGrow;
  TestStorageDelete;
  TestStorageList;
  TestInitDirectory;
  TestInitBucket;
  TestDirectorySerialization;
  TestBucketSerialization;
  TestDirectoryIndexByHash;
  TestFindAndAddRecordToBucket;
  TestDeleteRecordInBucket;
  TestSplitBucketAndGrowDirectory;
  TestInsertWithSplit;
  TestStorageTimestamp;
  TestStorageFindOldestKey;
  TestStorageDeleteOldest;
  TestStorageCompactData;
  WriteLn('Все тесты пройдены.');
end. 