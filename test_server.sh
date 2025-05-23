#!/bin/bash

# Адрес сервера
SERVER_URL="http://127.0.0.1:8080"
DBNAME="test"
KEY="foo"
VALUE="bar"

# 1. Авторизация
echo "== AUTH =="
TOKEN=$(curl -s -X POST "$SERVER_URL/auth" -H 'Content-Type: application/json' -d '{"dbname":"'$DBNAME'","password":"any"}' | grep -o '"token":"[^"]*"' | cut -d '"' -f4)
echo "TOKEN: $TOKEN"

# 2. Запись значения
echo "== SET =="
curl -s -X POST "$SERVER_URL/db/$DBNAME/set" \
  -H "X-Auth-Token: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"key":"'$KEY'","value":"'$VALUE'"}'
echo

# 3. Получение значения
echo "== GET =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=$KEY" \
  -H "X-Auth-Token: $TOKEN"
echo

# 4. Список ключей
echo "== LIST =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/list" \
  -H "X-Auth-Token: $TOKEN"
echo

# 5. Удаление ключа
echo "== DELETE =="
curl -s -X DELETE "$SERVER_URL/db/$DBNAME/del?key=$KEY" \
  -H "X-Auth-Token: $TOKEN"
echo

# 6. Очистка базы
echo "== CLEAR =="
curl -s -X POST "$SERVER_URL/db/$DBNAME/clear" \
  -H "X-Auth-Token: $TOKEN"
echo

# 7. Ошибка: несуществующий ключ
echo "== GET NOT FOUND =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=notfound" \
  -H "X-Auth-Token: $TOKEN"
echo

# 8. Ошибка: без токена
echo "== GET WITHOUT TOKEN =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=$KEY"
echo 