#!/bin/bash

# Адрес сервера
SERVER_URL="http://127.0.0.1:8080"
DBNAME="db1"
KEY="foo"
VALUE="bar"

# 1. Авторизация (верный пароль)
echo "== AUTH (valid password) =="
TOKEN=$(curl -s -X POST "$SERVER_URL/auth" -H 'Content-Type: application/json' -d '{"dbname":"'$DBNAME'","password":"secret1"}' | grep -o '"token":"[^"]*"' | cut -d '"' -f4)
echo "TOKEN: $TOKEN"

# 1.1 Авторизация (неверный пароль)
echo "== AUTH (invalid password) =="
RESP=$(curl -s -X POST "$SERVER_URL/auth" -H 'Content-Type: application/json' -d '{"dbname":"'$DBNAME'","password":"wrongpass"}')
echo "$RESP"

# 2. Запись значения
echo "== SET (valid token) =="
curl -s -X POST "$SERVER_URL/db/$DBNAME/set" \
  -H "X-Auth-Token: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"key":"'$KEY'","value":"'$VALUE'"}'
echo

# 2.1 Запись значения с токеном для другой БД
echo "== SET (token for another DB) =="
# Получаем токен для другой БД (db2)
TOKEN2=$(curl -s -X POST "$SERVER_URL/auth" -H 'Content-Type: application/json' -d '{"dbname":"db2","password":"secret2"}' | grep -o '"token":"[^"]*"' | cut -d '"' -f4)
curl -s -X POST "$SERVER_URL/db/$DBNAME/set" \
  -H "X-Auth-Token: $TOKEN2" \
  -H 'Content-Type: application/json' \
  -d '{"key":"'$KEY'","value":"'$VALUE'"}'
echo

# 2.2 Запись значения с некорректным токеном
echo "== SET (invalid token) =="
curl -s -X POST "$SERVER_URL/db/$DBNAME/set" \
  -H "X-Auth-Token: invalidtoken" \
  -H 'Content-Type: application/json' \
  -d '{"key":"'$KEY'","value":"'$VALUE'"}'
echo

# 3. Получение значения
echo "== GET (valid token) =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=$KEY" \
  -H "X-Auth-Token: $TOKEN"
echo

# 4. Список ключей
echo "== LIST (valid token) =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/list" \
  -H "X-Auth-Token: $TOKEN"
echo

# 5. Удаление ключа
echo "== DELETE (valid token) =="
curl -s -X DELETE "$SERVER_URL/db/$DBNAME/del?key=$KEY" \
  -H "X-Auth-Token: $TOKEN"
echo

# 6. Очистка базы
echo "== CLEAR (valid token) =="
curl -s -X POST "$SERVER_URL/db/$DBNAME/clear" \
  -H "X-Auth-Token: $TOKEN"
echo

# 7. Ошибка: несуществующий ключ
echo "== GET NOT FOUND (valid token) =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=notfound" \
  -H "X-Auth-Token: $TOKEN"
echo

# 8. Ошибка: без токена
echo "== GET WITHOUT TOKEN =="
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=$KEY"
echo

# 9. Ошибка: просроченный токен
echo "== GET (expired token) =="
# Генерируем токен с истёкшим сроком (jwt_expiry=0 через ручной запрос)
EXPIRED_TOKEN=$(curl -s -X POST "$SERVER_URL/auth" -H 'Content-Type: application/json' -d '{"dbname":"'$DBNAME'","password":"secret1"}' | grep -o '"token":"[^"]*"' | cut -d '"' -f4)
sleep 2 # Ждём 2 секунды, если сервер поддерживает exp=now
curl -s -X GET "$SERVER_URL/db/$DBNAME/get?key=$KEY" \
  -H "X-Auth-Token: $EXPIRED_TOKEN"
echo 