# PKeyDB

**PKeyDB** — легковесная сетевая СУБД (ключ-значение) с REST API, авторизацией, хранением данных на диске, конфигурируемостью через INI-файл, логированием в stdout/stderr, поддержкой нескольких БД с паролями и лимитами.

> ⚠️ **Внимание: проект находится в активной разработке и не готов для использования в продакшене!**

---

## Возможности
- REST API (CRUD-операции, авторизация)
- Хранение данных на диске (индекс + файл данных)
- Конфигурирование через INI-файл
- Логирование в stdout/stderr с меткой времени
- Простая система токенов (заголовок `X-Auth-Token`)
- Поддержка нескольких баз с лимитами

## Быстрый старт

1. **Сборка:**
   ```bash
   fpc main.pas
   ```
2. **Запуск:**
   ```bash
   ./main pkeydb.ini
   ```
3. **Тестирование API:**
   ```bash
   ./test_server.sh
   ```

## Пример запроса

```bash
curl -X POST http://localhost:8080/auth -H 'Content-Type: application/json' -d '{"dbname":"db1","password":"mypassword"}'
curl -X POST http://localhost:8080/db/db1/set -H 'X-Auth-Token: <token>' -H 'Content-Type: application/json' -d '{"key":"foo","value":"bar"}'
curl -X GET  http://localhost:8080/db/db1/get?key=foo -H 'X-Auth-Token: <token>'
```

## Структура проекта
- `main.pas` — точка входа, запуск сервера
- `server.pas` — HTTP сервер, обработка REST API
- `config.pas` — парсер INI-файла
- `test_server.pas` — интеграционные тесты
- `test_server.sh` — shell-скрипт для тестирования API
- `docs/` — документация (API, roadmap)

## Документация
- [docs/api.md](docs/api.md) — описание REST API, примеры запросов
- [docs/roadmap.md](docs/roadmap.md) — архитектура и план разработки

## Статус
- [x] Каркас сервера и API
- [x] Конфигурирование и логирование
- [x] Интеграционные тесты и shell-скрипт
- [ ] Реальное файловое хранилище (индекс + mmap)
- [ ] Безопасная авторизация и управление токенами
- [ ] Тестирование на реальной нагрузке
- [ ] Документация и примеры для production

---

**Проект развивается!**

Любые вопросы и предложения — через issues или pull requests. 