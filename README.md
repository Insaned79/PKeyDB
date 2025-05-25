# PKeyDB

[![License: BSD-2-Clause](https://img.shields.io/badge/License-BSD%202--Clause-blue.svg)](LICENSE)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/Insaned79/PKeyDB/CI?label=CI)](https://github.com/Insaned79/PKeyDB/actions)
[![GitHub stars](https://img.shields.io/github/stars/Insaned79/PKeyDB?style=social)](https://github.com/Insaned79/PKeyDB)

---

## English

**PKeyDB** is a lightweight network key-value database with REST API, authorization, disk storage, INI-based configuration, stdout/stderr logging, and support for multiple databases with passwords and limits.

> ⚠️ **Warning: This project is under active development and NOT ready for production use!**

### Features
- REST API (CRUD, authorization)
- Disk storage (index + data file)
- INI configuration
- Logging to stdout/stderr with timestamp
- Simple token system (`X-Auth-Token` header)
- Multiple databases with limits

### Quick start
1. **Build:**
   ```bash
   fpc main.pas
   ```
2. **Run:**
   ```bash
   ./main pkeydb.ini
   ```
3. **Test API:**
   ```bash
   ./test_server.sh
   ```

### Example request
```bash
curl -X POST http://localhost:8080/auth -H 'Content-Type: application/json' -d '{"dbname":"db1","password":"mypassword"}'
curl -X POST http://localhost:8080/db/db1/set -H 'X-Auth-Token: <token>' -H 'Content-Type: application/json' -d '{"key":"foo","value":"bar"}'
curl -X GET  http://localhost:8080/db/db1/get?key=foo -H 'X-Auth-Token: <token>'
```

### Project structure
- `main.pas` — entry point, server startup
- `server.pas` — HTTP server, REST API
- `config.pas` — INI parser
- `test_server.pas` — integration tests
- `test_server.sh` — shell script for API testing
- `docs/` — documentation (API, roadmap)

### Documentation
- [docs/api.md](docs/api.md) — REST API description, request examples
- [docs/roadmap.md](docs/roadmap.md) — architecture and roadmap
- `pkeydb.ini.example` — example config with comments

### Status
- [x] Server and API skeleton
- [x] Configuration and logging
- [x] Integration tests and shell script
- [ ] Real file storage (index + mmap)
- [x] Secure authorization and token management
- [ ] Real-world load testing
- [ ] Production-ready documentation and examples

### License
BSD 2-Clause. See [LICENSE](LICENSE).

---

# PKeyDB (на русском)

**PKeyDB** — легковесная сетевая СУБД (ключ-значение) с REST API, авторизацией, хранением данных на диске, конфигурируемостью через INI-файл, логированием в stdout/stderr, поддержкой нескольких БД с паролями и лимитами.

> ⚠️ **Внимание: проект находится в активной разработке и не готов для использования в продакшене!**

## Возможности
- REST API (CRUD-операции, авторизация)
- Хранение данных на диске (индекс + файл данных)
- Конфигурирование через INI-файл
- Логирование в stdout/stderr с меткой времени
- Простая система токенов (заголовок `X-Auth-Token`)
- Поддержка нескольких баз с лимитами

## Быстрый старт

1. Склонируйте репозиторий и зависимости.
2. Соберите проект через `make`.
3. Скопируйте пример конфига:
   ```bash
   cp pkeydb.ini.example pkeydb.ini
   ```
   и отредактируйте под свои нужды.

**ВАЖНО:** Ваш рабочий `pkeydb.ini` не коммитится в репозиторий. Для примера используйте `pkeydb.ini.example` (он всегда актуален и содержит комментарии по всем параметрам).

### Пример запроса

```bash
curl -X POST http://localhost:8080/auth -H 'Content-Type: application/json' -d '{"dbname":"db1","password":"mypassword"}'
curl -X POST http://localhost:8080/db/db1/set -H 'X-Auth-Token: <token>' -H 'Content-Type: application/json' -d '{"key":"foo","value":"bar"}'
curl -X GET  http://localhost:8080/db/db1/get?key=foo -H 'X-Auth-Token: <token>'
```

### Структура проекта
- `main.pas` — точка входа, запуск сервера
- `server.pas` — HTTP сервер, обработка REST API
- `config.pas` — парсер INI-файла
- `test_server.pas` — интеграционные тесты
- `test_server.sh` — shell-скрипт для тестирования API
- `docs/` — документация (API, roadmap)

### Документация
- [docs/api.md](docs/api.md) — описание REST API, примеры запросов
- [docs/roadmap.md](docs/roadmap.md) — архитектура и план разработки
- `pkeydb.ini.example` — пример конфига с комментариями

### Статус
- [x] Каркас сервера и API
- [x] Конфигурирование и логирование
- [x] Интеграционные тесты и shell-скрипт
- [ ] Реальное файловое хранилище (индекс + mmap)
- [x] Безопасная авторизация и управление токенами
- [ ] Тестирование на реальной нагрузке
- [ ] Документация и примеры для production

---

**Проект развивается!**

Любые вопросы и предложения — через issues или pull requests.

## Troubleshooting

- **Не удаётся авторизоваться:**
  - Проверьте, что в конфиге указан SHA256-хэш пароля, а не plain-текст.
  - Проверьте, что хэш в нижнем регистре, без пробелов.
  - Проверьте, что сервер перезапущен после изменения конфига. 