# RLT тестовое
Тестовое задание
## Краткое описание
Backend для telegram бота с поддержкой:
- Redis для кэширования и очередей задач
- Telegram bot
## Установка и запуск

### Требования
- Docker и Docker Compose
- Git
- Python 3.10+ (для локальной разработки)
- PostgreSQL 14+
- Redis 7+
### Установка

1. Клонируйте репозиторий:
```bash
git clone <repository_url>
cd rlt-test-bot
```

2. Создайте файл `.env`:
```bash
cp env.example .env
```

3. Отредактируйте файл `.env` и заполните все необходимые значения