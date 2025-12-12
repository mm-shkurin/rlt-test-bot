# RLT тестовое задание

Telegram-бот для аналитики по видео на основе задач на естественном языке.

## Описание

Бот принимает текстовые запросы на русском языке и возвращает числовые ответы на основе статистики по видео-креаторам. Данные хранятся в PostgreSQL и включают итоговую статистику по видео и почасовые снапшоты для отслеживания динамики.

## Требования

- Docker и Docker Compose
- Git
- Python 3.10+ (для локальной разработки)
- PostgreSQL 14+
- Redis 7+

## Установка и запуск

### Вариант 1: Запуск с Docker (рекомендуется)

1. Клонируйте репозиторий:
```bash
git clone <repository_url>
cd rlt-test-bot
```

2. Создайте файл `.env` в корне проекта:
```bash
cp env.example .env
```

3. Запустите контейнеры:
```bash
docker-compose up -d
```

5. Выполните миграции базы данных:
```bash
docker-compose exec app alembic upgrade head
```

6. Загрузите данные из JSON файла:
```bash
docker-compose exec app python scripts/load_data.py data/videos.json
```
