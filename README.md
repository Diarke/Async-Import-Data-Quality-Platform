# Async File Import & Data Quality Platform

## Обзор

Распределённая система обработки файлов в реальном времени с:
- **Потоковой загрузкой** файлов без полной загрузки в памяти
- **Автоматической нормализацией** данных (телефоны, даты, ИНН)
- **Асинхронной обработкой** через RabbitMQ
- **Идемпотентной доставкой** сообщений
- **Надёжной сборкой результатов** с Redis буферизацией

## Архитектура

```
┌─────────────────┐     HTTP        ┌──────────────────┐
│     Client      │─────────────────▶│ File-Import-Svc │
└─────────────────┘                 └────────┬─────────┘
                                             │
                                      Publish chunks
                                             │
                                    ▼────────────────┐
                                  ┌──────────────────┴──┐
                                  │     RabbitMQ        │
                                  │  file_chunks queue  │
                                  └─────────┬───────────┘
                                            │
                                    Consume & process
                                            │
                                  ▼─────────────────────┐
                                  │File-Conversion-Svc  │
                                  │  (Normalize data)   │
                                  └────────┬────────────┘
                                           │
                                   ublish normalized
                                           │
                                  ▼────────────────┐
                                  │    RabbitMQ    │
                                  │ processed queue│
                                  └────────┬───────┘
                                           │
                                   Consume & buffer
                                           │
                                  ▼────────────────┐
                                  │Result-Collector│
                                  │  (Assemble)    │
                                  └────────┬───────┘
                                           │
                                     Store result
                                           │
                                        Redis
                                      + Storage FS
```

## Компоненты

### 1. File-Import-Service (FastAPI)

**Ответственность:**
- Приём файлов через `POST /process-file`
- Потоковое разбиение на чанки
- Публикация в RabbitMQ
- Отслеживание статуса job

**API:**
```
POST /api/v1/jobs/process-file
  Request: multipart/form-data {file, client_id?}
  Response: 202 Accepted {job_id, status_url}

GET /api/v1/jobs/{job_id}
  Response: 200 OK {job_id, status, progress, ...}

GET /api/v1/jobs/{job_id}/download
  Response: 200 OK (CSV file) | 400/404
```

### 2. File-Conversion-Consumer (Async)

**Ответственность:**
- Слушание очереди `file_chunks`
- Парсинг (CSV/TSV/JSONL)
- Нормализация полей
- Публикация обработанных чанков

**Нормализация:**
- `Phone`: E.164 формат (+7...) с проверкой по странам
- `Date`: ISO 8601 (YYYY-MM-DD или YYYY-MM-DDTHH:MM:SSZ)
- `INN`: Очистка от спецсимволов, проверка длины (10/12)

**Надёжность:**
- At-least-once delivery (ack после успеха)
- Retry с экспоненциальной задержкой
- DLQ для постоянных ошибок

### 3. Result-Collector (Async)

**Ответственность:**
- Слушание очереди `file_chunks_processed`
- Буферизация в Redis
- Сборка чанков в порядке
- Запись в хранилище

## Установка и запуск

### Требования
- Docker & Docker Compose
- Python 3.11+ (для локальной разработки)
- RabbitMQ 3.12+
- Redis 7+

### Вариант 1: Docker Compose (рекомендуется)

```bash
# Клонировать репозиторий
git clone <repo>
cd Async-Import-Data-Quality-Platform

# Запустить все сервисы
docker-compose up -d

# Проверить логи
docker-compose logs -f

# Остановить
docker-compose down
```

**URL:**
- FastAPI: http://localhost:8000
- RabbitMQ Management: http://localhost:15672 (guest/guest)
- Redis: localhost:6379

### Вариант 2: Локальная разработка

```bash
# 1. Запустить RabbitMQ
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 2. Запустить Redis
docker run -d -p 6379:6379 redis:7

# 3. Установить зависимости
cd file-import-service
pip install -r requirements.txt

# 4. Создать .env файл
cp .env.example .env

# 5. Запустить сервис
uvicorn src.main:app --reload

# 6. В других терминалах:
# Consumer
cd file-conversion-consumer
pip install -r requirements.txt
python main.py

# Collector
cd result-collector
pip install -r requirements.txt
python main.py
```

## Тестирование

### Unit Tests
```bash
# Тесты нормализации
cd file-conversion-consumer
pytest tests/test_normalizers.py -v

# Тесты API
cd file-import-service
pytest tests/test_normalizers.py -v
```

### Integration Tests
```bash
# Все интеграционные тесты
pytest tests/test_integration.py -v
```

### Manual Integration Test
```bash
# Требует запущенные сервисы

python -c "
import asyncio
import aiohttp
import io

async def test():
    # Create test CSV
    csv_data = b'name,phone,date\\nJohn,79991234567,2022-12-25\\n'
    
    async with aiohttp.ClientSession() as session:
        # Upload file
        with open('test.csv', 'wb') as f:
            f.write(csv_data)
        
        with open('test.csv', 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename='test.csv')
            
            async with session.post(
                'http://localhost:8000/api/v1/jobs/process-file',
                data=data
            ) as resp:
                result = await resp.json()
                job_id = result['job_id']
                print(f'Created job: {job_id}')
                
                # Poll status
                import time
                for _ in range(30):
                    time.sleep(1)
                    async with session.get(
                        f'http://localhost:8000/api/v1/jobs/{job_id}'
                    ) as status_resp:
                        status = await status_resp.json()
                        print(f'Status: {status[\"status\"]}')
                        if status['status'] == 'completed':
                            break

asyncio.run(test())
"
```

## Контракты сообщений RabbitMQ

### file_chunks (input)
```json
{
  "headers": {
    "job_id": "uuid",
    "chunk_index": 0,
    "total_chunks": 10,
    "filename": "data.csv",
    "original_format": "csv",
    "client_id": "optional",
    "attempt": 1
  },
  "body": "<binary chunk data>",
  "correlation_id": "job_id",
  "message_id": "job_id-chunk_index"
}
```

### file_chunks_processed (output)
```json
{
  "headers": {
    "job_id": "uuid",
    "chunk_index": 0,
    "total_chunks": 10,
    "filename": "data.csv",
    "status": "processed",
    "processed_rows": 100,
    "processing_errors": 0
  },
  "body": "<normalized CSV data>",
  "correlation_id": "job_id"
}
```

### file_chunks_dlq (Dead-Letter)
```json
{
  "headers": {
    "job_id": "uuid",
    "error": "Error message",
    "dlq_timestamp": "2024-01-01T12:00:00Z"
  },
  "body": "<original chunk data>"
}
```

## Конфигурация

### Переменные окружения

#### file-import-service
```
# App
DEBUG=true
TITLE=File Import Service
VERSION=1.0.0

# RabbitMQ
RMQ_HOST=localhost
RMQ_PORT=5672
RMQ_USER=guest
RMQ_PASS=guest

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Processing
CHUNK_SIZE=524288  # 512 KB
MAX_MESSAGE_SIZE=1048576  # 1 MB
DEFAULT_COUNTRY_CODE=KZ

# Storage
STORAGE_PATH=./storage/results
TEMP_PATH=./storage/temp
```

#### file-conversion-consumer
```
RMQ_HOST=localhost
RMQ_PORT=5672
RMQ_USER=guest
RMQ_PASS=guest

MAX_RETRIES=3
PREFETCH_COUNT=1
DEFAULT_COUNTRY_CODE=KZ
```

#### result-collector
```
RMQ_HOST=localhost
RMQ_PORT=5672
RMQ_USER=guest
RMQ_PASS=guest

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

STORAGE_PATH=./storage/results
```

## Примеры использования

### Запустить обработку файла
```bash
curl -X POST http://localhost:8000/api/v1/jobs/process-file \\
  -F "file=@data.csv" \\
  -F "client_id=user123"

# Response
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status_url": "/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000"
}
```

### Получить статус
```bash
curl http://localhost:8000/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000

# Response
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "filename": "data.csv",
  "status": "processing",
  "total_chunks": 5,
  "processed_chunks": 3,
  "failed_chunks": 0,
  "created_at": "2024-01-01T12:00:00",
  "updated_at": "2024-01-01T12:01:30",
  "result_url": null,
  "error": null
}
```

### Скачать результат
```bash
curl -O http://localhost:8000/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000/download

# После завершения статус будет "completed" и файл будет доступен
```

## Идемпотентность и надёжность

### Защита от дублирования
- `message_id = {job_id}-{chunk_index}` - уникален для каждого чанка
- При повторной доставке тот же `message_id` позволит не обработать дубликат

### Retry Policy
1. Транзиентные ошибки → `nack(requeue=True)` с экспоненциальной задержкой
2. После MAX_RETRIES попыток → отправить в DLQ
3. DLQ сохраняет сообщение с info об ошибке для анализа

### At-Least-Once Delivery
- Consumer ack только после успешной публикации результата
- Если crash до ack → RabbitMQ переотправит сообщение
- Идемпотентность гарантирует корректность при повторной доставке

## Мониторинг и метрики

### Логирование
Все компоненты логируют в stdout:
```
[2024-01-01T12:00:00] INFO: Processing chunk 0/10 for job 550e8400...
[2024-01-01T12:00:01] INFO: Chunk 0 processed and published successfully
[2024-01-01T12:00:30] INFO: All chunks received for job 550e8400, assembling result...
```

### RabbitMQ Management UI
- http://localhost:15672
- Просмотр очередей, сообщений, DLQ
- Анализ задержек и bottlenecks

### Redis CLI
```bash
# Проверить ключи job
redis-cli KEYS "job:550e8400*"

# Просмотреть метаданные
redis-cli GET "job:550e8400"

# Просмотреть обработанные чанки
redis-cli LRANGE "job:550e8400:processed_count" 0 -1
```

## Troubleshooting

### Consumer не обрабатывает сообщения
```bash
# 1. Проверить RabbitMQ подключение
docker-compose logs file-conversion-consumer

# 2. Проверить DLQ
docker-compose exec rabbitmq rabbitmqctl list_queues

# 3. Перезапустить consumer
docker-compose restart file-conversion-consumer
```

### Результаты не собираются
```bash
# 1. Проверить Redis
docker-compose exec redis redis-cli KEYS "job:*"

# 2. Проверить логи collector
docker-compose logs result-collector

# 3. Проверить хранилище
ls -la storage/results/
```

### Слишком медленная обработка
```bash
# Увеличить PREFETCH_COUNT в consumer
PREFETCH_COUNT=5

# Уменьшить CHUNK_SIZE
CHUNK_SIZE=262144

# Добавить больше реплик consumer
docker-compose up -d --scale file-conversion-consumer=3
```

## Performance Considerations

### Оптимизация под размер файла

| Размер файла | CHUNK_SIZE | Рекомендаций |
|---|---|---|
| < 1 MB | 256 KB | Быстро |
| 1-100 MB | 512 KB | Сбалансировано |
| 100-1000 MB | 1 MB | Экономит память |
| > 1 GB | 2 MB | Минимум overhead |

### Масштабирование
```bash
# Добавить более медленно процессоры
docker-compose up -d --scale file-conversion-consumer=5

# Добавить collector instances
docker-compose up -d --scale result-collector=3

# Увеличить RabbitMQ очередь
QUEUE_MAX_LENGTH=1000000
```

## Security Notes

⚠️ **Для production:**
- Использовать HTTPS/TLS для API
- Аутентификация и авторизация (JWT, OAuth)
- Ограничить размер загружаемый файлов
- Валидировать content-type файлов
- Использовать credentials vault для RabbitMQ/Redis
- Логировать доступ и операции
- Регулярно очищать DLQ и хранилище

## Лицензия

MIT

## Contributing

1. Fork репозиторий
2. Создать feature branch
3. Commit изменения
4. Push и создать Pull Request

## Контакты

Для вопросов и issues создавайте Issue на GitHub.
