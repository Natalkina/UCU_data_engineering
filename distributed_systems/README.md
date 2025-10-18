# Distributed Systems - Replicated Log

## Iteration 2: Write Concern & Eventual Consistency

### Запуск системи

```bash
docker-compose up --build
```

### Тестування Write Concern

#### w=1 (тільки master)
```bash
curl -X POST -H "Content-Type: application/json" http://localhost:42000/messages -d '{"message": "msg1", "w": 1}'
```

#### w=2 (master + 1 secondary)
```bash
curl -X POST -H "Content-Type: application/json" http://localhost:42000/messages -d '{"message": "msg2", "w": 2}'
```

#### w=3 (master + 2 secondaries)
```bash
curl -X POST -H "Content-Type: application/json" http://localhost:42000/messages -d '{"message": "msg3", "w": 3}'
```

### Перевірка повідомлень

#### Master
```bash
curl http://localhost:42000/messages
```

#### Secondary 1 (з затримкою 10с)
```bash
curl http://localhost:42001/messages
```

#### Secondary 2 (без затримки)
```bash
curl http://localhost:42002/messages
```

### Тестування дедуплікації

```bash
# Відправити повідомлення з ID
curl -X POST -H "Content-Type: application/json" http://localhost:42000/messages -d '{"message": "test", "w": 1, "id": 100}'

# Відправити те ж повідомлення знову (буде проігноровано)
curl -X POST -H "Content-Type: application/json" http://localhost:42000/messages -d '{"message": "test", "w": 1, "id": 100}'
```

### Автоматичне тестування

```bash
python test_write_concern.py
```

### Архітектура

- **Master** (порт 42000): обробляє запити та чекає ACK згідно з write concern
- **Secondary1** (порт 42001): затримка 10 секунд для демонстрації eventual consistency
- **Secondary2** (порт 42002): без затримки

### Параметри Write Concern

- `w=1`: тільки master (найшвидше)
- `w=2`: master + 1 secondary
- `w=3`: master + 2 secondaries (найповільніше)

### Особливості

- **Дедуплікація**: повідомлення з однаковим ID ігноруються
- **Упорядкування**: повідомлення мають sequence numbers для консистентного порядку
- **Eventual Consistency**: secondary1 обробляє повідомлення з затримкою