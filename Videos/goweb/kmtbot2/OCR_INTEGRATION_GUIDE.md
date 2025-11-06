# Gemini Vision OCR Integration Guide

Полное руководство по интеграции OCR распознавания расписаний в kmtbot.

## 📋 Обзор изменений

Данная интеграция добавляет автоматическое распознавание изображений расписаний через Gemini Vision API и сохранение структурированных данных в PostgreSQL.

### Ключевые возможности:
- ✅ Автоматическое OCR распознавание новых расписаний
- ✅ Извлечение групп, предметов, преподавателей и аудиторий
- ✅ Оценка уверенности распознавания (confidence score)
- ✅ Логирование всех OCR-операций
- ✅ Готовность к гибридной рассылке (текст/фото)

---

## 🔧 Шаг 1: Обновление импортов

В начале `NORM.go` добавьте импорт `encoding/base64`:

```go
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"  // <-- ДОБАВИТЬ
	"encoding/json"
	"errors"
	"fmt"
	"io"
	// ... остальные импорты
)
```

---

## 📝 Шаг 2: Добавление константы промпта OCR

После константы `broadcastBatchSize` добавьте:

```go
const (
	// ... существующие константы
	
	// Промпт для OCR расписания
	geminiSchedulePrompt = `Ты — помощник для извлечения расписания занятий из таблицы.

СТРУКТУРА ИЗОБРАЖЕНИЯ:
- Таблица с расписанием техникума на один день
- Столбцы: Группы | 1 пара | 2 пара | 3 пара | 4 пара | 5 пара | 6 пара
- В столбце "Группы": коды групп (формат XX-XXX-X, например: 25-СА-1, 24-ТМ-1)
- В ячейках пар: дисциплина, аудитория "Каб.XXX", ФИО преподавателя

ЗАДАЧА:
Извлеки ВСЕ занятия и верни JSON в точном формате ниже.

ФОРМАТ ОТВЕТА:
{
  "schedule": [
    {
      "group": "25-СА-1",
      "pair_number": 1,
      "subject": "Математика",
      "teacher": "Курсанова Е.В.",
      "room": "Каб.303",
      "confidence": 0.95
    }
  ],
  "metadata": {
    "date": "07.11.2025",
    "corpus": "A",
    "total_entries": 120,
    "average_confidence": 0.87
  }
}

ПРАВИЛА:
1. group: Код группы из столбца "Группы"
2. pair_number: Номер пары от 1 до 6
3. subject: Название дисциплины БЕЗ аудитории и преподавателя
4. teacher: ФИО преподавателя (формат "Фамилия И.О."), если нет — null
5. room: Аудитория в формате "Каб.XXX", если нет — null
6. confidence: Уверенность от 0.0 до 1.0 (0.9-1.0: чёткий текст, 0.7-0.9: небольшие сомнения, 0.5-0.7: размыт, <0.5: ненадёжно)
7. Пустые ячейки ("-" или пробелы) — НЕ добавляй
8. Если ячейка содержит несколько строк, объедини их
9. metadata.date: дата из заголовка таблицы
10. metadata.corpus: "A" или "B"

ЕСЛИ ТАБЛИЦА НЕЧИТАЕМА:
{"schedule": [], "metadata": {"error": "Не удалось распознать расписание"}}

Верни ТОЛЬКО JSON, без комментариев.`
)
```

---

## 🏗️ Шаг 3: Обновление структур

### 3.1 Обновить GPart для поддержки изображений

Заменить существующую структуру `GPart`:

```go
// БЫЛО:
type GPart struct {
	Text string `json:"text,omitempty"`
}

// СТАЛО:
type InlineData struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"` // base64
}

type GPart struct {
	Text       string      `json:"text,omitempty"`
	InlineData *InlineData `json:"inline_data,omitempty"`
}
```

### 3.2 Добавить структуры для OCR ответов

После структур `GResponse` добавить:

```go
// Gemini OCR structures
type GeminiScheduleResponse struct {
	Schedule []ScheduleEntry  `json:"schedule"`
	Metadata ScheduleMetadata `json:"metadata"`
}

type ScheduleEntry struct {
	Group      string   `json:"group"`
	PairNumber int      `json:"pair_number"`
	Subject    string   `json:"subject"`
	Teacher    *string  `json:"teacher"`
	Room       *string  `json:"room"`
	Confidence float64  `json:"confidence"`
}

type ScheduleMetadata struct {
	Date              string  `json:"date"`
	Corpus            string  `json:"corpus"`
	TotalEntries      int     `json:"total_entries"`
	AverageConfidence float64 `json:"average_confidence"`
	Error             string  `json:"error,omitempty"`
}
```

---

## 💾 Шаг 4: Добавление таблиц БД

После функции `ensureUsersTable` добавить:

```go
func ensureScheduleParsedTable(ctx context.Context) error {
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS schedule_parsed (
		id SERIAL PRIMARY KEY,
		
		-- Мета-информация
		image_url TEXT NOT NULL,
		date DATE NOT NULL,
		corpus TEXT NOT NULL CHECK (corpus IN ('A', 'B')),
		
		-- Данные занятия
		group_name TEXT NOT NULL,
		pair_number INTEGER CHECK (pair_number BETWEEN 1 AND 6),
		subject TEXT NOT NULL,
		teacher TEXT,
		room TEXT,
		
		-- Качество распознавания
		confidence_score FLOAT DEFAULT 0.0 CHECK (confidence_score BETWEEN 0 AND 1),
		status TEXT DEFAULT 'pending' CHECK (status IN ('verified', 'partial', 'needs_review', 'failed')),
		
		-- Логи
		raw_json JSONB,
		error_message TEXT,
		created_at TIMESTAMPTZ DEFAULT now(),
		updated_at TIMESTAMPTZ DEFAULT now(),
		
		CONSTRAINT unique_schedule_entry UNIQUE (date, corpus, group_name, pair_number)
	);
	
	CREATE INDEX IF NOT EXISTS idx_schedule_date_group ON schedule_parsed(date, group_name);
	CREATE INDEX IF NOT EXISTS idx_schedule_corpus ON schedule_parsed(corpus, date);
	CREATE INDEX IF NOT EXISTS idx_schedule_status ON schedule_parsed(status);
	`)
	if err != nil {
		return fmt.Errorf("создание таблицы schedule_parsed: %w", err)
	}
	
	_, err = db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS ocr_logs (
		id SERIAL PRIMARY KEY,
		image_url TEXT NOT NULL,
		date DATE,
		corpus TEXT,
		status TEXT,
		confidence_avg FLOAT,
		records_extracted INTEGER,
		error_message TEXT,
		processing_time_ms INTEGER,
		created_at TIMESTAMPTZ DEFAULT now()
	);
	`)
	return err
}
```

### 4.1 Обновить main()

В функции `main()` после `ensureScheduleTable` добавить:

```go
if err := ensureScheduleParsedTable(ctx); err != nil {
	log.Fatalf("ensureScheduleParsedTable: %v", err)
}
```

---

## 🤖 Шаг 5: Функции OCR

В конец файла `NORM.go` (перед `scrapeImages()`) добавить следующие функции:

### 5.1 callGeminiWithRequest

```go
// Универсальная функция для вызова Gemini API с поддержкой изображений
func callGeminiWithRequest(reqBody GRequest) (string, error) {
	key := os.Getenv("GEMINI_API_KEY")
	if key == "" {
		return "", fmt.Errorf("GEMINI_API_KEY не задан")
	}

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	
	select {
	case <-geminiLimiter:
		log.Printf("Получен токен для Gemini API")
	case <-timeout.C:
		return "", fmt.Errorf("превышен лимит запросов к Gemini API")
	}

	b, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", geminiURL, bytes.NewBuffer(b))
	if err != nil { return "", err }
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", key)

	client := &http.Client{Timeout: 90 * time.Second} // Увеличено для OCR
	resp, err := client.Do(req)
	if err != nil { return "", err }
	defer resp.Body.Close()
	
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Gemini API %d: %s", resp.StatusCode, string(body))
	}
	
	var gr GResponse
	if err := json.Unmarshal(body, &gr); err != nil { return "", err }
	if gr.Error != nil { return "", fmt.Errorf("Gemini error: %s", gr.Error.Message) }
	if len(gr.Candidates) == 0 || len(gr.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("Gemini вернул пустой ответ")
	}
	
	return gr.Candidates[0].Content.Parts[0].Text, nil
}
```

### 5.2 processScheduleOCR

```go
func processScheduleOCR(item *ScheduleItem, corpus string) error {
	startTime := time.Now()
	
	// 1. Скачать изображение
	resp, err := http.Get(item.URL)
	if err != nil {
		logOCRError(item.URL, "download_failed", err)
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()
	
	imageBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logOCRError(item.URL, "read_failed", err)
		return fmt.Errorf("read failed: %w", err)
	}
	
	// 2. Кодировать в base64
	imageB64 := base64.StdEncoding.EncodeToString(imageBytes)
	
	// 3. Вызвать Gemini Vision API
	reqBody := GRequest{
		SystemInstruction: &GContent{Parts: []GPart{{Text: "Отвечай только валидным JSON, без комментариев."}}},
		Contents: []GContent{{
			Parts: []GPart{
				{InlineData: &InlineData{MimeType: "image/jpeg", Data: imageB64}},
				{Text: geminiSchedulePrompt},
			},
		}},
	}
	
	jsonResponse, err := callGeminiWithRequest(reqBody)
	if err != nil {
		logOCRError(item.URL, "gemini_failed", err)
		return fmt.Errorf("gemini error: %w", err)
	}
	
	// 4. Парсинг JSON
	var geminiResp GeminiScheduleResponse
	if err := json.Unmarshal([]byte(jsonResponse), &geminiResp); err != nil {
		logOCRError(item.URL, "json_parse_failed", err)
		return fmt.Errorf("json parse error: %w", err)
	}
	
	// 5. Проверка на ошибку
	if geminiResp.Metadata.Error != "" {
		log.Printf("Gemini не смог распознать %s: %s", item.URL, geminiResp.Metadata.Error)
		logOCRFailure(item.URL, item.Date, corpus, 0, 0.0, time.Since(startTime))
		return fmt.Errorf("recognition failed: %s", geminiResp.Metadata.Error)
	}
	
	// 6. Вставка в БД
	ctx := context.Background()
	successCount := 0
	
	for _, entry := range geminiResp.Schedule {
		// Валидация
		if entry.Group == "" || entry.Subject == "" || entry.PairNumber < 1 || entry.PairNumber > 6 {
			log.Printf("Пропускаем невалидную запись: %+v", entry)
			continue
		}
		
		// Определение статуса
		status := "verified"
		if entry.Confidence < 0.6 {
			status = "needs_review"
		} else if entry.Confidence < 0.8 {
			status = "partial"
		}
		
		// Вставка
		_, err := db.Exec(ctx, `
			INSERT INTO schedule_parsed 
				(image_url, date, corpus, group_name, pair_number, subject, teacher, room, 
				 confidence_score, status, raw_json, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
			ON CONFLICT (date, corpus, group_name, pair_number) 
			DO UPDATE SET 
				subject = EXCLUDED.subject,
				teacher = EXCLUDED.teacher,
				room = EXCLUDED.room,
				confidence_score = EXCLUDED.confidence_score,
				status = EXCLUDED.status,
				raw_json = EXCLUDED.raw_json,
				updated_at = now()
		`, item.URL, item.Date, corpus, entry.Group, entry.PairNumber, 
		   entry.Subject, entry.Teacher, entry.Room, entry.Confidence, status, jsonResponse)
		
		if err != nil {
			log.Printf("Ошибка вставки в БД: %v", err)
			continue
		}
		successCount++
	}
	
	// 7. Логирование
	logOCRSuccess(item.URL, item.Date, corpus, successCount, geminiResp.Metadata.AverageConfidence, time.Since(startTime))
	
	log.Printf("OCR завершён: %s → %d/%d записей, avg=%.2f, time=%v", 
		item.URL, successCount, len(geminiResp.Schedule), geminiResp.Metadata.AverageConfidence, time.Since(startTime))
	
	return nil
}
```

### 5.3 Функции логирования

```go
func logOCRSuccess(url string, date time.Time, corpus string, records int, confidence float64, duration time.Duration) {
	ctx := context.Background()
	_, _ = db.Exec(ctx, `
		INSERT INTO ocr_logs (image_url, date, corpus, status, confidence_avg, records_extracted, processing_time_ms)
		VALUES ($1, $2, $3, 'success', $4, $5, $6)
	`, url, date, corpus, confidence, records, duration.Milliseconds())
}

func logOCRFailure(url string, date time.Time, corpus string, records int, confidence float64, duration time.Duration) {
	ctx := context.Background()
	_, _ = db.Exec(ctx, `
		INSERT INTO ocr_logs (image_url, date, corpus, status, confidence_avg, records_extracted, processing_time_ms)
		VALUES ($1, $2, $3, 'failed', $4, $5, $6)
	`, url, date, corpus, confidence, records, duration.Milliseconds())
}

func logOCRError(url string, errorType string, err error) {
	ctx := context.Background()
	_, _ = db.Exec(ctx, `
		INSERT INTO ocr_logs (image_url, status, error_message)
		VALUES ($1, $2, $3)
	`, url, "failed", fmt.Sprintf("%s: %v", errorType, err))
}
```

---

## 🔗 Шаг 6: Интеграция в scrapeImages()

В функции `scrapeImages()` после строки `wgUpload.Wait()` добавить:

```go
	wgUpload.Wait()

	log.Printf("Загрузка новых изображений завершена за %v", time.Since(uploadStart))

	// === НАЧАЛО БЛОКА OCR ===
	log.Println("Начинаем OCR для новых расписаний...")
	ocrStart := time.Now()

	// Ограничиваем количество одновременных OCR-запросов
	ocrSemaphore := make(chan struct{}, 2) // Максимум 2 параллельных OCR
	var wgOCR sync.WaitGroup

	// Обрабатываем OCR для новых изображений корпуса А
	for _, item := range newItemsA {
		wgOCR.Add(1)
		go func(it *ScheduleItem) {
			defer wgOCR.Done()
			ocrSemaphore <- struct{}{}
			defer func() { <-ocrSemaphore }()
			
			if err := processScheduleOCR(it, "A"); err != nil {
				log.Printf("Ошибка OCR для %s: %v", it.URL, err)
			}
		}(item)
	}

	// Обрабатываем OCR для новых изображений корпуса Б
	for _, item := range newItemsB {
		wgOCR.Add(1)
		go func(it *ScheduleItem) {
			defer wgOCR.Done()
			ocrSemaphore <- struct{}{}
			defer func() { <-ocrSemaphore }()
			
			if err := processScheduleOCR(it, "B"); err != nil {
				log.Printf("Ошибка OCR для %s: %v", it.URL, err)
			}
		}(item)
	}

	wgOCR.Wait()
	log.Printf("OCR завершён для %d изображений за %v", len(newItemsA)+len(newItemsB), time.Since(ocrStart))
	// === КОНЕЦ БЛОКА OCR ===

	mu.Lock()
	// ... остальной код продолжается
```

---

## ✅ Шаг 7: Тестирование

После деплоя выполните SQL-запросы для проверки:

```sql
-- Количество распознанных записей
SELECT COUNT(*) FROM schedule_parsed;

-- Топ-5 групп по количеству пар
SELECT group_name, COUNT(*) as pairs 
FROM schedule_parsed 
GROUP BY group_name 
ORDER BY pairs DESC 
LIMIT 5;

-- Средний confidence по корпусам
SELECT corpus, AVG(confidence_score) as avg_conf, COUNT(*) as total
FROM schedule_parsed 
GROUP BY corpus;

-- Последние логи OCR
SELECT * FROM ocr_logs 
ORDER BY created_at DESC 
LIMIT 10;

-- Проблемные записи (низкий confidence)
SELECT * FROM schedule_parsed 
WHERE status = 'needs_review' 
ORDER BY confidence_score ASC 
LIMIT 20;
```

---

## 🚀 Деплой

После внесения всех изменений:

```bash
git add NORM.go OCR_INTEGRATION_GUIDE.md
git commit -m "Complete Gemini Vision OCR integration"
git push origin feature/gemini-ocr-schedule
```

Затем выполните деплой на Render и проверьте логи для подтверждения создания таблиц БД.

---

## 📊 Мониторинг

Добавьте админ-команду `/ocrtest` для проверки статистики OCR (опционально).

---

## ⚠️ Важные замечания

1. **Лимиты API**: Gemini API имеет лимит 9 запросов в минуту. Установлен семафор на 2 параллельных OCR-запроса.

2. **Память**: OCR-обработка загружает изображения в память. Большие изображения (>5MB) могут потребовать увеличения лимитов памяти на Render.

3. **База данных**: Убедитесь, что `DATABASE_URL` настроен корректно в переменных окружения.

4. **Тестирование**: Рекомендуется сначала протестировать на 1-2 расписаниях перед массовой обработкой.

---

## 📝 Контрольный список

- [ ] Добавлен импорт `encoding/base64`
- [ ] Добавлена константа `geminiSchedulePrompt`
- [ ] Обновлены структуры `GPart` и добавлены `InlineData`
- [ ] Добавлены OCR-структуры (`GeminiScheduleResponse`, и т.д.)
- [ ] Добавлена функция `ensureScheduleParsedTable()`
- [ ] Обновлена `main()` с вызовом `ensureScheduleParsedTable()`
- [ ] Добавлены все OCR-функции
- [ ] Интегрирован блок OCR в `scrapeImages()`
- [ ] Проверены и запушены изменения
- [ ] Выполнен деплой на Render
- [ ] Протестированы SQL-запросы

---

**Статус**: ✅ Готово к интеграции
**Версия**: 1.0
**Дата**: 06.11.2025
