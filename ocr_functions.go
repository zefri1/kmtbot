// Функции для обработки Gemini OCR
// Добавить в ocr_functions.go
package main

import (
    "context"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "time"
)

// Универсальный вызов Gemini с поддержкой изображений
func callGeminiWithRequest(reqBody GRequest) (string, error) {
    key := getenv("GEMINI_API_KEY")
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
    client := &http.Client{Timeout: 90 * time.Second}
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

// OCR обработка изображения и запись в БД
func processScheduleOCR(item *ScheduleItem, corpus string) error {
    startTime := time.Now()
    // Скачать изображение в память
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
    // Base64
    imageB64 := base64.StdEncoding.EncodeToString(imageBytes)
    // Промпт
    promptText := geminiSchedulePrompt
    reqBody := GRequest{
        SystemInstruction: &GContent{Parts: []GPart{{Text: "Отвечай только валидным JSON, без комментариев."}}},
        Contents: []GContent{{
            Parts: []GPart{{InlineData: &InlineData{MimeType: "image/jpeg", Data: imageB64}}, {Text: promptText}},
        }},
    }
    jsonResponse, err := callGeminiWithRequest(reqBody)
    if err != nil {
        logOCRError(item.URL, "gemini_failed", err)
        return fmt.Errorf("gemini error: %w", err)
    }
    var geminiResp GeminiScheduleResponse
    if err := json.Unmarshal([]byte(jsonResponse), &geminiResp); err != nil {
        logOCRError(item.URL, "json_parse_failed", err)
        return fmt.Errorf("json parse error: %w", err)
    }
    if geminiResp.Metadata.Error != "" {
        log.Printf("Gemini не смог распознать %s: %s", item.URL, geminiResp.Metadata.Error)
        logOCRFailure(item.URL, item.Date, corpus, 0, 0.0, time.Since(startTime))
        return fmt.Errorf("recognition failed: %s", geminiResp.Metadata.Error)
    }
    ctx := context.Background()
    successCount := 0
    for _, entry := range geminiResp.Schedule {
        if entry.Group == "" || entry.Subject == "" || entry.PairNumber < 1 || entry.PairNumber > 6 {
            log.Printf("Пропускаем невалидную запись: %+v", entry)
            continue
        }
        status := "verified"
        if entry.Confidence < 0.6 {
            status = "needs_review"
        } else if entry.Confidence < 0.8 {
            status = "partial"
        }
        _, err := db.Exec(ctx, `
            INSERT INTO schedule_parsed (image_url, date, corpus, group_name, pair_number, subject, teacher, room, confidence_score, status, raw_json, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
            ON CONFLICT (date, corpus, group_name, pair_number) DO UPDATE SET subject = EXCLUDED.subject, teacher = EXCLUDED.teacher, room = EXCLUDED.room, confidence_score = EXCLUDED.confidence_score, status = EXCLUDED.status, raw_json = EXCLUDED.raw_json, updated_at = now()
        `, item.URL, item.Date, corpus, entry.Group, entry.PairNumber, entry.Subject, entry.Teacher, entry.Room, entry.Confidence, status, jsonResponse)
        if err != nil {
            log.Printf("Ошибка вставки в БД: %v", err)
            continue
        }
        successCount++
    }
    logOCRSuccess(item.URL, item.Date, corpus, successCount, geminiResp.Metadata.AverageConfidence, time.Since(startTime))
    log.Printf("OCR завершён: %s → %d/%d записей, avg=%.2f, time=%v", item.URL, successCount, len(geminiResp.Schedule), geminiResp.Metadata.AverageConfidence, time.Since(startTime))
    return nil
}

func logOCRSuccess(url string, date time.Time, corpus string, records int, confidence float64, duration time.Duration) {
    ctx := context.Background()
    _, _ = db.Exec(ctx, `INSERT INTO ocr_logs (image_url, date, corpus, status, confidence_avg, records_extracted, processing_time_ms) VALUES ($1, $2, $3, 'success', $4, $5, $6)`, url, date, corpus, confidence, records, duration.Milliseconds())
}

func logOCRFailure(url string, date time.Time, corpus string, records int, confidence float64, duration time.Duration) {
    ctx := context.Background()
    _, _ = db.Exec(ctx, `INSERT INTO ocr_logs (image_url, date, corpus, status, confidence_avg, records_extracted, processing_time_ms) VALUES ($1, $2, $3, 'failed', $4, $5, $6)`, url, date, corpus, confidence, records, duration.Milliseconds())
}

func logOCRError(url string, errorType string, err error) {
    ctx := context.Background()
    _, _ = db.Exec(ctx, `INSERT INTO ocr_logs (image_url, status, error_message) VALUES ($1, $2, $3)`, url, "failed", fmt.Sprintf("%s: %v", errorType, err))
}
