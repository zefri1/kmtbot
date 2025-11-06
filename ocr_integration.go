// Интеграция OCR в обработку новых изображений расписания
// Добавить в файл ocr_integration.go
package main

import (
    "log"
    "sync"
    "time"
)

// Вызов OCR после загрузки новых изображений scrapeImages()
func runOcrOnNewSchedules(newItemsA, newItemsB map[string]*ScheduleItem) {
    log.Println("Начинаем OCR для новых расписаний...")
    ocrStart := time.Now()
    ocrSemaphore := make(chan struct{}, 2) // максимум 2 одновременных OCR
    var wgOCR sync.WaitGroup
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
}
