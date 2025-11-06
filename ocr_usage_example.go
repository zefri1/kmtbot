// Пример интеграции OCR в workflow скрапинга расписания
// Добавить в файл ocr_usage_example.go
package main

import (
    "log"
)

func afterScheduleUploadWorkflow(newItemsA, newItemsB map[string]*ScheduleItem) {
    // ... другие этапы обработки расписания ...
    // После загрузки изображений:
    runOcrOnNewSchedules(newItemsA, newItemsB)
    log.Println("OCR обработка новых расписаний завершена.")
}

// Используйте afterScheduleUploadWorkflow после всех загрузок изображений, чтобы запустить OCR автоматически.
