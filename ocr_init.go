// Инициализация таблиц OCR для расписания
// Добавить в файл ocr_init.go
package main

import (
    "context"
    "fmt"
)

func ensureScheduleParsedTable(ctx context.Context) error {
    _, err := db.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS schedule_parsed (
        id SERIAL PRIMARY KEY,
        image_url TEXT NOT NULL,
        date DATE NOT NULL,
        corpus TEXT NOT NULL CHECK (corpus IN ('A', 'B')),
        group_name TEXT NOT NULL,
        pair_number INTEGER CHECK (pair_number BETWEEN 1 AND 6),
        subject TEXT NOT NULL,
        teacher TEXT,
        room TEXT,
        confidence_score FLOAT DEFAULT 0.0 CHECK (confidence_score BETWEEN 0 AND 1),
        status TEXT DEFAULT 'pending' CHECK (status IN ('verified', 'partial', 'needs_review', 'failed')),
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
