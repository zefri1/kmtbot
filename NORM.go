package main

import (
	// ... остальные импорты
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/gocolly/colly/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ... Константы и переменные без изменений

func main() {
	telegramToken := os.Getenv("TELEGRAM_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN не задан")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL не задан")
	}

	initGeminiLimiter()
	log.Printf("Инициализирован глобальный лимитер Gemini API: %d запросов в минуту", geminiRPM)

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute

	db, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Не удалось подключиться к БД: %v", err)
	}
	log.Println("Успешно подключились к Postgres")

	if err := ensureUsersTable(ctx); err != nil {
		log.Fatalf("ensureUsersTable: %v", err)
	}
	if err := ensureScheduleTable(ctx); err != nil {
		log.Fatalf("ensureScheduleTable: %v", err)
	}
	if err := ensureScheduleParsedTable(ctx); err != nil {
		log.Fatalf("ensureScheduleParsedTable: %v", err)
	}

	// ... остальной код main функции
}

func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скрапере: %v", r)
			lastScrapeSuccess = false
		}
	}()

	// ... код до загрузки новых файлов 
	var wgUpload sync.WaitGroup
	semaphore := make(chan struct{}, 4)
	// ... загрузка новых файлов
	wgUpload.Wait()
	
	runOcrOnNewSchedules(newItemsA, newItemsB)
	
	// ... всё что дальше без изменений
}

// ... остальной код файла без изменений
