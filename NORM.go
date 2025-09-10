// main.go
package main

import (
	"context"
	"database/sql" // Для sql.NullString
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/gocolly/colly/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // если потребуется совместимость с database/sql
)

const (
	webhookPath = "/webhook"
	baseSiteURL = "https://kmtko.my1.ru"
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
	// adminChatID: обязательно int64, чтобы корректно передавать в API
	adminChatID = int64(6436017953)
)

type ScheduleItem struct {
	URL    string
	Date   time.Time
	FileID string
}

var (
	bot       *tgbotapi.BotAPI
	db        *pgxpool.Pool
	mu        sync.RWMutex
	scheduleA = make(map[string]*ScheduleItem)
	scheduleB = make(map[string]*ScheduleItem)
)

func main() {
	telegramToken := os.Getenv("TELEGRAM_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN не задан. Установите переменную окружения.")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL не задан. Установите переменную окружения.")
	}

	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	// Параметры пула — можно менять под нагрузку
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute

	db, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("pgxpool.NewWithConfig: %v", err)
	}
	defer db.Close()

	// Проверка соединения
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Не удалось подключиться к БД: %v", err)
	}
	log.Println("Успешно подключились к Postgres")

	// Простая миграция
	if err := ensureUsersTable(ctx); err != nil {
		log.Fatalf("ensureUsersTable: %v", err)
	}
	if err := ensureScheduleTable(ctx); err != nil {
		log.Fatalf("ensureScheduleTable: %v", err)
	}

	// Инициализация бота
	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

	// Вебхук
	externalURL := os.Getenv("RENDER_EXTERNAL_URL")
	if externalURL == "" {
		externalURL = "http://localhost:8080"
		log.Println("RENDER_EXTERNAL_URL не найден, использую localhost (локально).")
	}
	webhookURL := strings.TrimRight(externalURL, "/") + webhookPath
	wh, err := tgbotapi.NewWebhook(webhookURL)
	if err != nil {
		log.Fatalf("Ошибка при создании webhook: %v", err)
	}
	_, err = bot.Request(wh)
	if err != nil {
		log.Fatalf("Ошибка при установке вебхука: %v", err)
	}
	log.Printf("Вебхук установлен на: %s", webhookURL)

	// pprof (локально)
	go func() {
		log.Println("pprof слушает на :6060 (локально)")
		log.Fatal(http.ListenAndServe(":6060", nil))
	}()

	// Запуск скрейпера в фоне
	go func() {
		// Первый запуск сразу
		scrapeImages()
		for {
			time.Sleep(30 * time.Minute)
			scrapeImages()
		}
	}()

	// HTTP handlers
	http.HandleFunc(webhookPath, handleWebhook)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Bot running"))
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("HTTP-сервер стартует на :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// ensureUsersTable создаёт таблицу users, если её нет
func ensureUsersTable(ctx context.Context) error {
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS users (
	  id BIGINT PRIMARY KEY,
	  username TEXT,
	  first_seen TIMESTAMPTZ DEFAULT now(),
	  last_seen TIMESTAMPTZ DEFAULT now()
	);
	`)
	return err
}

// ensureScheduleTable создаёт таблицу schedule_cache, если её нет
func ensureScheduleTable(ctx context.Context) error {
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS schedule_cache (
		url TEXT PRIMARY KEY,
		corpus TEXT NOT NULL,
		scraped_date DATE NOT NULL,
		file_id TEXT
	);
	`)
	return err
}

// handleWebhook обрабатывает входящие webhook'и
func handleWebhook(w http.ResponseWriter, r *http.Request) {
	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("Ошибка декодирования обновления: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Обработка асинхронно, но ответ webhook'у сразу
	go processUpdate(update)
	w.WriteHeader(http.StatusOK)
}

// processUpdate обрабатывает одно обновление
func processUpdate(update tgbotapi.Update) {
	// Сохраняем/обновляем пользователя
	if update.Message != nil && update.Message.From != nil {
		if err := saveUserFromUpdate(update); err != nil {
			log.Printf("saveUserFromUpdate err: %v", err)
		}
	}

	// Обработка команд и текста
	if update.Message != nil && update.Message.IsCommand() && update.Message.Command() == "start" {
		sendStartMessage(update.Message.Chat.ID)
	} else if update.Message != nil && update.Message.Text != "" {
		switch update.Message.Text {
		case "Расписание А":
			sendSchedule(update.Message.Chat.ID, "A")
		case "Расписание Б":
			sendSchedule(update.Message.Chat.ID, "B")
		case "Поддержка и предложения":
			sendSupportMessage(update.Message.Chat.ID)
		default:
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Выберите кнопку на клавиатуре или напишите команду /start")
			_, _ = bot.Send(msg)
		}
	}
}

func saveUserFromUpdate(update tgbotapi.Update) error {
	if update.Message == nil || update.Message.From == nil {
		return nil
	}
	userId := int64(update.Message.From.ID)
	username := update.Message.From.UserName

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := db.Exec(ctx, `
	INSERT INTO users (id, username, first_seen, last_seen)
	VALUES ($1, $2, now(), now())
	ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, last_seen = now();
	`, userId, username)
	if err != nil {
		return fmt.Errorf("db exec: %w", err)
	}
	log.Printf("User saved: %d (%s)", userId, username)
	return nil
}

// getUsersIDs получает список всех ID пользователей из БД
func getUsersIDs(ctx context.Context) ([]int64, error) {
	rows, err := db.Query(ctx, "SELECT id FROM users")
	if err != nil {
		return nil, fmt.Errorf("db.Query users: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("rows.Scan user id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err users: %w", err)
	}
	log.Printf("Получено %d пользователей для уведомлений", len(ids))
	return ids, nil
}

// loadScheduleCache загружает текущий кэш расписания из БД
func loadScheduleCache(ctx context.Context) (map[string]*ScheduleItem, map[string]*ScheduleItem, error) {
	rows, err := db.Query(ctx, "SELECT url, corpus, scraped_date, file_id FROM schedule_cache")
	if err != nil {
		return nil, nil, fmt.Errorf("db.Query: %w", err)
	}
	defer rows.Close()

	sA := make(map[string]*ScheduleItem)
	sB := make(map[string]*ScheduleItem)

	for rows.Next() {
		var item ScheduleItem
		var corpus string
		var fileID sql.NullString
		if err := rows.Scan(&item.URL, &corpus, &item.Date, &fileID); err != nil {
			return nil, nil, fmt.Errorf("rows.Scan: %w", err)
		}
		item.FileID = fileID.String
		if corpus == "a" {
			sA[item.URL] = &item
		} else if corpus == "b" {
			sB[item.URL] = &item
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows.Err: %w", err)
	}
	return sA, sB, nil
}

// saveScheduleCache сохраняет новый кэш расписания в БД (через транзакцию)
func saveScheduleCache(ctx context.Context, scheduleA, scheduleB map[string]*ScheduleItem) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "DELETE FROM schedule_cache")
	if err != nil {
		return fmt.Errorf("tx.Exec DELETE: %w", err)
	}

	if _, err := tx.Prepare(ctx, "insert_schedule", "INSERT INTO schedule_cache (url, corpus, scraped_date, file_id) VALUES ($1, $2, $3, $4)"); err != nil {
		return fmt.Errorf("tx.Prepare: %w", err)
	}

	for _, item := range scheduleA {
		if _, err := tx.Exec(ctx, "insert_schedule", item.URL, "a", item.Date, item.FileID); err != nil {
			return fmt.Errorf("tx.Exec INSERT A: %w", err)
		}
	}
	for _, item := range scheduleB {
		if _, err := tx.Exec(ctx, "insert_schedule", item.URL, "b", item.Date, item.FileID); err != nil {
			return fmt.Errorf("tx.Exec INSERT B: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx.Commit: %w", err)
	}
	log.Println("Кэш расписания успешно сохранен в БД")
	return nil
}

// findNewItems находит элементы в newMap, которых нет в oldMap
func findNewItems(newMap, oldMap map[string]*ScheduleItem) map[string]*ScheduleItem {
	newItems := make(map[string]*ScheduleItem)
	for url, newItem := range newMap {
		if _, exists := oldMap[url]; !exists {
			newItems[url] = newItem
		}
	}
	return newItems
}

// sendNotificationForNewItems отправляет конкретные новые расписания пользователю
func sendNotificationForNewItems(chatID int64, corpus string, newItems map[string]*ScheduleItem) {
	type item struct{ *ScheduleItem }
	var items []item
	for _, it := range newItems {
		items = append(items, item{it})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Date.Before(items[j].Date)
	})

	weekdays := map[time.Weekday]string{
		time.Monday:    "Понедельник",
		time.Tuesday:   "Вторник",
		time.Wednesday: "Среда",
		time.Thursday:  "Четверг",
		time.Friday:    "Пятница",
		time.Saturday:  "Суббота",
		time.Sunday:    "Воскресенье",
	}

	headerMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("🔔 Новое расписание для корпуса %s!", corpus))
	if _, err := bot.Send(headerMsg); err != nil {
		log.Printf("Ошибка отправки заголовка уведомления пользователю %d: %v", chatID, err)
	}

	for _, it := range items {
		weekday := weekdays[it.Date.Weekday()]
		caption := fmt.Sprintf("%s — %02d.%02d.%d", weekday, it.Date.Day(), it.Date.Month(), it.Date.Year())

		var msg tgbotapi.Chattable
		if it.FileID != "" {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileID(it.FileID))
			photo.Caption = caption
			msg = photo
		} else {
			uniqueURL := fmt.Sprintf("%s?notify_new=%d", it.URL, time.Now().UnixNano())
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
			photo.Caption = caption
			msg = photo
		}

		if _, err := bot.Send(msg); err != nil {
			log.Printf("Ошибка отправки нового расписания пользователю %d: %v", chatID, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// notifyUsersAboutNewSchedule уведомляет всех пользователей о новых расписаниях
func notifyUsersAboutNewSchedule(newScheduleA, newScheduleB, oldScheduleA, oldScheduleB map[string]*ScheduleItem) {
	newItemsA := findNewItems(newScheduleA, oldScheduleA)
	newItemsB := findNewItems(newScheduleB, oldScheduleB)

	if len(newItemsA) == 0 && len(newItemsB) == 0 {
		log.Println("Нет новых расписаний для уведомления пользователей.")
		return
	}
	log.Printf("Найдено новых расписаний: A=%d, B=%d. Начинаем уведомление пользователей.", len(newItemsA), len(newItemsB))

	ctx := context.Background()
	userIDs, err := getUsersIDs(ctx)
	if err != nil {
		log.Printf("Ошибка получения списка пользователей: %v", err)
		// продолжаем с пустым списком (ничего не отправим)
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // ограничение параллельности

	for _, userID := range userIDs {
		wg.Add(1)
		go func(chatID int64) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if len(newItemsA) > 0 {
				sendNotificationForNewItems(chatID, "А", newItemsA)
			}
			if len(newItemsB) > 0 {
				sendNotificationForNewItems(chatID, "Б", newItemsB)
			}
		}(userID)
	}
	wg.Wait()
	log.Println("Уведомления пользователям отправлены.")
}

// copyScheduleMap копирует мапу *ScheduleItem (копирует указатели)
func copyScheduleMap(src map[string]*ScheduleItem) map[string]*ScheduleItem {
	dst := make(map[string]*ScheduleItem, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// sendSchedule отправляет пользователю все расписания корпуса
func sendSchedule(chatID int64, corpus string) {
	var scheduleMap map[string]*ScheduleItem

	mu.RLock()
	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = copyScheduleMap(scheduleA)
	case "B":
		scheduleMap = copyScheduleMap(scheduleB)
	default:
		mu.RUnlock()
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, "Неизвестный корпус"))
		return
	}
	mu.RUnlock()

	if len(scheduleMap) == 0 {
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, "Расписание не найдено."))
		return
	}

	type item struct{ *ScheduleItem }
	var items []item
	for _, it := range scheduleMap {
		items = append(items, item{it})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Date.Before(items[j].Date)
	})

	weekdays := map[time.Weekday]string{
		time.Monday:    "Понедельник",
		time.Tuesday:   "Вторник",
		time.Wednesday: "Среда",
		time.Thursday:  "Четверг",
		time.Friday:    "Пятница",
		time.Saturday:  "Суббота",
		time.Sunday:    "Воскресенье",
	}

	for _, it := range items {
		weekday := weekdays[it.Date.Weekday()]
		caption := fmt.Sprintf("%s — %02d.%02d.%d", weekday, it.Date.Day(), it.Date.Month(), it.Date.Year())

		var msg tgbotapi.Chattable
		if it.FileID != "" {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileID(it.FileID))
			photo.Caption = caption
			msg = photo
		} else {
			uniqueURL := fmt.Sprintf("%s?send_cb=%d", it.URL, time.Now().UnixNano())
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
			photo.Caption = caption
			msg = photo
		}

		if _, err := bot.Send(msg); err != nil {
			log.Printf("Ошибка отправки фото (URL: %s, FileID: %s): %v", it.URL, it.FileID, err)
		} else {
			log.Printf("Успешно отправлено фото -> chat %d (%s)", chatID, caption)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func sendStartMessage(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "Привет! Выберите расписание:")
	keyboard := tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Расписание А"),
			tgbotapi.NewKeyboardButton("Расписание Б"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Поддержка и предложения"),
		),
	)
	msg.ReplyMarkup = keyboard
	if _, err := bot.Send(msg); err != nil {
		log.Printf("sendStartMessage err: %v", err)
	}
}

func sendSupportMessage(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "По вопросам поддержки: @podkmt")
	if _, err := bot.Send(msg); err != nil {
		log.Printf("sendSupportMessage err: %v", err)
	}
}

// scrapeImages собирает изображения с сайта, загружает новые изображения в Telegram для получения file_id и обновляет кэш
func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скрапере: %v", r)
		}
	}()

	start := time.Now()
	log.Println("Начало скрапинга...")

	ctxLoad := context.Background()
	oldScheduleA, oldScheduleB, err := loadScheduleCache(ctxLoad)
	if err != nil {
		log.Printf("Ошибка загрузки кэша расписания из БД: %v. Продолжаем с пустым кэшем.", err)
		oldScheduleA = make(map[string]*ScheduleItem)
		oldScheduleB = make(map[string]*ScheduleItem)
	} else {
		log.Printf("Загружен кэш расписания: A=%d, B=%d", len(oldScheduleA), len(oldScheduleB))
	}

	c := colly.NewCollector(colly.Async(true))
	c.SetRequestTimeout(30 * time.Second)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 2, RandomDelay: 1 * time.Second})

	tempScheduleA := make(map[string]*ScheduleItem)
	tempScheduleB := make(map[string]*ScheduleItem)

	// Регекс ожидает: /1Raspisanie/дд.мм(.гггг)?_korpus_[a|v].jpg
	re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?_korpus_([av])\.jpe?g$`)

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		imageSrc := e.Attr("src")
		matches := re.FindStringSubmatch(imageSrc)
		if len(matches) < 5 {
			log.Printf("Пропущено: URL не соответствует формату %s", imageSrc)
			return
		}
		day, _ := strconv.Atoi(matches[1])
		month, _ := strconv.Atoi(matches[2])
		yearStr := matches[3]
		corpusChar := matches[4]

		year := time.Now().Year()
		if yearStr != "" {
			if y, err := strconv.Atoi(yearStr); err == nil {
				year = y
			}
		}

		date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
		if !strings.HasPrefix(imageSrc, "/") {
			imageSrc = "/" + imageSrc
		}
		fullURL := strings.TrimRight(baseSiteURL, "/") + imageSrc

		item := &ScheduleItem{
			URL:  fullURL,
			Date: date,
		}

		var oldCache map[string]*ScheduleItem
		var corpus string
		if corpusChar == "a" {
			oldCache = oldScheduleA
			corpus = "a"
		} else {
			oldCache = oldScheduleB
			corpus = "b"
		}
		if oldItem, exists := oldCache[fullURL]; exists && oldItem.FileID != "" {
			item.FileID = oldItem.FileID
			log.Printf("FileID для %s взят из кэша: %s", fullURL, item.FileID)
		}

		if corpus == "a" {
			tempScheduleA[fullURL] = item
			log.Printf("Найдено фото корпуса А: %s (%02d.%02d.%d)", fullURL, day, month, year)
		} else {
			tempScheduleB[fullURL] = item
			log.Printf("Найдено фото корпуса Б: %s (%02d.%02d.%d)", fullURL, day, month, year)
		}
	})

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s", r.URL.String())
	})
	c.OnError(func(r *colly.Response, err error) {
		if r != nil && r.Request != nil {
			log.Printf("Ошибка скрапинга %s: %v", r.Request.URL.String(), err)
		} else {
			log.Printf("Ошибка скрапинга: %v", err)
		}
	})

	if err := c.Visit(strings.TrimRight(baseSiteURL, "/ ") + targetPath); err != nil {
		log.Printf("Ошибка Visit: %v", err)
		return
	}
	c.Wait()

	// Функция загрузки фото в Telegram для получения file_id
	uploadAndGetFileID := func(item *ScheduleItem) string {
		uploadURL := fmt.Sprintf("%s?upload_cache_bust_scrape=%d", item.URL, time.Now().UnixNano())
		log.Printf("Попытка загрузки %s (уникальный URL: %s) в чат %d", item.URL, uploadURL, adminChatID)
		photo := tgbotapi.NewPhoto(adminChatID, tgbotapi.FileURL(uploadURL))
		photo.DisableNotification = true
		photo.Caption = fmt.Sprintf("[Кэширование] %s", item.URL)

		msg, err := bot.Send(photo)
		if err != nil {
			log.Printf("Ошибка загрузки фото в Telegram (для кэширования) %s: %v", item.URL, err)
			return ""
		}
		if len(msg.Photo) > 0 {
			fileID := msg.Photo[len(msg.Photo)-1].FileID
			log.Printf("Загружено и закэшировано фото %s -> FileID: %s", item.URL, fileID)
			// Опционально можно удалить сообщение администратору, но это может требовать прав
			return fileID
		}
		log.Printf("Ошибка: Сообщение с фото не содержит фото %s. Ответ от Telegram: %+v", item.URL, msg)
		return ""
	}

	// Определяем новые элементы (которые были не в старом кэше)
	newItemsA := findNewItems(tempScheduleA, oldScheduleA)
	newItemsB := findNewItems(tempScheduleB, oldScheduleB)
	log.Printf("Найдено новых изображений для загрузки: A=%d, B=%d", len(newItemsA), len(newItemsB))

	uploadStart := time.Now()
	var wgUpload sync.WaitGroup

	// Параллельно загружаем (с лимитом)
	limit := 5
	semaA := make(chan struct{}, limit)
	for _, item := range newItemsA {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaA <- struct{}{}
				defer func() { <-semaA }()
				it.FileID = uploadAndGetFileID(it)
			}(item)
		}
	}
	wgUpload.Wait()

	semaB := make(chan struct{}, limit)
	for _, item := range newItemsB {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaB <- struct{}{}
				defer func() { <-semaB }()
				it.FileID = uploadAndGetFileID(it)
			}(item)
		}
	}
	wgUpload.Wait()

	log.Printf("Загрузка новых изображений завершена за %v", time.Since(uploadStart))

	// Обновляем глобальные мапы
	mu.Lock()
	scheduleA = make(map[string]*ScheduleItem)
	scheduleB = make(map[string]*ScheduleItem)
	for k, v := range tempScheduleA {
		scheduleA[k] = v
	}
	for k, v := range tempScheduleB {
		scheduleB[k] = v
	}
	mu.Unlock()

	// Сохраняем кэш в БД
	ctxSave := context.Background()
	if err := saveScheduleCache(ctxSave, scheduleA, scheduleB); err != nil {
		log.Printf("Ошибка сохранения кэша расписания в БД: %v", err)
	} else {
		log.Println("Новый кэш расписания сохранен в БД.")
	}

	log.Printf("Скрапинг и кэширование завершены, A=%d B=%d за %v", len(scheduleA), len(scheduleB), time.Since(start))

	// Уведомляем пользователей о новых элементах
	notifyUsersAboutNewSchedule(scheduleA, scheduleB, oldScheduleA, oldScheduleB)
}
