package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

const (
	webhookPath = "/webhook"
	// Исправлено: убраны лишние пробелы
	baseSiteURL = "https://kmtko.my1.ru" // ИСПРАВЛЕНО: Пробелы убраны
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
	// adminChatID = int64(6436017953) // Убрано, так как не используется напрямую в этом коде
	// Добавлено: Команда для администратора для просмотра статистики
	adminCommandStats = "/stats"
)

// ИЗМЕНЕНО: Добавлено поле PreferredCorpus в структуру пользователя
type User struct {
	ID              int64
	Username        sql.NullString
	FirstSeen       time.Time
	LastSeen        time.Time
	PreferredCorpus sql.NullString // Новое поле
}

type ScheduleItem struct {
	URL    string
	Date   time.Time
	FileID string
}

var (
	bot               *tgbotapi.BotAPI
	db                *pgxpool.Pool
	mu                sync.RWMutex
	scheduleA         = make(map[string]*ScheduleItem)
	scheduleB         = make(map[string]*ScheduleItem)
	lastScrapeSuccess = false
	// ИЗМЕНЕНО: Убедитесь, что тип int64 и значение заменено на ваш реальный ID
	adminUserID int64 = 535803934 // Замените на реальный ID админа
)

func main() {
	telegramToken := os.Getenv("TELEGRAM_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN не задан")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL не задан")
	}

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

	// ИЗМЕНЕНО: ensureUsersTable обновлена
	if err := ensureUsersTable(ctx); err != nil {
		log.Fatalf("ensureUsersTable: %v", err)
	}
	if err := ensureScheduleTable(ctx); err != nil {
		log.Fatalf("ensureScheduleTable: %v", err)
	}

	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

	externalURL := os.Getenv("RENDER_EXTERNAL_URL")
	if externalURL == "" {
		externalURL = "http://localhost:8080"
		log.Println("RENDER_EXTERNAL_URL не найден, использую localhost")
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

	go func() {
		log.Println("pprof слушает на :6060")
		log.Fatal(http.ListenAndServe(":6060", nil))
	}()

	go func() {
		// Выполняем первый скрейпинг сразу при запуске
		scrapeImages()
		for {
			// Затем каждые 30 минут
			time.Sleep(30 * time.Minute)
			scrapeImages()
		}
	}()

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

// ИЗМЕНЕНО: ensureUsersTable теперь включает preferred_corpus
func ensureUsersTable(ctx context.Context) error {
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS users (
	  id BIGINT PRIMARY KEY,
	  username TEXT,
	  first_seen TIMESTAMPTZ DEFAULT now(),
	  last_seen TIMESTAMPTZ DEFAULT now(),
	  preferred_corpus TEXT -- Новое поле
	);
	`)
	return err
}

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

func handleWebhook(w http.ResponseWriter, r *http.Request) {
	var update tgbotapi.Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("Ошибка декодирования обновления: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	go processUpdate(update)
	w.WriteHeader(http.StatusOK)
}

func processUpdate(update tgbotapi.Update) {
	if update.Message != nil && update.Message.From != nil {
		if err := saveUserFromUpdate(update); err != nil {
			log.Printf("saveUserFromUpdate err: %v", err)
		}
	}

	if update.Message != nil && update.Message.IsCommand() {
		switch update.Message.Command() {
		case "start":
			sendStartMessage(update.Message.Chat.ID)
		case "stats":
			// Добавлено: Обработка команды /stats
			// ИЗМЕНЕНО: правильное сравнение типов int64
			if update.Message.From != nil && update.Message.From.ID == adminUserID {
				go sendStatsToAdmin(update.Message.Chat.ID)
			} else {
				msg := tgbotapi.NewMessage(update.Message.Chat.ID, "У вас нет доступа к этой команде.")
				_, _ = bot.Send(msg)
			}
		}
	} else if update.Message != nil && update.Message.Text != "" {
		switch update.Message.Text {
		case "Расписание А":
			log.Printf("Пользователь %d (%s) запросил расписание корпуса А", update.Message.From.ID, update.Message.From.UserName)
			sendSchedule(update.Message.Chat.ID, "A")
			// ИЗМЕНЕНО: Обновляем предпочтение пользователя
			if err := updateUserPreference(update.Message.From.ID, "A"); err != nil {
				log.Printf("Ошибка обновления предпочтения пользователя %d: %v", update.Message.From.ID, err)
			}
		case "Расписание Б":
			log.Printf("Пользователь %d (%s) запросил расписание корпуса Б", update.Message.From.ID, update.Message.From.UserName)
			sendSchedule(update.Message.Chat.ID, "B")
			// ИЗМЕНЕНО: Обновляем предпочтение пользователя
			if err := updateUserPreference(update.Message.From.ID, "B"); err != nil {
				log.Printf("Ошибка обновления предпочтения пользователя %d: %v", update.Message.From.ID, err)
			}
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

// ИЗМЕНЕНО: Добавлена функция для обновления предпочтений пользователя
// ПРОВЕРЕНО: Эта функция корректно сохраняет данные в таблицу users
func updateUserPreference(userID int64, corpus string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := db.Exec(ctx, `
	UPDATE users SET preferred_corpus = $1 WHERE id = $2;
	`, corpus, userID)
	if err != nil {
		return fmt.Errorf("db exec update preferred_corpus: %w", err)
	}
	log.Printf("Предпочтение пользователя %d обновлено: %s", userID, corpus)
	return nil
}

// ИЗМЕНЕНО: Обновлена функция getUsers для возврата структуры User
func getUsers(ctx context.Context) ([]User, error) {
	rows, err := db.Query(ctx, "SELECT id, username, first_seen, last_seen, preferred_corpus FROM users")
	if err != nil {
		return nil, fmt.Errorf("db.Query users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		var username, preferredCorpus sql.NullString
		if err := rows.Scan(&u.ID, &username, &u.FirstSeen, &u.LastSeen, &preferredCorpus); err != nil {
			return nil, fmt.Errorf("rows.Scan user: %w", err)
		}
		u.Username = username
		u.PreferredCorpus = preferredCorpus
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err users: %w", err)
	}
	log.Printf("Получено %d пользователей", len(users))
	return users, nil
}

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

func findNewItems(newMap, oldMap map[string]*ScheduleItem) map[string]*ScheduleItem {
	newItems := make(map[string]*ScheduleItem)
	for url, newItem := range newMap {
		if _, exists := oldMap[url]; !exists {
			newItems[url] = newItem
		}
	}
	return newItems
}

func notifyUsersAboutNewSchedule(newScheduleA, newScheduleB, oldScheduleA, oldScheduleB map[string]*ScheduleItem) {
	newItemsA := findNewItems(newScheduleA, oldScheduleA)
	newItemsB := findNewItems(newScheduleB, oldScheduleB)

	if len(newItemsA) == 0 && len(newItemsB) == 0 {
		log.Println("Нет новых расписаний для уведомления пользователей.")
		return
	}
	log.Printf("Найдено новых расписаний: A=%d, B=%d. Начинаем уведомление пользователей.", len(newItemsA), len(newItemsB))

	ctx := context.Background()
	// ИЗМЕНЕНО: Используем getUsers вместо getUsersIDs
	users, err := getUsers(ctx)
	if err != nil {
		log.Printf("Ошибка получения списка пользователей: %v", err)
		return
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)

	for _, user := range users {
		wg.Add(1)
		go func(u User) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if len(newItemsA) > 0 {
				sendNotificationForNewItems(u.ID, "А", newItemsA)
			}
			if len(newItemsB) > 0 {
				sendNotificationForNewItems(u.ID, "Б", newItemsB)
			}
		}(user)
	}
	wg.Wait()
	log.Println("Уведомления пользователям отправлены.")
}

func sendNotificationForNewItems(chatID int64, corpus string, newItems map[string]*ScheduleItem) {
	if len(newItems) == 0 {
		return
	}
	headerMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("🔔 Появилось новое расписание для корпуса %s! Чтобы посмотреть, нажмите кнопку «Расписание %s»", corpus, corpus))
	if _, err := bot.Send(headerMsg); err != nil {
		log.Printf("Ошибка отправки уведомления пользователю %d: %v", chatID, err)
	}
}

func copyScheduleMap(src map[string]*ScheduleItem) map[string]*ScheduleItem {
	dst := make(map[string]*ScheduleItem, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ИЗМЕНЕНО: sendSchedule теперь логирует больше информации
func sendSchedule(chatID int64, corpus string) {
	log.Printf("Начало отправки расписания корпуса %s пользователю %d", corpus, chatID)

	var scheduleMap map[string]*ScheduleItem
	// НЕБЛОКИРУЮЩЕЕ ЧТЕНИЕ: Используем RLock для чтения
	mu.RLock()
	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = copyScheduleMap(scheduleA)
	case "B":
		scheduleMap = copyScheduleMap(scheduleB)
	default:
		mu.RUnlock()
		log.Printf("Неизвестный корпус '%s' для пользователя %d", corpus, chatID)
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, "Неизвестный корпус"))
		return
	}
	// RUnlock происходит сразу после копирования
	mu.RUnlock()

	if len(scheduleMap) == 0 {
		log.Printf("Расписание корпуса %s не найдено для пользователя %d", corpus, chatID)
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

	log.Printf("Отправка %d изображений расписания корпуса %s пользователю %d", len(items), corpus, chatID)
	for i, it := range items {
		weekday := weekdays[it.Date.Weekday()]
		caption := fmt.Sprintf("%s — %02d.%02d.%d", weekday, it.Date.Day(), it.Date.Month(), it.Date.Year())

		var msg tgbotapi.Chattable
		if it.FileID != "" {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileID(it.FileID))
			photo.Caption = caption
			msg = photo
			log.Printf("Отправка по FileID: %s -> chat %d (%s) [элемент %d/%d]", it.FileID, chatID, caption, i+1, len(items))
		} else {
			log.Printf("FileID отсутствует, отправка по URL: %s -> chat %d [элемент %d/%d]", it.URL, chatID, i+1, len(items))
			uniqueURL := fmt.Sprintf("%s?send_cb=%d", it.URL, time.Now().UnixNano())
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
			photo.Caption = caption
			msg = photo
		}

		if _, err := bot.Send(msg); err != nil {
			log.Printf("Ошибка отправки фото (URL: %s, FileID: %s) пользователю %d: %v", it.URL, it.FileID, chatID, err)
		} else {
			log.Printf("Успешно отправлено фото -> chat %d (%s) [элемент %d/%d]", chatID, caption, i+1, len(items))
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Printf("Завершена отправка расписания корпуса %s пользователю %d", corpus, chatID)
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

// ДОБАВЛЕНО И ПРОВЕРЕНО: Функция для отправки статистики администратору
// Эта функция использует getUsers, которая получает данные из таблицы users, включая preferred_corpus
func sendStatsToAdmin(chatID int64) {
	ctx := context.Background()
	users, err := getUsers(ctx)
	if err != nil {
		log.Printf("Ошибка получения пользователей для статистики: %v", err)
		msg := tgbotapi.NewMessage(chatID, "Ошибка получения статистики.")
		_, _ = bot.Send(msg)
		return
	}

	// Подсчет предпочтений
	corpusACount := 0
	corpusBCount := 0
	noPreferenceCount := 0

	for _, user := range users {
		if user.PreferredCorpus.Valid {
			switch strings.ToUpper(user.PreferredCorpus.String) {
			case "A":
				corpusACount++
			case "B":
				corpusBCount++
			default:
				noPreferenceCount++
			}
		} else {
			noPreferenceCount++
		}
	}

	statsText := fmt.Sprintf(
		"📊 Статистика пользователей:\n\n"+
			"Всего пользователей: %d\n"+
			"Предпочитают корпус А: %d\n"+
			"Предпочитают корпус Б: %d\n"+
			"Без предпочтений: %d",
		len(users), corpusACount, corpusBCount, noPreferenceCount,
	)

	msg := tgbotapi.NewMessage(chatID, statsText)
	_, err = bot.Send(msg)
	if err != nil {
		log.Printf("Ошибка отправки статистики админу %d: %v", chatID, err)
	}
	log.Printf("Статистика отправлена администратору %d", chatID)
}

// ИСПРАВЛЕНО: Функция для загрузки и получения file_id
// Убрана предварительная проверка isImageAccessible из-за ошибки 403
// Теперь сразу пытаемся загрузить изображение через Telegram
func uploadAndGetFileID(item *ScheduleItem) string {
	// Генерируем уникальный URL для загрузки (чтобы Telegram не использовал свой кэш)
	uploadURL := fmt.Sprintf("%s?upload_cache_bust_scrape=%d", item.URL, time.Now().UnixNano())
	log.Printf("Попытка загрузки %s (уникальный URL: %s) в чат администратора для кэширования", item.URL, uploadURL)

	// Создаем фото для отправки в чат администратора
	// ДОБАВЛЕНО: adminChatID как локальная константа для ясности
	const adminChatID = int64(6436017953) // Предполагаемый ID чата для кэширования
	photo := tgbotapi.NewPhoto(adminChatID, tgbotapi.FileURL(uploadURL))
	photo.DisableNotification = true // Не уведомлять администратора
	photo.Caption = fmt.Sprintf("[Кэширование] %s", item.URL)

	// Пытаемся отправить фото в Telegram для получения file_id
	// Telegram сам попытается получить изображение по URL
	msg, err := bot.Send(photo)
	if err != nil {
		log.Printf("Ошибка загрузки фото в Telegram (для кэширования) %s: %v", item.URL, err)
		// Проверяем тип ошибки Telegram API
		if apiErr, ok := err.(tgbotapi.Error); ok {
			log.Printf("Детали ошибки Telegram API: Code=%d, Message=%s", apiErr.Code, apiErr.Message)
		}
		// Даже если Telegram не может получить изображение, мы не паникуем, просто не кэшируем
		return "" // Возвращаем пустую строку в случае ошибки
	}

	// Проверяем, что Telegram вернул информацию о фото
	if len(msg.Photo) == 0 {
		log.Printf("Ошибка: Telegram вернул сообщение без фото для %s. Ответ: %+v", item.URL, msg)
		return ""
	}

	// Получаем file_id из отправленного сообщения (берем фото с наилучшим качеством)
	fileID := msg.Photo[len(msg.Photo)-1].FileID
	log.Printf("Загружено и закэшировано фото %s -> FileID: %s", item.URL, fileID)

	// Пытаемся удалить сообщение из кэш-чата, чтобы не засорять его
	_, delErr := bot.Request(tgbotapi.NewDeleteMessage(adminChatID, msg.MessageID))
	if delErr != nil {
		log.Printf("Предупреждение: Не удалось удалить сообщение %d из кэш-чата: %v", msg.MessageID, delErr)
		// Это не критично, продолжаем работу
	} else {
		log.Printf("Сообщение %d успешно удалено из кэш-чата", msg.MessageID)
	}

	return fileID
}

// ИСПРАВЛЕНО: scrapeImages с неблокирующим поведением
func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скрапере: %v", r)
			lastScrapeSuccess = false
		}
	}()

	start := time.Now()
	log.Println("=== Начало скрапинга ===")

	ctxLoad := context.Background()
	oldScheduleA, oldScheduleB, err := loadScheduleCache(ctxLoad)
	if err != nil {
		log.Printf("Ошибка загрузки кэша: %v. Продолжаем с пустым кэшем.", err)
		oldScheduleA = make(map[string]*ScheduleItem)
		oldScheduleB = make(map[string]*ScheduleItem)
	} else {
		log.Printf("Загружен кэш расписания: A=%d, B=%d", len(oldScheduleA), len(oldScheduleB))
	}

	c := colly.NewCollector(colly.Async(true))
	c.SetRequestTimeout(30 * time.Second)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 1, RandomDelay: 500 * time.Millisecond})

	tempScheduleA := make(map[string]*ScheduleItem)
	tempScheduleB := make(map[string]*ScheduleItem)

	// ИСПРАВЛЕНО: Уточнено регулярное выражение для соответствия реальным URL из HTML
	// Обрабатывает даты в форматах DD.MM и D.M, с необязательным годом, и расширения .jpg, .jpeg
	re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?_korpus_([av])\.jpe?g(?:\?.*)?$`)

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		src := e.Attr("src")
		// Убираем возможные параметры запроса перед обработкой регулярным выражением
		srcClean := strings.Split(src, "?")[0]
		matches := re.FindStringSubmatch(srcClean)
		if len(matches) < 5 { // Проверяем, что найдено хотя бы 4 группы + полное совпадение
			// log.Printf("Несовпадение RE для %s (очищенный: %s)", src, srcClean) // Для отладки
			return
		}
		day, _ := strconv.Atoi(matches[1])
		month, _ := strconv.Atoi(matches[2])
		yearStr := matches[3]
		corpus := strings.ToLower(matches[4]) // 'a' или 'v' (как 'b')

		// Определяем год
		year := time.Now().Year()
		if yearStr != "" {
			if parsedYear, err := strconv.Atoi(yearStr); err == nil {
				year = parsedYear
			}
		}

		// Создаем дату
		date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)

		// Формируем полный URL надежным способом
		// Убедимся, что baseSiteURL не заканчивается на '/', а src начинается с '/'
		fullURL := strings.TrimRight(baseSiteURL, "/") + path.Clean("/"+strings.TrimLeft(srcClean, "/"))

		// Создаем элемент расписания
		item := &ScheduleItem{
			URL:  fullURL,
			Date: date,
			// FileID будет заполнен позже или взят из кэша
		}

		// --- Попытка взять FileID из старого кэша ---
		var oldCache map[string]*ScheduleItem
		if corpus == "a" {
			oldCache = oldScheduleA
		} else { // corpus == "v" -> "b"
			oldCache = oldScheduleB
		}
		if oldItem, exists := oldCache[item.URL]; exists && oldItem.FileID != "" {
			item.FileID = oldItem.FileID // Используем существующий FileID
			log.Printf("FileID для %s взят из кэша: %s", item.URL, item.FileID)
		}
		// --- Конец получения из кэша ---

		// Добавляем во временные мапы
		if corpus == "a" {
			tempScheduleA[item.URL] = item
			log.Printf("Найдено фото корпуса А: %s (%02d.%02d.%d)", item.URL, day, month, year)
		} else { // corpus == "v"
			tempScheduleB[item.URL] = item
			log.Printf("Найдено фото корпуса Б: %s (%02d.%02d.%d)", item.URL, day, month, year)
		}
	})

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s", r.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Printf("Ошибка скрапинга %s: %v", r.Request.URL.String(), err)
	})

	visitURL := strings.TrimRight(baseSiteURL, "/") + targetPath
	log.Printf("Начинаем посещение: %s", visitURL)
	err = c.Visit(visitURL)
	if err != nil {
		log.Printf("Ошибка посещения сайта: %v", err)
		lastScrapeSuccess = false
		return
	}
	c.Wait()
	log.Println("Скрапинг HTML завершен.")

	log.Println("Начинаем загрузку новых изображений в Telegram...")
	uploadStart := time.Now()

	newItemsA := findNewItems(tempScheduleA, oldScheduleA)
	newItemsB := findNewItems(tempScheduleB, oldScheduleB)

	log.Printf("Найдено новых изображений для загрузки: A=%d, B=%d", len(newItemsA), len(newItemsB))

	var wgUpload sync.WaitGroup
	semaphoreA := make(chan struct{}, 2)
	for _, item := range newItemsA {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaphoreA <- struct{}{}
				defer func() { <-semaphoreA }()
				it.FileID = uploadAndGetFileID(it)
			}(item)
		}
	}
	wgUpload.Wait()

	semaphoreB := make(chan struct{}, 2)
	for _, item := range newItemsB {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaphoreB <- struct{}{}
				defer func() { <-semaphoreB }()
				it.FileID = uploadAndGetFileID(it)
			}(item)
		}
	}
	wgUpload.Wait()

	log.Printf("Загрузка новых изображений завершена за %v", time.Since(uploadStart))

	// НЕБЛОКИРУЮЩЕЕ ОБНОВЛЕНИЕ: Блокировка mu удерживается минимальное время
	// только на атомарное обновление глобальных переменных
	mu.Lock()
	// Создаем новые мапы для глобального состояния
	newScheduleA := make(map[string]*ScheduleItem, len(tempScheduleA))
	newScheduleB := make(map[string]*ScheduleItem, len(tempScheduleB))
	// Копируем данные из временных мап
	for k, v := range tempScheduleA {
		newScheduleA[k] = v
	}
	for k, v := range tempScheduleB {
		newScheduleB[k] = v
	}
	// Атомарно заменяем ссылки на глобальные мапы
	scheduleA = newScheduleA
	scheduleB = newScheduleB
	// Блокировка освобождается сразу после замены
	mu.Unlock()
	log.Println("Глобальные мапы расписаний обновлены.")

	ctxSave := context.Background()
	if saveErr := saveScheduleCache(ctxSave, scheduleA, scheduleB); saveErr != nil {
		log.Printf("Ошибка сохранения кэша после скрапинга: %v", saveErr)
		lastScrapeSuccess = false
	} else {
		log.Println("Новый кэш расписания успешно сохранен в БД.")
		if len(oldScheduleA) == 0 && len(oldScheduleB) == 0 {
			log.Println("Первый запуск скрапинга: уведомления не отправляются")
		} else {
			notifyUsersAboutNewSchedule(scheduleA, scheduleB, oldScheduleA, oldScheduleB)
		}
		lastScrapeSuccess = true
	}

	log.Printf("=== Скрапинг завершён за %s ===", time.Since(start))
}
