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
	baseSiteURL = "https://kmtko.my1.ru"
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
	// adminChatID = int64(6436017953) // Убрано, так как не используется напрямую в этом коде
)

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
	lastScrapeSuccess = false // Переменная для контроля успешного скрапинга
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

// --- Логика уведомлений ---
// Уведомления отправляются, если текущий скрапинг успешен и найдены новые элементы.
func notifyUsersAboutNewSchedule(newScheduleA, newScheduleB, oldScheduleA, oldScheduleB map[string]*ScheduleItem) {
	// Убрано условие if !lastScrapeSuccess

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
		return
	}

	var wg sync.WaitGroup
	// Ограничитель для параллельных отправок
	semaphore := make(chan struct{}, 10)

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

func sendNotificationForNewItems(chatID int64, corpus string, newItems map[string]*ScheduleItem) {
	if len(newItems) == 0 {
		return
	}
	// Сообщение изменено для соответствия логике кнопок
	headerMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("🔔 Появилось новое расписание для корпуса %s! Чтобы посмотреть, нажмите кнопку «Расписание %s»", corpus, corpus))
	if _, err := bot.Send(headerMsg); err != nil {
		log.Printf("Ошибка отправки уведомления пользователю %d: %v", chatID, err)
	}
}

// --- Логика отправки расписания пользователю ---
func copyScheduleMap(src map[string]*ScheduleItem) map[string]*ScheduleItem {
	dst := make(map[string]*ScheduleItem, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

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
			log.Printf("Отправка по FileID: %s -> chat %d (%s)", it.FileID, chatID, caption)
		} else {
			log.Printf("FileID отсутствует, отправка по URL: %s -> chat %d", it.URL, chatID)
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
		time.Sleep(50 * time.Millisecond) // Минимальная задержка
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

// --- scrapeImages с восстановленной логикой загрузки ---
func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скрапере: %v", r)
			lastScrapeSuccess = false
		}
	}()

	start := time.Now()
	log.Println("=== Начало скрапинга ===")

	// --- Загружаем предыдущий кэш ---
	ctxLoad := context.Background()
	oldScheduleA, oldScheduleB, err := loadScheduleCache(ctxLoad)
	if err != nil {
		log.Printf("Ошибка загрузки кэша: %v. Продолжаем с пустым кэшем.", err)
		oldScheduleA = make(map[string]*ScheduleItem)
		oldScheduleB = make(map[string]*ScheduleItem)
	} else {
		log.Printf("Загружен кэш расписания: A=%d, B=%d", len(oldScheduleA), len(oldScheduleB))
	}
	// --- Конец загрузки кэша ---

	c := colly.NewCollector(colly.Async(true))
	c.SetRequestTimeout(30 * time.Second)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 2, RandomDelay: 1 * time.Second})

	// Временные мапы для хранения новых данных
	tempScheduleA := make(map[string]*ScheduleItem)
	tempScheduleB := make(map[string]*ScheduleItem)

	// Регулярное выражение для парсинга URL расписаний
	re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?_korpus_([av])\.jpe?g$`)

	// Используем более точный селектор
	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		src := e.Attr("src")
		matches := re.FindStringSubmatch(src)
		if len(matches) != 5 {
			// log.Printf("Несовпадение RE для %s", src) // Для отладки
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
		fullURL := strings.TrimRight(baseSiteURL, "/") + path.Clean("/"+strings.TrimLeft(src, "/"))
		// Альтернатива: fullURL := strings.TrimRight(baseSiteURL, "/") + "/" + strings.TrimLeft(src, "/")

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

	// --- Логика загрузки изображений в Telegram (только для новых) ---
	log.Println("Начинаем загрузку новых изображений в Telegram...")
	uploadStart := time.Now()

	// Функция для загрузки и получения file_id
	uploadAndGetFileID := func(item *ScheduleItem) string {
		// Генерируем уникальный URL для загрузки (чтобы Telegram не использовал свой кэш)
		uploadURL := fmt.Sprintf("%s?upload_cache_bust_scrape=%d", item.URL, time.Now().UnixNano())
		log.Printf("DEBUG: Попытка загрузки %s (уникальный URL: %s) в чат администратора", item.URL, uploadURL)

		// Создаем фото для отправки в чат администратора (замените на реальный ID чата)
		// Для тестирования можно использовать ID тестового чата или ID бота
		adminChatID := int64(6436017953) // Предполагаем, что это ID чата для кэширования
		photo := tgbotapi.NewPhoto(adminChatID, tgbotapi.FileURL(uploadURL))
		photo.DisableNotification = true // Не уведомлять администратора о сообщении
		photo.Caption = fmt.Sprintf("[Кэширование] %s", item.URL) // Добавляем подпись для идентификации

		msg, err := bot.Send(photo)
		if err != nil {
			log.Printf("Ошибка загрузки фото в Telegram (для кэширования) %s: %v", item.URL, err)
			return "" // Возвращаем пустую строку в случае ошибки
		}

		log.Printf("DEBUG: Сообщение с фото отправлено в Telegram, MessageID: %d", msg.MessageID)
		log.Printf("DEBUG: Длина msg.Photo: %d", len(msg.Photo))

		// Получаем file_id из отправленного сообщения
		if len(msg.Photo) > 0 {
			// Берем фото с наилучшим качеством (обычно последний элемент)
			fileID := msg.Photo[len(msg.Photo)-1].FileID
			log.Printf("Загружено и закэшировано фото %s -> FileID: %s", item.URL, fileID)
			// Пытаемся удалить сообщение, чтобы не засорять чат администратора
			// _, delErr := bot.Send(tgbotapi.NewDeleteMessage(adminChatID, msg.MessageID))
			// if delErr != nil {
			// 	log.Printf("Предупреждение: Не удалось удалить сообщение %d: %v", msg.MessageID, delErr)
			// } else {
			// 	log.Printf("DEBUG: Сообщение %d удалено из чата администратора", msg.MessageID)
			// }
			return fileID
		} else {
			log.Printf("Ошибка: Сообщение с фото не содержит фото %s. Ответ от Telegram: %+v", item.URL, msg)
			return ""
		}
	}

	// --- Определяем и загружаем только новые изображения ---
	// Определяем новые элементы
	newItemsA := findNewItems(tempScheduleA, oldScheduleA)
	newItemsB := findNewItems(tempScheduleB, oldScheduleB)

	log.Printf("Найдено новых изображений для загрузки: A=%d, B=%d", len(newItemsA), len(newItemsB))

	// Асинхронная загрузка новых изображений для корпуса A
	var wgUpload sync.WaitGroup
	semaphoreA := make(chan struct{}, 5) // Ограничиваем кол-во одновременных горутин
	for _, item := range newItemsA {
		if item.FileID == "" { // Загружаем только если FileID еще не установлен
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaphoreA <- struct{}{}
				defer func() { <-semaphoreA }()
				it.FileID = uploadAndGetFileID(it)
				// time.Sleep(50 * time.Millisecond) // Задержка внутри горутины, можно уменьшить или убрать
			}(item)
		}
	}
	wgUpload.Wait()

	// Асинхронная загрузка новых изображений для корпуса B
	semaphoreB := make(chan struct{}, 5)
	for _, item := range newItemsB {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaphoreB <- struct{}{}
				defer func() { <-semaphoreB }()
				it.FileID = uploadAndGetFileID(it)
				// time.Sleep(50 * time.Millisecond)
			}(item)
		}
	}
	wgUpload.Wait() // Ждем завершения всех загрузок

	log.Printf("Загрузка новых изображений завершена за %v", time.Since(uploadStart))
	// --- Конец логики загрузки ---

	// --- Обновляем глобальные мапы и сохраняем кэш ---
	mu.Lock()
	// Очищаем старые данные
	scheduleA = make(map[string]*ScheduleItem)
	scheduleB = make(map[string]*ScheduleItem)
	// Копируем новые данные
	for k, v := range tempScheduleA {
		scheduleA[k] = v
	}
	for k, v := range tempScheduleB {
		scheduleB[k] = v
	}
	mu.Unlock()

	// --- Сохраняем новый кэш в БД ---
	ctxSave := context.Background()
	if saveErr := saveScheduleCache(ctxSave, scheduleA, scheduleB); saveErr != nil {
		log.Printf("Ошибка сохранения кэша после скрапинга: %v", saveErr)
		lastScrapeSuccess = false // Устанавливаем false при ошибке сохранения
	} else {
		log.Println("Новый кэш расписания успешно сохранен в БД.")
		// --- Отправляем уведомления, если сохранение прошло успешно ---
		// Первый запуск — уведомления не отправляем
		if len(oldScheduleA) == 0 && len(oldScheduleB) == 0 {
			log.Println("Первый запуск скрапинга: уведомления не отправляются")
		} else {
			notifyUsersAboutNewSchedule(scheduleA, scheduleB, oldScheduleA, oldScheduleB)
		}
		lastScrapeSuccess = true // Устанавливаем true, если дошли до этого места без критических ошибок
	}
	// --- Конец сохранения кэша и отправки уведомлений ---

	log.Printf("=== Скрапинг завершён за %s ===", time.Since(start))
}