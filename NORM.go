package main

import (
	"context"
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
)

const (
	webhookPath = "/webhook"
	baseSiteURL = "https://kmtko.my1.ru"
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
)

// ScheduleItem хранит информацию о расписании, включая file_id
type ScheduleItem struct {
	URL    string
	Date   time.Time
	FileID string // Поле для хранения file_id после загрузки
}

var (
	bot       *tgbotapi.BotAPI
	db        *pgxpool.Pool
	mu        sync.RWMutex
	scheduleA = make(map[string]*ScheduleItem) // Изменено на *ScheduleItem
	scheduleB = make(map[string]*ScheduleItem) // Изменено на *ScheduleItem
)

func main() {
	// Читаем переменные окружения
	telegramToken := os.Getenv("TELEGRAM_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN не задан. Установите переменную окружения.")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL не задан. Установите переменную окружения.")
	}

	// Инициализируем пул Postgres
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	// Рекомендуемые настройки (подберите по своему инстансу)
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

	// Простая миграция: создаём таблицу users, если её нет
	if err := ensureUsersTable(ctx); err != nil {
		log.Fatalf("ensureUsersTable: %v", err)
	}

	// Инициализируем Telegram Bot API
	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

	// Настройка вебхука
	externalURL := os.Getenv("RENDER_EXTERNAL_URL")
	if externalURL == "" {
		externalURL = "http://localhost:8080" // локально
		log.Println("RENDER_EXTERNAL_URL не найден, использую localhost (локально).")
	}
	webhookURL := externalURL + webhookPath

	wh, err := tgbotapi.NewWebhook(webhookURL)
	if err != nil {
		log.Fatalf("Ошибка при создании webhook: %v", err)
	}
	_, err = bot.Request(wh)
	if err != nil {
		log.Fatalf("Ошибка при установке вебхука: %v", err)
	}
	log.Printf("Вебхук установлен на: %s", webhookURL)

	// Запускаем pprof (локально доступен на :6060 если вы захотите изменить)
	go func() {
		// на Render порт 6060 скорее всего недоступен извне, но локально полезно
		log.Println("pprof слушает на :6060 (локально)")
		log.Fatal(http.ListenAndServe(":6060", nil))
	}()

	// Запускаем скрейпер в фоне
	go func() {
		for {
			scrapeImages()
			time.Sleep(30 * time.Minute) // интервал сканирования
		}
	}()

	// HTTP handlers
	http.HandleFunc(webhookPath, handleWebhook)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Bot running"))
			return
		}
		http.NotFound(w, r)
	})
	// health endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
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

// handleWebhook обрабатывает входящие webhook'и
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

// processUpdate обрабатывает одно обновление
func processUpdate(update tgbotapi.Update) {
	// Сохраняем/обновляем пользователя в БД
	if update.Message != nil && update.Message.From != nil {
		if err := saveUserFromUpdate(update); err != nil {
			log.Printf("saveUserFromUpdate err: %v", err)
		}
	}

	// Обработка команд/текстовых сообщений
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
			// Можно отправить подсказку
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Выберите кнопку на клавиатуре или напишите команду /start")
			bot.Send(msg)
		}
	}
}

// saveUserFromUpdate сохраняет/обновляет пользователя в таблице users
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

func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скрапере: %v", r)
		}
	}()
	start := time.Now()
	log.Println("Начало скрапинга...")

	c := colly.NewCollector(colly.Async(true))
	c.SetRequestTimeout(30 * time.Second)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 2, RandomDelay: 1 * time.Second})

	// Временные мапы для хранения новых данных
	tempScheduleA := make(map[string]*ScheduleItem)
	tempScheduleB := make(map[string]*ScheduleItem)

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		imageSrc := e.Attr("src")
		// Обновленное рег. выражение для захвата года
		re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?_korpus_([av])\.jpe?g$`)
		matches := re.FindStringSubmatch(imageSrc)
		// Проверка на 5 элементов теперь
		if len(matches) < 5 {
			log.Printf("Пропущено: URL не соответствует формату %s", imageSrc)
			return
		}
		day, _ := strconv.Atoi(matches[1])
		month, _ := strconv.Atoi(matches[2])
		yearStr := matches[3] // Может быть пустой
		corpus := matches[4]

		// Определяем год
		now := time.Now()
		year := now.Year() // По умолчанию
		if yearStr != "" {
			if parsedYear, err := strconv.Atoi(yearStr); err == nil {
				year = parsedYear
			}
		}

		// Создаем дату
		date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)

		if !strings.HasPrefix(imageSrc, "/") {
			imageSrc = "/" + imageSrc
		}
		// Используем TrimRight для корректного формирования URL
		fullURL := strings.TrimRight(baseSiteURL, "/") + imageSrc

		// Создаем элемент расписания
		item := &ScheduleItem{
			URL:  fullURL,
			Date: date,
			// FileID будет заполнен позже
		}

		// Добавляем во временные мапы
		if corpus == "a" {
			tempScheduleA[fullURL] = item
			log.Printf("Найдено фото корпуса А: %s (%02d.%02d.%d)", fullURL, day, month, year)
		} else { // corpus == "v" -> "b"
			tempScheduleB[fullURL] = item
			log.Printf("Найдено фото корпуса Б: %s (%02d.%02d.%d)", fullURL, day, month, year)
		}
	})

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s", r.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Printf("Ошибка скрапинга %s: %v", r.Request.URL.String(), err)
	})

	if err := c.Visit(strings.TrimRight(baseSiteURL, "/ ") + targetPath); err != nil { // Trim пробелов тоже
		log.Printf("Ошибка Visit: %v", err)
	}
	c.Wait()

	// --- НОВАЯ ЛОГИКА: Загрузка изображений в Telegram ---
	log.Println("Начинаем загрузку изображений в Telegram...")
	uploadStart := time.Now()

	// Получаем текущие данные для сравнения file_id
	mu.RLock()
	currentScheduleA := copyScheduleMap(scheduleA)
	currentScheduleB := copyScheduleMap(scheduleB)
	mu.RUnlock()

	// Функция для загрузки и обновления file_id
	uploadAndSetFileID := func(item *ScheduleItem, currentMap map[string]*ScheduleItem) {
		// Проверяем, есть ли уже file_id для этого URL в текущих данных
		if currentItem, ok := currentMap[item.URL]; ok && currentItem.FileID != "" {
			// Если URL и дата совпадают, используем старый file_id
			// Это предполагает, что изображение не изменилось
			// Более надежный способ - хешировать содержимое, но это сложно без скачивания
			// Для простоты будем считать, что если URL тот же, то изображение то же
			// В реальности, если файл на сервере изменился, но имя осталось, это не сработает.
			// Лучше всегда перезаливать, если важна актуальность.
			// item.FileID = currentItem.FileID
			// log.Printf("Используем существующий file_id для %s", item.URL)
		}

		// Всегда перезаливаем для гарантии актуальности
		// Генерируем уникальный URL для загрузки (чтобы Telegram не использовал свой кэш)
		uploadURL := fmt.Sprintf("%s?upload_cb=%d", item.URL, time.Now().UnixNano())
		photo := tgbotapi.NewPhoto(bot.Self.ID, tgbotapi.FileURL(uploadURL)) // Отправляем боту самому себе
		photo.DisableNotification = true // Не уведомлять бота о сообщении

		msg, err := bot.Send(photo)
		if err != nil {
			log.Printf("Ошибка загрузки фото в Telegram (для кэширования) %s: %v", item.URL, err)
			// Не прерываем, просто оставляем FileID пустым
			item.FileID = "" // Явно указываем
		} else {
			// Получаем file_id из отправленного сообщения
			if len(msg.Photo) > 0 {
				// Берем фото с наилучшим качеством (обычно последний элемент)
				item.FileID = msg.Photo[len(msg.Photo)-1].FileID
				log.Printf("Загружено и закэшировано фото %s -> FileID: %s", item.URL, item.FileID)
			} else {
				log.Printf("Ошибка: Сообщение с фото не содержит фото %s", item.URL)
				item.FileID = ""
			}
			// Удаляем сообщение у бота, чтобы не засорять его чат
			// Это не обязательно, но рекомендуется
			// bot.DeleteMessage(tgbotapi.NewDeleteMessage(bot.Self.ID, msg.MessageID))
			// Или просто игнорируем сообщения бота в processUpdate
		}
	}

	// Загружаем изображения для корпуса A
	for _, item := range tempScheduleA {
		uploadAndSetFileID(item, currentScheduleA)
		// Небольшая задержка между загрузками, чтобы не перегружать Telegram API
		time.Sleep(100 * time.Millisecond)
	}

	// Загружаем изображения для корпуса B
	for _, item := range tempScheduleB {
		uploadAndSetFileID(item, currentScheduleB)
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Загрузка изображений завершена за %v", time.Since(uploadStart))

	// --- Обновляем глобальные мапы ---
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

	log.Printf("Скрапинг и кэширование завершены, A=%d B=%d за %v", len(scheduleA), len(scheduleB), time.Since(start))
	notifyNewSchedule()
}

// copyScheduleMap копирует мапу *ScheduleItem
func copyScheduleMap(src map[string]*ScheduleItem) map[string]*ScheduleItem {
	dst := make(map[string]*ScheduleItem, len(src))
	for k, v := range src {
		// Копируем указатель, так как ScheduleItem не изменяется после создания scrapeImages
		// Если бы изменялся, нужно было бы копировать структуру
		dst[k] = v
	}
	return dst
}

// notifyNewSchedule уведомляет пользователей о новых расписаниях
func notifyNewSchedule() {
	// Реализация функции notifyNewSchedule (если требуется)
	// Пока оставим пустой или с минимальной реализацией
	log.Println("notifyNewSchedule called")
}

func sendSchedule(chatID int64, corpus string) {
	var scheduleMap map[string]*ScheduleItem // Тип изменен

	mu.RLock()
	// Копируем указатели на ScheduleItem
	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = copyScheduleMap(scheduleA)
	case "B":
		scheduleMap = copyScheduleMap(scheduleB)
	default:
		mu.RUnlock()
		bot.Send(tgbotapi.NewMessage(chatID, "Неизвестный корпус"))
		return
	}
	mu.RUnlock()

	if len(scheduleMap) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Расписание не найдено."))
		return
	}

	// Сортировка по дате
	type item struct {
		*ScheduleItem // Встраиваем для прямого доступа к полям
	}
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
		// Проверяем, есть ли file_id
		if it.FileID != "" {
			// Отправляем по file_id - быстро и без повторной загрузки
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileID(it.FileID))
			photo.Caption = caption
			msg = photo
			log.Printf("Отправка по FileID: %s -> chat %d (%s)", it.FileID, chatID, caption)
		} else {
			// Если file_id нет (например, ошибка загрузки), отправляем по URL с уникальным параметром
			log.Printf("FileID отсутствует, отправка по URL: %s -> chat %d", it.URL, chatID)
			uniqueURL := fmt.Sprintf("%s?send_cb=%d", it.URL, time.Now().UnixNano())
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
			photo.Caption = caption
			msg = photo
		}

		if _, err := bot.Send(msg); err != nil {
			logError := fmt.Sprintf("Ошибка отправки фото (URL: %s, FileID: %s): %v", it.URL, it.FileID, err)
			log.Printf(logError)
			// Можно отправить сообщение об ошибке пользователю
			// bot.Send(tgbotapi.NewMessage(chatID, "Ошибка при отправке расписания."))
		} else {
			log.Printf("Успешно отправлено фото -> chat %d (%s)", chatID, caption)
		}
	}
}

// sendStartMessage отправляет клавиатуру
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

// keepAlive для Render
func keepAlive(url string) {
	if url == "http://localhost:8080" {
		return
	}
	for {
		time.Sleep(3 * time.Minute)
		resp, err := http.Get(url + "/health")
		if err != nil {
			log.Printf("keepAlive err: %v", err)
			continue
		}
		_ = resp.Body.Close()
		log.Println("keepAlive ping ok")
	}
}

// getTimeOrZero helper
func getTimeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}