package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // включаем pprof на /debug/pprof/ (локально)
	"os"
	"regexp"
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
)

// глобальные переменные
var (
	bot       *tgbotapi.BotAPI
	db        *pgxpool.Pool
	mu        sync.RWMutex
	scheduleA = make(map[string]time.Time) // URL -> date
	scheduleB = make(map[string]time.Time)
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
			time.Sleep(10 * time.Minute) // интервал сканирования
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

	// Обработка команд/текстовых сообщений (копируйте вашу логику)
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

// --- Скрейпер (взято и адаптировано из вашего примера) ---
const (
	baseSiteURL = "https://kmtko.my1.ru" // поменяйте, если нужно
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
)

func scrapeImages() {
	log.Println("Начинаем скрапинг...")

	// очищаем старые данные
	mu.Lock()
	scheduleA = make(map[string]time.Time)
	scheduleB = make(map[string]time.Time)
	mu.Unlock()
	log.Println("Старые данные расписаний очищены")

	c := colly.NewCollector()

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		imageSrc := e.Attr("src")
		altText := e.Attr("alt")
		log.Printf("Найдено изображение: src=%s alt=%s", imageSrc, altText)

		re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.\d{4})?_korpus_([av])\.jpe?g$`)
		matches := re.FindStringSubmatch(imageSrc)
		if len(matches) != 4 {
			log.Printf("URL не соответствует формату: %s", imageSrc)
			return
		}

		dayStr := matches[1]
		monthStr := matches[2]
		corpusLetter := strings.ToLower(matches[3])

		day, errD := strconv.Atoi(dayStr)
		month, errM := strconv.Atoi(monthStr)
		if errD != nil || errM != nil {
			log.Printf("Ошибка парсинга даты: %v %v", errD, errM)
			return
		}

		now := time.Now()
		loc := time.Local
		imageDate := time.Date(now.Year(), time.Month(month), day, 0, 0, 0, 0, loc)

		if !strings.HasPrefix(imageSrc, "/") {
			imageSrc = "/" + imageSrc
		}
		fullURL := strings.TrimRight(baseSiteURL, "/") + imageSrc

		mu.Lock()
		if corpusLetter == "a" {
			scheduleA[fullURL] = imageDate
			log.Printf("Добавлено расписание A: %s (%s)", fullURL, imageDate.Format("2006-01-02"))
		} else if corpusLetter == "v" {
			scheduleB[fullURL] = imageDate
			log.Printf("Добавлено расписание B: %s (%s)", fullURL, imageDate.Format("2006-01-02"))
		}
		mu.Unlock()
	})

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s", r.URL.String())
	})
	c.OnError(func(r *colly.Response, err error) {
		log.Printf("Colly error: %v (url=%s)", err, r.Request.URL.String())
	})

	err := c.Visit(baseSiteURL + targetPath)
	if err != nil {
		log.Printf("Ошибка Visit: %v", err)
		return
	}

	mu.RLock()
	log.Printf("Скрапинг завершён. A=%d B=%d", len(scheduleA), len(scheduleB))
	mu.RUnlock()
}

// sendSchedule отправляет все найденные изображения указанного корпуса
func sendSchedule(chatID int64, corpus string) {
	var scheduleMap map[string]time.Time
	var corpusName string

	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = scheduleA
		corpusName = "корпуса А"
	case "B":
		scheduleMap = scheduleB
		corpusName = "корпуса Б"
	default:
		msg := tgbotapi.NewMessage(chatID, "Неизвестный корпус.")
		bot.Send(msg)
		return
	}

	mu.RLock()
	if len(scheduleMap) == 0 {
		mu.RUnlock()
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("Расписание для %s не найдено.", corpusName))
		bot.Send(msg)
		return
	}

	// отправляем (по одному фото на сообщение)
	for url, date := range scheduleMap {
		// Форматируем дату для описания
		caption := fmt.Sprintf("Расписание на %02d.%02d (примерно)", date.Day(), date.Month())
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(url))
		photo.Caption = caption
		if _, err := bot.Send(photo); err != nil {
			log.Printf("Ошибка отправки фото %s: %v", url, err)
		} else {
			log.Printf("Отправлено фото %s -> chat %d", url, chatID)
		}
	}
	mu.RUnlock()
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
