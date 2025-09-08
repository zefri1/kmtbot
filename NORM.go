package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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
)

var (
	bot       *tgbotapi.BotAPI
	db        *pgxpool.Pool
	mu        sync.RWMutex
	scheduleA = make(map[string]time.Time) // URL -> date (day+month, year may be ignored)
	scheduleB = make(map[string]time.Time)
)

func main() {
	rand.Seed(time.Now().UnixNano())

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

	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

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

	go func() {
		log.Println("pprof слушает на :6060 (локально)")
		log.Fatal(http.ListenAndServe(":6060", nil))
	}()

	// Скрейпер
	go func() {
		for {
			scrapeImages()
			time.Sleep(30 * time.Minute)
		}
	}()

	// keep-alive (3мин ±30с)
	go keepAlive(externalURL)

	http.HandleFunc(webhookPath, handleWebhook)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Bot running"))
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
		w.Write([]byte("ok"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("HTTP-сервер стартует на :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func keepAlive(externalURL string) {
	client := &http.Client{Timeout: 5 * time.Second}
	base := 3 * time.Minute
	for {
		j := time.Duration(rand.Intn(61)-30) * time.Second
		interval := base + j
		url := strings.TrimRight(externalURL, "/") + "/health"

		resp, err := client.Get(url)
		if err != nil {
			log.Printf("keepAlive: ping error: %v", err)
		} else {
			resp.Body.Close()
			log.Printf("keepAlive: ping %s -> %d (next in %v)", url, resp.StatusCode, interval)
		}
		time.Sleep(interval)
	}
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
			bot.Send(msg)
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

// --- Скрейпер ---
const (
	baseSiteURL = "https://kmtko.my1.ru"
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
)

func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скраperе: %v", r)
		}
	}()

	start := time.Now()
	log.Println("Начинаем скрапинг...")

	c := colly.NewCollector(colly.Async(true))
	c.SetRequestTimeout(30 * time.Second)
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 4,
		RandomDelay: 0,
	})

	mu.Lock()
	scheduleA = make(map[string]time.Time)
	scheduleB = make(map[string]time.Time)
	mu.Unlock()
	log.Println("Старые данные расписаний очищены")

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

	if err := c.Visit(baseSiteURL + targetPath); err != nil {
		log.Printf("Ошибка Visit: %v", err)
		return
	}
	c.Wait()

	mu.RLock()
	log.Printf("Скрапинг завершён. A=%d B=%d. Занял: %v", len(scheduleA), len(scheduleB), time.Since(start))
	mu.RUnlock()
}

func copyMap(src map[string]time.Time) map[string]time.Time {
	dst := make(map[string]time.Time, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

var weekdayRus = map[time.Weekday]string{
	time.Monday:    "Понедельник",
	time.Tuesday:   "Вторник",
	time.Wednesday: "Среда",
	time.Thursday:  "Четверг",
	time.Friday:    "Пятница",
	time.Saturday:  "Суббота",
	time.Sunday:    "Воскресенье",
}

// sendSchedule: отправляет фото по дате (старые -> новые).
// Первое фото отправляется сразу, между сообщениями минимальная пауза (~150ms).
// Подпись: "Понедельник — 02.03.2025"
func sendSchedule(chatID int64, corpus string) {
	var scheduleMap map[string]time.Time
	var corpusName string

	switch strings.ToUpper(corpus) {
	case "A":
		corpusName = "корпуса А"
	case "B":
		corpusName = "корпуса Б"
	default:
		msg := tgbotapi.NewMessage(chatID, "Неизвестный корпус.")
		bot.Send(msg)
		return
	}

	// Копируем под мьютексом
	mu.RLock()
	if strings.ToUpper(corpus) == "A" {
		scheduleMap = copyMap(scheduleA)
	} else {
		scheduleMap = copyMap(scheduleB)
	}
	mu.RUnlock()

	if len(scheduleMap) == 0 {
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("Расписание для %s не найдено.", corpusName))
		bot.Send(msg)
		return
	}

	// Собираем и сортируем по дате (old->new)
	type item struct {
		url  string
		date time.Time
	}
	items := make([]item, 0, len(scheduleMap))
	for u, d := range scheduleMap {
		items = append(items, item{url: u, date: d})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].date.Equal(items[j].date) {
			return items[i].url < items[j].url
		}
		return items[i].date.Before(items[j].date)
	})

	// Интервал между отправками: примерно 150ms ±20ms
	baseInterval := 150 * time.Millisecond
	jitterRange := 40 * time.Millisecond // ±20ms

	for idx, it := range items {
		// Формируем подпись с днём недели для 2025 года
		weekdayDate := time.Date(2025, it.date.Month(), it.date.Day(), 0, 0, 0, 0, time.Local)
		wname := weekdayRus[weekdayDate.Weekday()]
		caption := fmt.Sprintf("%s — %02d.%02d.2025", wname, it.date.Day(), int(it.date.Month()))

		uniqueURL := fmt.Sprintf("%s?cb=%d", it.url, time.Now().UnixNano())
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
		photo.Caption = caption

		if _, err := bot.Send(photo); err != nil {
			log.Printf("Ошибка отправки фото %s: %v", it.url, err)
		} else {
			log.Printf("Отправлено фото %s -> chat %d (%s)", it.url, chatID, caption)
		}

		// Никакой паузы после последнего сообщения
		if idx < len(items)-1 {
			j := time.Duration(rand.Int63n(int64(jitterRange))) - jitterRange/2
			interval := baseInterval + j
			if interval < 50*time.Millisecond {
				interval = 50 * time.Millisecond
			}
			time.Sleep(interval)
		}
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
