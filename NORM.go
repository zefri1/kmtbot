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
)

var (
	bot       *tgbotapi.BotAPI
	db        *pgxpool.Pool
	mu        sync.RWMutex
	scheduleA = make(map[string]time.Time)
	scheduleB = make(map[string]time.Time)
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
	log.Println("Подключились к Postgres")

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
	}
	webhookURL := externalURL + webhookPath

	wh, err := tgbotapi.NewWebhook(webhookURL)
	if err != nil {
		log.Fatalf("Ошибка при создании webhook: %v", err)
	}
	if _, err = bot.Request(wh); err != nil {
		log.Fatalf("Ошибка при установке вебхука: %v", err)
	}
	log.Printf("Вебхук установлен: %s", webhookURL)

	go func() {
		log.Println("pprof слушает на :6060 (локально)")
		log.Fatal(http.ListenAndServe(":6060", nil))
	}()

	go func() {
		for {
			scrapeImages()
			time.Sleep(30 * time.Minute)
		}
	}()

	// self-ping, чтобы Render не засыпал
	go keepAlive(externalURL)

	http.HandleFunc(webhookPath, handleWebhook)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Bot running"))
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
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	go processUpdate(update)
	w.WriteHeader(http.StatusOK)
}

func processUpdate(update tgbotapi.Update) {
	if update.Message != nil && update.Message.From != nil {
		go func(up tgbotapi.Update) {
			if err := saveUserFromUpdate(up); err != nil {
				log.Printf("saveUserFromUpdate err: %v", err)
			}
		}(update)
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
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Выберите кнопку или /start")
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
	return err
}

const (
	baseSiteURL = "https://kmtko.my1.ru"
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
)

func scrapeImages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Паника в скрапере: %v", r)
		}
	}()
	start := time.Now()
	c := colly.NewCollector(colly.Async(true))
	c.SetRequestTimeout(30 * time.Second)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 2, RandomDelay: 1 * time.Second})

	mu.Lock()
	scheduleA = make(map[string]time.Time)
	scheduleB = make(map[string]time.Time)
	mu.Unlock()

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		imageSrc := e.Attr("src")
		re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.\d{4})?_korpus_([av])\.jpe?g$`)
		matches := re.FindStringSubmatch(imageSrc)
		if len(matches) != 4 {
			return
		}
		day, _ := strconv.Atoi(matches[1])
		month, _ := strconv.Atoi(matches[2])
		corpus := matches[3]
		now := time.Now()
		date := time.Date(now.Year(), time.Month(month), day, 0, 0, 0, 0, time.Local)
		if !strings.HasPrefix(imageSrc, "/") {
			imageSrc = "/" + imageSrc
		}
		fullURL := strings.TrimRight(baseSiteURL, "/") + imageSrc
		mu.Lock()
		if corpus == "a" {
			scheduleA[fullURL] = date
		} else {
			scheduleB[fullURL] = date
		}
		mu.Unlock()
	})
	c.Visit(baseSiteURL + targetPath)
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

func sendSchedule(chatID int64, corpus string) {
	var scheduleMap map[string]time.Time
	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = copyMap(scheduleA)
	case "B":
		scheduleMap = copyMap(scheduleB)
	default:
		bot.Send(tgbotapi.NewMessage(chatID, "Неизвестный корпус"))
		return
	}
	if len(scheduleMap) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "Расписание не найдено."))
		return
	}
	type item struct {
		url  string
		date time.Time
	}
	var items []item
	for u, d := range scheduleMap {
		items = append(items, item{u, d})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].date.Before(items[j].date)
	})

	for _, it := range items {
		weekday := it.date.Weekday().String()
		caption := fmt.Sprintf("%s — %02d.%02d.%d", weekday, it.date.Day(), it.date.Month(), it.date.Year())
		uniqueURL := fmt.Sprintf("%s?cb=%d", it.url, time.Now().UnixNano())
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
		photo.Caption = caption
		if _, err := bot.Send(photo); err != nil {
			log.Printf("Ошибка отправки %s: %v", it.url, err)
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
	bot.Send(msg)
}

func sendSupportMessage(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "По вопросам поддержки: @podkmt")
	bot.Send(msg)
}
