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

	// Создаем таблицы
	ensureTables(ctx)

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

	// pprof
	go func() { log.Fatal(http.ListenAndServe(":6060", nil)) }()

	// Скрейпер
	go func() {
		for {
			scrapeImages()
			time.Sleep(30 * time.Minute)
		}
	}()

	// Self-ping для Render
	go keepAlive(externalURL)

	// HTTP сервер
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

func ensureTables(ctx context.Context) {
	_, _ = db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS users (
	  id BIGINT PRIMARY KEY,
	  username TEXT,
	  first_seen TIMESTAMPTZ DEFAULT now(),
	  last_seen TIMESTAMPTZ DEFAULT now()
	);
	CREATE TABLE IF NOT EXISTS user_last_schedule (
	  user_id BIGINT PRIMARY KEY,
	  last_day_a DATE,
	  last_day_b DATE
	);
	`)
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
		go saveUser(update)
	}

	if update.Message != nil && update.Message.IsCommand() && update.Message.Command() == "start" {
		log.Printf("Пользователь %d нажал /start", update.Message.From.ID)
		sendStartMessage(update.Message.Chat.ID)
	} else if update.Message != nil && update.Message.Text != "" {
		switch update.Message.Text {
		case "Расписание А":
			log.Printf("Пользователь %d запросил Расписание А", update.Message.From.ID)
			sendSchedule(update.Message.Chat.ID, "A")
		case "Расписание Б":
			log.Printf("Пользователь %d запросил Расписание Б", update.Message.From.ID)
			sendSchedule(update.Message.Chat.ID, "B")
		case "Поддержка и предложения":
			log.Printf("Пользователь %d запросил поддержку", update.Message.From.ID)
			sendSupportMessage(update.Message.Chat.ID)
		default:
			log.Printf("Пользователь %d написал неизвестную команду: %s", update.Message.From.ID, update.Message.Text)
			bot.Send(tgbotapi.NewMessage(update.Message.Chat.ID, "Выберите кнопку или /start"))
		}
	}
}

func saveUser(update tgbotapi.Update) {
	if update.Message == nil || update.Message.From == nil {
		return
	}
	ctx := context.Background()
	userId := int64(update.Message.From.ID)
	username := update.Message.From.UserName
	_, _ = db.Exec(ctx, `
	INSERT INTO users (id, username, first_seen, last_seen)
	VALUES ($1,$2,now(),now())
	ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, last_seen=now();
	`, userId, username)
	// создаём запись для last_schedule, если нет
	_, _ = db.Exec(ctx, `
	INSERT INTO user_last_schedule(user_id)
	VALUES($1)
	ON CONFLICT DO NOTHING;
	`, userId)
	log.Printf("User saved: %d (%s)", userId, username)
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

	mu.Lock()
	scheduleA = make(map[string]time.Time)
	scheduleB = make(map[string]time.Time)
	mu.Unlock()

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		imageSrc := e.Attr("src")
		re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.\d{4})?_korpus_([av])\.jpe?g$`)
		matches := re.FindStringSubmatch(imageSrc)
		if len(matches) != 4 {
			log.Printf("Пропущено: URL не соответствует формату %s", imageSrc)
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
			log.Printf("Найдено фото корпуса А: %s (%02d.%02d.%d)", fullURL, day, month, now.Year())
		} else {
			scheduleB[fullURL] = date
			log.Printf("Найдено фото корпуса Б: %s (%02d.%02d.%d)", fullURL, day, month, now.Year())
		}
		mu.Unlock()
	})

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Visiting %s", r.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Printf("Ошибка скрапинга %s: %v", r.Request.URL.String(), err)
	})

	if err := c.Visit(baseSiteURL + targetPath); err != nil {
		log.Printf("Ошибка Visit: %v", err)
	}
	c.Wait()
	log.Printf("Скрапинг завершён, A=%d B=%d за %v", len(scheduleA), len(scheduleB), time.Since(start))
	notifyNewSchedule()
}

func notifyNewSchedule() {
	ctx := context.Background()

	mu.RLock()
	scheduleACopy := copyMap(scheduleA)
	scheduleBCopy := copyMap(scheduleB)
	mu.RUnlock()

	var maxA, maxB time.Time
	for _, d := range scheduleACopy {
		if d.After(maxA) {
			maxA = d
		}
	}
	for _, d := range scheduleBCopy {
		if d.After(maxB) {
			maxB = d
		}
	}

	rows, err := db.Query(ctx, "SELECT user_id, last_day_a, last_day_b FROM user_last_schedule")
	if err != nil {
		log.Printf("Ошибка запроса user_last_schedule: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var userID int64
		var lastA, lastB *time.Time
		if err := rows.Scan(&userID, &lastA, &lastB); err != nil {
			log.Printf("Ошибка Scan: %v", err)
			continue
		}

		if maxA.After(getTimeOrZero(lastA)) {
			msgText := fmt.Sprintf("Появилось новое расписание корпуса А: %02d.%02d.%d", maxA.Day(), maxA.Month(), maxA.Year())
			bot.Send(tgbotapi.NewMessage(userID, msgText))
			db.Exec(ctx, "UPDATE user_last_schedule SET last_day_a=$1 WHERE user_id=$2", maxA, userID)
			log.Printf("Уведомление A отправлено пользователю %d", userID)
		}
		if maxB.After(getTimeOrZero(lastB)) {
			msgText := fmt.Sprintf("Появилось новое расписание корпуса Б: %02d.%02d.%d", maxB.Day(), maxB.Month(), maxB.Year())
			bot.Send(tgbotapi.NewMessage(userID, msgText))
			db.Exec(ctx, "UPDATE user_last_schedule SET last_day_b=$1 WHERE user_id=$2", maxB, userID)
			log.Printf("Уведомление B отправлено пользователю %d", userID)
		}
	}
}

func getTimeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
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
		weekday := weekdays[it.date.Weekday()]
		caption := fmt.Sprintf("%s — %02d.%02d.%d", weekday, it.date.Day(), it.date.Month(), it.date.Year())
		uniqueURL := fmt.Sprintf("%s?cb=%d", it.url, time.Now().UnixNano())
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
		photo.Caption = caption
		if _, err := bot.Send(photo); err != nil {
			log.Printf("Ошибка отправки фото %s: %v", it.url, err)
		} else {
			log.Printf("Отправлено фото %s -> chat %d (%s)", it.url, chatID, caption)
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
