package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	scheduleA = make(map[string]time.Time) // URL -> date
	scheduleB = make(map[string]time.Time)
)

var weekdayRus = map[time.Weekday]string{
	time.Monday:    "Понедельник",
	time.Tuesday:   "Вторник",
	time.Wednesday: "Среда",
	time.Thursday:  "Четверг",
	time.Friday:    "Пятница",
	time.Saturday:  "Суббота",
	time.Sunday:    "Воскресенье",
}

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

	// Postgres pool
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

	// Telegram bot
	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

	// Webhook
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

	// Скрейпер (фон)
	go func() {
		for {
			scrapeImages()
			time.Sleep(30 * time.Minute)
		}
	}()

	// self-ping (keep-alive) 3 мин ±30s
	go keepAlive(externalURL)

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

// keepAlive периодически пингует /health чтобы держать инстанс "тёплым"
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

// processUpdate — сохраняет пользователя в фоне и немедленно обрабатывает команду/текст
func processUpdate(update tgbotapi.Update) {
	// сохраняем пользователя в фоне — чтобы DB не блокировала отправку
	if update.Message != nil && update.Message.From != nil {
		// передаём копию update в горутину (передача по значению)
		go func(up tgbotapi.Update) {
			if err := saveUserFromUpdate(up); err != nil {
				log.Printf("saveUserFromUpdate err: %v", err)
			}
		}(update)
	}

	// Обработка команд/текстовых сообщений (отправляем немедленно)
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

// saveUserFromUpdate — синхронная функция сохранения (вызывается в фоне из processUpdate)
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

// fetchImageBytes — параллельный фетч картинки в память (timeout задаётся)
func fetchImageBytes(ctx context.Context, url string, timeout time.Duration) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("bad status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// sendSchedule — параллельная предзагрузка первых двух фото, первое и второе отправляются как можно быстрее.
// подпись: "Понедельник — 08.09.2025"
func sendSchedule(chatID int64, corpus string) {
	var scheduleMap map[string]time.Time
	var corpusName string

	switch strings.ToUpper(corpus) {
	case "A":
		corpusName = "корпуса А"
	case "B":
		corpusName = "корпуса Б"
	default:
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, "Неизвестный корпус."))
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
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("Расписание для %s не найдено.", corpusName)))
		return
	}

	// Собираем и сортируем по дате (old -> new)
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

	// helper для подписи (2025)
	buildCaption := func(d time.Time) string {
		wd := time.Date(2025, d.Month(), d.Day(), 0, 0, 0, 0, time.Local).Weekday()
		return fmt.Sprintf("%s — %02d.%02d.2025", weekdayRus[wd], d.Day(), int(d.Month()))
	}

	// подготовка каналов для первых двух fetch'ей
	type fetchRes struct {
		data []byte
		err  error
	}
	var ch0, ch1 chan fetchRes
	ctxFetch := context.Background()
	fetchTimeout := 6 * time.Second

	if len(items) >= 1 {
		ch0 = make(chan fetchRes, 1)
		go func(url string) {
			start := time.Now()
			b, err := fetchImageBytes(ctxFetch, url, fetchTimeout)
			log.Printf("fetch first finished for %s (err=%v) in %v", url, err, time.Since(start))
			ch0 <- fetchRes{data: b, err: err}
		}(items[0].url)
	}
	if len(items) >= 2 {
		ch1 = make(chan fetchRes, 1)
		go func(url string) {
			start := time.Now()
			b, err := fetchImageBytes(ctxFetch, url, fetchTimeout)
			log.Printf("fetch second finished for %s (err=%v) in %v", url, err, time.Since(start))
			ch1 <- fetchRes{data: b, err: err}
		}(items[1].url)
	}

	// --- First: ждём результат первого фетча (с timeout на самом fetch) и отправляем ---
	if len(items) >= 1 {
		it := items[0]
		caption := buildCaption(it.date)

		var r0 fetchRes
		if ch0 != nil {
			r0 = <-ch0
		} else {
			r0 = fetchRes{data: nil, err: fmt.Errorf("no fetch")}
		}

		if r0.err == nil && len(r0.data) > 0 {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileBytes{Name: "schedule1.jpg", Bytes: r0.data})
			photo.Caption = caption
			if _, err := bot.Send(photo); err != nil {
				log.Printf("Ошибка отправки первого фото (upload) %s: %v", it.url, err)
				// fallback по URL
				photoURL := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(it.url))
				photoURL.Caption = caption
				_, _ = bot.Send(photoURL)
			} else {
				log.Printf("Отправлено первое фото (upload) %s -> chat %d (%s)", it.url, chatID, caption)
			}
		} else {
			// fallback: отправляем по URL
			photoURL := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(it.url))
			photoURL.Caption = caption
			if _, err := bot.Send(photoURL); err != nil {
				log.Printf("Ошибка отправки первого фото по URL %s: %v", it.url, err)
			} else {
				log.Printf("Отправлено первое фото (url) %s -> chat %d (%s)", it.url, chatID, caption)
			}
		}
	}

	// --- Second: ждём результат второго фетча и отправляем как можно быстрее ---
	if len(items) >= 2 {
		it := items[1]
		caption := buildCaption(it.date)

		var r1 fetchRes
		if ch1 != nil {
			r1 = <-ch1
		} else {
			r1 = fetchRes{data: nil, err: fmt.Errorf("no fetch")}
		}

		if r1.err == nil && len(r1.data) > 0 {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileBytes{Name: "schedule2.jpg", Bytes: r1.data})
			photo.Caption = caption
			if _, err := bot.Send(photo); err != nil {
				log.Printf("Ошибка отправки второго фото (upload) %s: %v", it.url, err)
				photoURL := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(it.url))
				photoURL.Caption = caption
				_, _ = bot.Send(photoURL)
			} else {
				log.Printf("Отправлено второе фото (upload) %s -> chat %d (%s)", it.url, chatID, caption)
			}
		} else {
			photoURL := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(it.url))
			photoURL.Caption = caption
			if _, err := bot.Send(photoURL); err != nil {
				log.Printf("Ошибка отправки второго фото по URL %s: %v", it.url, err)
			} else {
				log.Printf("Отправлено второе фото (url) %s -> chat %d (%s)", it.url, chatID, caption)
			}
		}
	}

	// --- Оставшиеся фото (3+) отправляем в фоне с минимальной паузой, чтобы не блокировать обработчик ---
	if len(items) > 2 {
		rest := make([]item, 0, len(items)-2)
		for i := 2; i < len(items); i++ {
			rest = append(rest, items[i])
		}
		go func(toSend []item) {
			baseInterval := 40 * time.Millisecond
			jitterRange := 20 * time.Millisecond
			for idx, it := range toSend {
				wd := time.Date(2025, it.date.Month(), it.date.Day(), 0, 0, 0, 0, time.Local).Weekday()
				caption := fmt.Sprintf("%s — %02d.%02d.2025", weekdayRus[wd], it.date.Day(), int(it.date.Month()))
				photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(it.url))
				photo.Caption = caption
				if _, err := bot.Send(photo); err != nil {
					log.Printf("Ошибка отправки фото %s: %v", it.url, err)
				}
				// пауза перед следующим
				if idx < len(toSend)-1 {
					j := time.Duration(rand.Int63n(int64(jitterRange))) - jitterRange/2
					interval := baseInterval + j
					if interval < 10*time.Millisecond {
						interval = 10 * time.Millisecond
					}
					time.Sleep(interval)
				}
			}
		}(rest)
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
	_, _ = bot.Send(msg)
}

func sendSupportMessage(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "По вопросам поддержки: @podkmt")
	_, _ = bot.Send(msg)
}
