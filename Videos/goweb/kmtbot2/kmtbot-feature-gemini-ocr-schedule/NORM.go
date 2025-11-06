package main

import (
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

const (
	webhookPath = "/webhook"
	baseSiteURL = "https://kmtko.my1.ru"
	targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
	adminCommandStats = "/stats"
	// Лимиты для чат-бота
	dailyRequestLimit = 10
	limitResetHour    = 3 // 3:00 MSK
	// Gemini API
	geminiModel = "gemini-2.5-flash"
	geminiURL   = "https://generativelanguage.googleapis.com/v1beta/models/" + geminiModel + ":generateContent"
	// Глобальный лимит Gemini API - 9 запросов в минуту
	geminiRPM = 9
	// Для рассылки
	broadcastRateLimit = 30 // ~30 сообщений в секунду
	broadcastBatchSize = 30
	// Промпт для OCR расписания
	geminiSchedulePrompt = `Ты — помощник для извлечения расписания занятий из таблицы.

СТРУКТУРА ИЗОБРАЖЕНИЯ:
- Таблица с расписанием техникума на один день
- Столбцы: Группы | 1 пара | 2 пара | 3 пара | 4 пара | 5 пара | 6 пара
- В столбце "Группы": коды групп (формат XX-XXX-X, например: 25-СА-1, 24-ТМ-1)
- В ячейках пар: дисциплина, аудитория "Каб.XXX", ФИО преподавателя

ЗАДАЧА:
Извлеки ВСЕ занятия и верни JSON в точном формате ниже.

ФОРМАТ ОТВЕТА:
{
  "schedule": [
    {
      "group": "25-СА-1",
      "pair_number": 1,
      "subject": "Математика",
      "teacher": "Курсанова Е.В.",
      "room": "Каб.303",
      "confidence": 0.95
    }
  ],
  "metadata": {
    "date": "07.11.2025",
    "corpus": "A",
    "total_entries": 120,
    "average_confidence": 0.87
  }
}

ПРАВИЛА:
1. group: Код группы из столбца "Группы"
2. pair_number: Номер пары от 1 до 6
3. subject: Название дисциплины БЕЗ аудитории и преподавателя
4. teacher: ФИО преподавателя (формат "Фамилия И.О."), если нет — null
5. room: Аудитория в формате "Каб.XXX", если нет — null
6. confidence: Уверенность от 0.0 до 1.0 (0.9-1.0: чёткий текст, 0.7-0.9: небольшие сомнения, 0.5-0.7: размыт, <0.5: ненадёжно)
7. Пустые ячейки ("-" или пробелы) — НЕ добавляй
8. Если ячейка содержит несколько строк, объедини их
9. metadata.date: дата из заголовка таблицы
10. metadata.corpus: "A" или "B"

ЕСЛИ ТАБЛИЦА НЕЧИТАЕМА:
{"schedule": [], "metadata": {"error": "Не удалось распознать расписание"}}

Верни ТОЛЬКО JSON, без комментариев.`
)

// Белый список для безлимитного доступа
var unlimitedUserIDs = map[int64]bool{
	535803934: true, // админ без лимитов
}

// User struct
type User struct {
	ID              int64
	Username        sql.NullString
	FirstSeen       time.Time
	LastSeen        time.Time
	PreferredCorpus sql.NullString
	ChatRequests    int
	LastResetDate   time.Time
}

type ScheduleItem struct {
	URL        string
	Date       time.Time
	ActualDate time.Time  // Скорректированная дата
	FileID     string
	IsValidURL bool       // Флаг валидности URL
}

// Gemini API structures
type InlineData struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"` // base64
}

type GPart struct {
	Text       string      `json:"text,omitempty"`
	InlineData *InlineData `json:"inline_data,omitempty"`
}

type GContent struct {
	Role  string  `json:"role,omitempty"`
	Parts []GPart `json:"parts"`
}

type GRequest struct {
	SystemInstruction *GContent  `json:"system_instruction,omitempty"`
	Contents          []GContent `json:"contents"`
}

type GCandidate struct {
	Content GContent `json:"content"`
}

type GResponse struct {
	Candidates []GCandidate `json:"candidates"`
	Error      *struct{Message string `json:"message"`} `json:"error,omitempty"`
}

// Gemini OCR structures
type GeminiScheduleResponse struct {
	Schedule []ScheduleEntry  `json:"schedule"`
	Metadata ScheduleMetadata `json:"metadata"`
}

type ScheduleEntry struct {
	Group      string   `json:"group"`
	PairNumber int      `json:"pair_number"`
	Subject    string   `json:"subject"`
	Teacher    *string  `json:"teacher"`
	Room       *string  `json:"room"`
	Confidence float64  `json:"confidence"`
}

type ScheduleMetadata struct {
	Date              string  `json:"date"`
	Corpus            string  `json:"corpus"`
	TotalEntries      int     `json:"total_entries"`
	AverageConfidence float64 `json:"average_confidence"`
	Error             string  `json:"error,omitempty"`
}

var (
	bot               *tgbotapi.BotAPI
	db                *pgxpool.Pool
	mu                sync.RWMutex
	scheduleA         = make(map[string]*ScheduleItem)
	scheduleB         = make(map[string]*ScheduleItem)
	lastScrapeSuccess = false
	adminUserID int64 = 535803934 // замените на реальный
	// Состояния пользователей для чат-бота
	userStates        = make(map[int64]string)
	userStatesMutex   sync.RWMutex
	// Глобальный лимитер для Gemini API
	geminiLimiter     chan struct{}
)

// Инициализация глобального лимитера для Gemini API
func initGeminiLimiter() {
	geminiLimiter = make(chan struct{}, geminiRPM)
	// Заполняем канал токенами
	for i := 0; i < geminiRPM; i++ {
		geminiLimiter <- struct{}{}
	}
	
	// Горутина для пополнения токенов каждые 60/geminiRPM секунд
	go func() {
		ticker := time.NewTicker(time.Duration(60/geminiRPM) * time.Second) // ~6.67 секунд между токенами
		defer ticker.Stop()
		
		for range ticker.C {
			select {
			case geminiLimiter <- struct{}{}:
				// Токен добавлен
			default:
				// Канал полон, пропускаем
			}
		}
	}()
}

// Умная коррекция даты
func smartDateCorrection(urlDate time.Time, fileName string) time.Time {
	now := time.Now()
	
	// Извлекаем день и месяц из URL
	day := urlDate.Day()
	month := int(urlDate.Month())
	year := urlDate.Year()
	
	log.Printf("Анализируем дату из URL: %02d.%02d.%d", day, month, year)
	
	// Если URL содержит дату из далекого прошлого (более 30 дней назад)
	daysDiff := now.Sub(urlDate).Hours() / 24
	if daysDiff > 30 {
		log.Printf("Обнаружена устаревшая дата (разница %.0f дней), корректируем...", daysDiff)
		
		// Пробуем заменить месяц на текущий, оставляя день
		correctedDate := time.Date(now.Year(), now.Month(), day, 0, 0, 0, 0, time.Local)
		
		// Если день больше количества дней в текущем месяце, берем последний день месяца
		lastDayOfMonth := time.Date(now.Year(), now.Month()+1, 0, 0, 0, 0, 0, time.Local).Day()
		if day > lastDayOfMonth {
			correctedDate = time.Date(now.Year(), now.Month(), lastDayOfMonth, 0, 0, 0, 0, time.Local)
			log.Printf("День %d больше чем дней в месяце, используем %d", day, lastDayOfMonth)
		}
		
		// Если скорректированная дата все еще в прошлом, пробуем следующий месяц
		if now.Sub(correctedDate).Hours() > 24 {
			nextMonth := now.AddDate(0, 1, 0)
			correctedDate = time.Date(nextMonth.Year(), nextMonth.Month(), day, 0, 0, 0, 0, time.Local)
			log.Printf("Текущий месяц в прошлом, пробуем следующий месяц")
			
			// Проверяем на валидность дня в следующем месяце
			lastDayOfNextMonth := time.Date(nextMonth.Year(), nextMonth.Month()+1, 0, 0, 0, 0, 0, time.Local).Day()
			if day > lastDayOfNextMonth {
				correctedDate = time.Date(nextMonth.Year(), nextMonth.Month(), lastDayOfNextMonth, 0, 0, 0, 0, time.Local)
			}
		}
		
		log.Printf("Дата скорректирована: %s -> %s", 
			urlDate.Format("02.01.2006"), 
			correctedDate.Format("02.01.2006"))
		
		return correctedDate
	}
	
	return urlDate
}

// Типы для очередей отправки
type sendResponse struct {
	Message *tgbotapi.Message
	Err     error
}

type sendTask struct {
	Chattable tgbotapi.Chattable
	Resp      chan sendResponse
}

var (
	userSendQueue   chan sendTask
	uploaderQueue   chan sendTask
)

func startSenderPool(workers int, queue chan sendTask) {
	for i := 0; i < workers; i++ {
		go func(id int) {
			for task := range queue {
				msg, err := bot.Send(task.Chattable)
				if task.Resp != nil {
					select {
					case task.Resp <- sendResponse{Message: &msg, Err: err}:
					default:
					}
				}
			}
		}(i)
	}
}

func enqueueUserSend(msg tgbotapi.Chattable, timeout time.Duration) (*tgbotapi.Message, error) {
	if userSendQueue == nil {
		return nil, errors.New("userSendQueue not initialized")
	}
	resp := make(chan sendResponse, 1)
	task := sendTask{Chattable: msg, Resp: resp}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case userSendQueue <- task:
		select {
		case r := <-resp:
			return r.Message, r.Err
		case <-timer.C:
			return nil, errors.New("user send timeout")
		}
	case <-timer.C:
		return nil, errors.New("enqueue user send timeout")
	}
}

func enqueueUploadSend(msg tgbotapi.Chattable, timeout time.Duration) (*tgbotapi.Message, error) {
	if uploaderQueue == nil {
		return nil, errors.New("uploaderQueue not initialized")
	}
	resp := make(chan sendResponse, 1)
	task := sendTask{Chattable: msg, Resp: resp}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case uploaderQueue <- task:
		select {
		case r := <-resp:
			return r.Message, r.Err
		case <-timer.C:
			return nil, errors.New("uploader send timeout")
		}
	case <-timer.C:
		return nil, errors.New("enqueue uploader send timeout")
	}
}

func enqueueFireAndForget(msg tgbotapi.Chattable, forUser bool) error {
	task := sendTask{Chattable: msg, Resp: nil}
	if forUser {
		select {
		case userSendQueue <- task:
			return nil
		default:
			return errors.New("user queue full")
		}
	} else {
		select {
		case uploaderQueue <- task:
			return nil
		default:
			return errors.New("uploader queue full")
		}
	}
}

// Функции для работы с Gemini API
func callGemini(prompt string) (string, error) {
	key := os.Getenv("GEMINI_API_KEY")
	if key == "" {
		return "", fmt.Errorf("GEMINI_API_KEY не задан")
	}

	// Ждем доступный токен из лимитера (с таймаутом 30 секунд)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	
	select {
	case <-geminiLimiter:
		// Получили токен, можем делать запрос
		log.Printf("Получен токен для Gemini API запроса")
	case <-timeout.C:
		return "", fmt.Errorf("превышен лимит запросов к Gemini API, попробуйте позже")
	}

	reqBody := GRequest{
		SystemInstruction: &GContent{Parts: []GPart{{Text: "Отвечай кратко, по-русски, без Markdown ссылок."}}},
		Contents: []GContent{{Role: "user", Parts: []GPart{{Text: prompt}}}},
	}
	b, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", geminiURL, bytes.NewBuffer(b))
	if err != nil { return "", err }
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", key)

	client := &http.Client{Timeout: 45 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return "", err }
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Gemini API %d: %s", resp.StatusCode, string(body))
	}
	var gr GResponse
	if err := json.Unmarshal(body, &gr); err != nil { return "", err }
	if gr.Error != nil { return "", fmt.Errorf("Gemini error: %s", gr.Error.Message) }
	if len(gr.Candidates) == 0 || len(gr.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("Gemini вернул пустой ответ")
	}
	return gr.Candidates[0].Content.Parts[0].Text, nil
}

// Функции для управления состояниями пользователей
func setUserState(userID int64, state string) {
	userStatesMutex.Lock()
	userStates[userID] = state
	userStatesMutex.Unlock()
}

func getUserState(userID int64) string {
	userStatesMutex.RLock()
	state := userStates[userID]
	userStatesMutex.RUnlock()
	return state
}

func clearUserState(userID int64) {
	userStatesMutex.Lock()
	delete(userStates, userID)
	userStatesMutex.Unlock()
}

// Функции для работы с лимитами запросов
func checkAndUpdateUserLimit(userID int64) (bool, int, error) {
	// Проверяем белый список безлимитных пользователей
	if unlimitedUserIDs[userID] {
		log.Printf("Пользователь %d в белом списке - безлимитный доступ", userID)
		return true, 999, nil // безлимитный доступ
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	
	now := time.Now()
	moscowTZ, _ := time.LoadLocation("Europe/Moscow")
	nowMoscow := now.In(moscowTZ)
	resetTime := time.Date(nowMoscow.Year(), nowMoscow.Month(), nowMoscow.Day(), limitResetHour, 0, 0, 0, moscowTZ)
	
	// Если сейчас раньше времени сброса, то берем сброс предыдущего дня
	if nowMoscow.Before(resetTime) {
		resetTime = resetTime.AddDate(0, 0, -1)
	}
	
	var user User
	err := db.QueryRow(ctx, "SELECT id, COALESCE(chat_requests, 0), COALESCE(last_reset_date, now()) FROM users WHERE id = $1", userID).Scan(
		&user.ID, &user.ChatRequests, &user.LastResetDate)
	
	if err != nil {
		if err == sql.ErrNoRows {
			// Пользователь не найден, создаем запись
			_, err = db.Exec(ctx, `
				INSERT INTO users (id, chat_requests, last_reset_date) 
				VALUES ($1, 1, $2)
				ON CONFLICT (id) DO UPDATE SET chat_requests = 1, last_reset_date = $2
			`, userID, resetTime)
			return err == nil, dailyRequestLimit - 1, err
		}
		return false, 0, err
	}
	
	// Проверяем, нужно ли сбросить счетчик
	if user.LastResetDate.Before(resetTime) {
		// Сбрасываем счетчик
		_, err = db.Exec(ctx, "UPDATE users SET chat_requests = 1, last_reset_date = $1 WHERE id = $2", resetTime, userID)
		return err == nil, dailyRequestLimit - 1, err
	}
	
	// Проверяем лимит
	if user.ChatRequests >= dailyRequestLimit {
		return false, 0, nil
	}
	
	// Увеличиваем счетчик
	_, err = db.Exec(ctx, "UPDATE users SET chat_requests = chat_requests + 1 WHERE id = $1", userID)
	remaining := dailyRequestLimit - user.ChatRequests - 1
	if remaining < 0 {
		remaining = 0
	}
	return err == nil, remaining, err
}

// Новые функции для админ-команд
// Проверка прав администратора
func isAdmin(userID int64) bool {
	return userID == adminUserID
}

// Рассылка сообщений всем пользователям
func broadcastMessage(message string) error {
	ctx := context.Background()
	users, err := getUsers(ctx)
	if err != nil {
		return fmt.Errorf("ошибка получения пользователей: %w", err)
	}

	log.Printf("Начинаем рассылку сообщения %d пользователям", len(users))

	// Создаем ticker для соблюдения rate limit
	ticker := time.NewTicker(time.Second / broadcastRateLimit)
	defer ticker.Stop()

	successCount := 0
	errorCount := 0

	for i, user := range users {
		<-ticker.C // Ждем разрешения на отправку

		msg := tgbotapi.NewMessage(user.ID, message)
		msg.ParseMode = "Markdown"

		if err := enqueueFireAndForget(msg, true); err != nil {
			log.Printf("Ошибка добавления сообщения в очередь для пользователя %d: %v", user.ID, err)
			errorCount++
		} else {
			successCount++
		}

		// Логируем прогресс каждые 50 сообщений
		if (i+1)%50 == 0 {
			log.Printf("Рассылка: отправлено %d/%d сообщений", i+1, len(users))
		}
	}

	log.Printf("Рассылка завершена: успешно=%d, ошибок=%d", successCount, errorCount)
	return nil
}

// Обработка команды /message
func handleMessageCommand(chatID int64, userID int64, args string) {
	if !isAdmin(userID) {
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "❌ У вас нет доступа к этой команде."), 3*time.Second)
		return
	}

	args = strings.TrimSpace(args)
	if args != "" {
		// Есть аргументы - сразу рассылаем
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "📤 Начинаю рассылку сообщения..."), 3*time.Second)
		
		go func() {
			if err := broadcastMessage(args); err != nil {
				log.Printf("Ошибка рассылки: %v", err)
				_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "❌ Ошибка при рассылке сообщения."), 3*time.Second)
			} else {
				_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "✅ Рассылка завершена успешно."), 3*time.Second)
			}
		}()
	} else {
		// Нет аргументов - запрашиваем текст для рассылки
		setUserState(userID, "waiting_for_broadcast_message")
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "📝 Отправьте текст для рассылки всем пользователям:"), 3*time.Second)
	}
}

// Обработка команды /reload
func handleReloadCommand(chatID int64, userID int64, args string) {
	if !isAdmin(userID) {
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "❌ У вас нет доступа к этой команде."), 3*time.Second)
		return
	}

	info := strings.TrimSpace(args)
	if info == "" {
		info = "Обновление системы"
	}

	messageText := fmt.Sprintf("🔄 Обновление бота: %s", info)

	// Создаем inline-клавиатуру с кнопкой перезагрузки
	inlineKeyboard := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🔄 Перезагрузить бота", "restart"),
		),
	)

	_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "📤 Начинаю рассылку уведомления об обновлении..."), 3*time.Second)

	go func() {
		ctx := context.Background()
		users, err := getUsers(ctx)
		if err != nil {
			log.Printf("Ошибка получения пользователей для reload: %v", err)
			_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "❌ Ошибка при получении списка пользователей."), 3*time.Second)
			return
		}

		log.Printf("Начинаем рассылку reload-уведомления %d пользователям", len(users))

		// Создаем ticker для соблюдения rate limit
		ticker := time.NewTicker(time.Second / broadcastRateLimit)
		defer ticker.Stop()

		successCount := 0
		errorCount := 0

		for i, user := range users {
			<-ticker.C

			msg := tgbotapi.NewMessage(user.ID, messageText)
			msg.ReplyMarkup = inlineKeyboard

			if err := enqueueFireAndForget(msg, true); err != nil {
				log.Printf("Ошибка добавления reload-сообщения в очередь для пользователя %d: %v", user.ID, err)
				errorCount++
			} else {
				successCount++
			}

			if (i+1)%50 == 0 {
				log.Printf("Reload рассылка: отправлено %d/%d сообщений", i+1, len(users))
			}
		}

		log.Printf("Reload рассылка завершена: успешно=%d, ошибок=%d", successCount, errorCount)
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "✅ Рассылка уведомления об обновлении завершена успешно."), 3*time.Second)
	}()
}

// Обработка callback_query для перезагрузки
func handleRestartCallback(callbackQuery *tgbotapi.CallbackQuery) {
	userID := callbackQuery.From.ID
	chatID := callbackQuery.Message.Chat.ID

	// Отвечаем на callback_query без всплывающего алерта
	callback := tgbotapi.NewCallback(callbackQuery.ID, "")
	if _, err := bot.Request(callback); err != nil {
		log.Printf("Ошибка ответа на callback_query: %v", err)
	}

	// Вызываем ту же логику, что и /start
	sendStartMessage(chatID)
	log.Printf("Пользователь %d перезагрузил бота через inline-кнопку", userID)
}

// main функция
func main() {
	telegramToken := os.Getenv("TELEGRAM_TOKEN")
	if telegramToken == "" {
		log.Fatal("TELEGRAM_TOKEN не задан")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL не задан")
	}

	// Инициализируем глобальный лимитер для Gemini API
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

	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

	userSendQueue = make(chan sendTask, 400)
	uploaderQueue = make(chan sendTask, 200)

	startSenderPool(12, userSendQueue)
	startSenderPool(6, uploaderQueue)

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
		scrapeImages()
		for {
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

func ensureScheduleTable(ctx context.Context) error {
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS schedule_cache (
		url TEXT PRIMARY KEY,
		corpus TEXT NOT NULL,
		scraped_date DATE NOT NULL,
		actual_date DATE,
		file_id TEXT,
		is_valid_url BOOLEAN DEFAULT true
	);
	`)
	return err
}

func ensureUsersTable(ctx context.Context) error {
	// Сначала создаем таблицу, если её нет
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS users (
		id BIGINT PRIMARY KEY,
		username TEXT,
		first_seen TIMESTAMPTZ DEFAULT now(),
		last_seen TIMESTAMPTZ DEFAULT now(),
		preferred_corpus TEXT
	);
	`)
	if err != nil {
		return fmt.Errorf("создание таблицы users: %w", err)
	}

	// Добавляем новые колонки, если их нет (миграция)
	_, err = db.Exec(ctx, `
	ALTER TABLE users
	ADD COLUMN IF NOT EXISTS chat_requests INTEGER DEFAULT 0,
	ADD COLUMN IF NOT EXISTS last_reset_date TIMESTAMPTZ DEFAULT now();
	`)
	if err != nil {
		return fmt.Errorf("добавление колонок для чат-бота: %w", err)
	}

	log.Println("Таблица users успешно создана/обновлена с поддержкой чат-бота")
	return nil
}

func ensureScheduleParsedTable(ctx context.Context) error {
	_, err := db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS schedule_parsed (
		id SERIAL PRIMARY KEY,

		-- Мета-информация
		image_url TEXT NOT NULL,
		date DATE NOT NULL,
		corpus TEXT NOT NULL CHECK (corpus IN ('A', 'B')),

		-- Данные занятия
		group_name TEXT NOT NULL,
		pair_number INTEGER CHECK (pair_number BETWEEN 1 AND 6),
		subject TEXT NOT NULL,
		teacher TEXT,
		room TEXT,

		-- Качество распознавания
		confidence_score FLOAT DEFAULT 0.0 CHECK (confidence_score BETWEEN 0 AND 1),
		status TEXT DEFAULT 'pending' CHECK (status IN ('verified', 'partial', 'needs_review', 'failed')),

		-- Логи
		raw_json JSONB,
		error_message TEXT,
		created_at TIMESTAMPTZ DEFAULT now(),
		updated_at TIMESTAMPTZ DEFAULT now(),

		CONSTRAINT unique_schedule_entry UNIQUE (date, corpus, group_name, pair_number)
	);

	CREATE INDEX IF NOT EXISTS idx_schedule_date_group ON schedule_parsed(date, group_name);
	CREATE INDEX IF NOT EXISTS idx_schedule_corpus ON schedule_parsed(corpus, date);
	CREATE INDEX IF NOT EXISTS idx_schedule_status ON schedule_parsed(status);
	`)
	if err != nil {
		return fmt.Errorf("создание таблицы schedule_parsed: %w", err)
	}

	_, err = db.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS ocr_logs (
		id SERIAL PRIMARY KEY,
		image_url TEXT NOT NULL,
		date DATE,
		corpus TEXT,
		status TEXT,
		confidence_avg FLOAT,
		records_extracted INTEGER,
		error_message TEXT,
		processing_time_ms INTEGER,
		created_at TIMESTAMPTZ DEFAULT now()
	);
	`)
	return err
}

func loadScheduleCache(ctx context.Context) (map[string]*ScheduleItem, map[string]*ScheduleItem, error) {
	rows, err := db.Query(ctx, "SELECT url, corpus, scraped_date, actual_date, file_id, COALESCE(is_valid_url, true) FROM schedule_cache")
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
		var actualDate sql.NullTime
		if err := rows.Scan(&item.URL, &corpus, &item.Date, &actualDate, &fileID, &item.IsValidURL); err != nil {
			return nil, nil, fmt.Errorf("rows.Scan: %w", err)
		}
		item.FileID = fileID.String
		if actualDate.Valid {
			item.ActualDate = actualDate.Time
		}
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

	if _, err := tx.Prepare(ctx, "insert_schedule", 
		"INSERT INTO schedule_cache (url, corpus, scraped_date, actual_date, file_id, is_valid_url) VALUES ($1, $2, $3, $4, $5, $6)"); err != nil {
		return fmt.Errorf("tx.Prepare: %w", err)
	}

	for _, item := range scheduleA {
		var actualDate interface{}
		if !item.ActualDate.IsZero() {
			actualDate = item.ActualDate
		}
		log.Printf("Сохранение в БД (корпус А): URL=%s, Date=%s, ActualDate=%v, IsValid=%v",
			item.URL, item.Date.Format("2006-01-02"), actualDate, item.IsValidURL)
		if _, err := tx.Exec(ctx, "insert_schedule", item.URL, "a", item.Date, actualDate, item.FileID, item.IsValidURL); err != nil {
			return fmt.Errorf("tx.Exec INSERT A: %w", err)
		}
	}
	for _, item := range scheduleB {
		var actualDate interface{}
		if !item.ActualDate.IsZero() {
			actualDate = item.ActualDate
		}
		log.Printf("Сохранение в БД (корпус Б): URL=%s, Date=%s, ActualDate=%v, IsValid=%v",
			item.URL, item.Date.Format("2006-01-02"), actualDate, item.IsValidURL)
		if _, err := tx.Exec(ctx, "insert_schedule", item.URL, "b", item.Date, actualDate, item.FileID, item.IsValidURL); err != nil {
			return fmt.Errorf("tx.Exec INSERT B: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tx.Commit: %w", err)
	}
	log.Println("Кэш расписания успешно сохранен в БД")
	return nil
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

	// Обработка callback_query для inline-кнопок
	if update.CallbackQuery != nil {
		if update.CallbackQuery.Data == "restart" {
			handleRestartCallback(update.CallbackQuery)
		}
		return
	}

	if update.Message != nil && update.Message.IsCommand() {
		userID := update.Message.From.ID
		chatID := update.Message.Chat.ID
		commandArgs := update.Message.CommandArguments()

		switch update.Message.Command() {
		case "start":
			sendStartMessage(chatID)
		case "stats":
			if isAdmin(userID) {
				go sendStatsToAdmin(chatID)
			} else {
				_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "У вас нет доступа к этой команде."), 3*time.Second)
			}
		case "message":
			handleMessageCommand(chatID, userID, commandArgs)
		case "reload":
			handleReloadCommand(chatID, userID, commandArgs)
		}
	} else if update.Message != nil && update.Message.Text != "" {
		userID := update.Message.From.ID
		chatID := update.Message.Chat.ID
		messageText := update.Message.Text
		
		// Проверяем состояние пользователя
		userState := getUserState(userID)
		
		if userState == "waiting_for_question" {
			// Пользователь отправил вопрос для чат-бота
			handleChatbotQuestion(chatID, userID, messageText)
			clearUserState(userID)
			return
		} else if userState == "waiting_for_broadcast_message" {
			// Админ отправил текст для рассылки
			if isAdmin(userID) {
				clearUserState(userID)
				_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "📤 Начинаю рассылку сообщения..."), 3*time.Second)
				
				go func() {
					if err := broadcastMessage(messageText); err != nil {
						log.Printf("Ошибка рассылки: %v", err)
						_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "❌ Ошибка при рассылке сообщения."), 3*time.Second)
					} else {
						_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "✅ Рассылка завершена успешно."), 3*time.Second)
					}
				}()
			} else {
				clearUserState(userID)
				_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "❌ У вас нет прав для выполнения этого действия."), 3*time.Second)
			}
			return
		}
		
		switch messageText {
		case "Расписание А":
			log.Printf("Пользователь %d (%s) запросил расписание корпуса А", update.Message.From.ID, update.Message.From.UserName)
			sendSchedule(update.Message.Chat.ID, "A")
			if err := updateUserPreference(update.Message.From.ID, "A"); err != nil {
				log.Printf("Ошибка обновления предпочтения пользователя %d: %v", update.Message.From.ID, err)
			}
		case "Расписание Б":
			log.Printf("Пользователь %d (%s) запросил расписание корпуса Б", update.Message.From.ID, update.Message.From.UserName)
			sendSchedule(update.Message.Chat.ID, "B")
			if err := updateUserPreference(update.Message.From.ID, "B"); err != nil {
				log.Printf("Ошибка обновления предпочтения пользователя %d: %v", update.Message.From.ID, err)
			}
		case "Поддержка и предложения":
			sendSupportMessage(update.Message.Chat.ID)
		case "Генерация":
			startChatbotSession(chatID, userID)
		default:
			_, _ = enqueueUserSend(tgbotapi.NewMessage(update.Message.Chat.ID, "Выберите кнопку на клавиатуре или напишите команду /start"), 3*time.Second)
		}
	}
}

func startChatbotSession(chatID, userID int64) {
	// Проверяем лимит запросов
	allowed, remaining, err := checkAndUpdateUserLimit(userID)
	if err != nil {
		log.Printf("Ошибка проверки лимита для пользователя %d: %v", userID, err)
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "Произошла ошибка. Попробуйте позже."), 3*time.Second)
		return
	}
	
	if !allowed {
		msg := "❌ Вы исчерпали дневной лимит запросов к чат-боту (10 запросов в день).\n\n" +
			"🕒 Лимит обновляется каждый день в 3:00 по московскому времени."
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, msg), 3*time.Second)
		return
	}
	
	// Устанавливаем состояние ожидания вопроса
	setUserState(userID, "waiting_for_question")
	
	var limitInfo string
	if unlimitedUserIDs[userID] {
		limitInfo = "📊 У вас безлимитный доступ!"
	} else {
		limitInfo = fmt.Sprintf("📊 Осталось запросов сегодня: %d/10\n🕒 Лимит обновляется в 3:00 МСК", remaining)
	}
	
	msg := fmt.Sprintf("🤖 Добро пожаловать в чат-бот!\n\n" +
		"💬 Напишите ваш вопрос, и я отвечу на него.\n\n" +
		"%s", limitInfo)
	
	_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, msg), 3*time.Second)
}

func handleChatbotQuestion(chatID, userID int64, question string) {
	log.Printf("Пользователь %d задал вопрос чат-боту: %s", userID, question)
	
	// Отправляем сообщение о том, что обрабатываем запрос
	processingMsg := "🔄 Обрабатываю ваш запрос, пожалуйста подождите..."
	_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, processingMsg), 3*time.Second)
	
	// Вызываем Gemini API
	answer, err := callGemini(question)
	if err != nil {
		log.Printf("Ошибка вызова Gemini API для пользователя %d: %v", userID, err)
		
		// Проверяем, связана ли ошибка с лимитом
		if strings.Contains(err.Error(), "превышен лимит") {
			errorMsg := "⏳ Превышен лимит запросов к ИИ. Попробуйте через несколько секунд."
			_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, errorMsg), 3*time.Second)
		} else {
			errorMsg := "❌ Произошла ошибка при обработке вашего запроса. Попробуйте позже."
			_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, errorMsg), 3*time.Second)
		}
		return
	}
	
	// Ограничиваем длину ответа (Telegram имеет лимит 4096 символов)
	if len(answer) > 4000 {
		answer = answer[:4000] + "...\n\n[Ответ сокращен из-за ограничений Telegram]"
	}
	
	// Отправляем ответ
	responseMsg := fmt.Sprintf("🤖 **Ответ:**\n\n%s", answer)
	msg := tgbotapi.NewMessage(chatID, responseMsg)
	msg.ParseMode = "Markdown"
	_, _ = enqueueUserSend(msg, 10*time.Second)
	
	log.Printf("Отправлен ответ чат-бота пользователю %d", userID)
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

			if u.PreferredCorpus.Valid {
				switch strings.ToUpper(u.PreferredCorpus.String) {
				case "A":
					if len(newItemsA) > 0 {
						sendNotificationForNewItems(u.ID, "А", newItemsA)
					}
				case "B":
					if len(newItemsB) > 0 {
						sendNotificationForNewItems(u.ID, "Б", newItemsB)
					}
				default:
					if len(newItemsA) > 0 {
						sendNotificationForNewItems(u.ID, "А", newItemsA)
					}
					if len(newItemsB) > 0 {
						sendNotificationForNewItems(u.ID, "Б", newItemsB)
					}
				}
				return
			}

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
	if _, err := enqueueUserSend(headerMsg, 6*time.Second); err != nil {
		_ = enqueueFireAndForget(headerMsg, true)
		log.Printf("Ошибка отправки уведомления пользователю %d: %v (falling back to fire-and-forget)", chatID, err)
	}
}

func copyScheduleMap(src map[string]*ScheduleItem) map[string]*ScheduleItem {
	dst := make(map[string]*ScheduleItem, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func sendSchedule(chatID int64, corpus string) {
	log.Printf("Начало отправки расписания корпуса %s пользователю %d", corpus, chatID)

	var scheduleMap map[string]*ScheduleItem
	mu.RLock()
	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = copyScheduleMap(scheduleA)
	case "B":
		scheduleMap = copyScheduleMap(scheduleB)
	default:
		mu.RUnlock()
		log.Printf("Неизвестный корпус '%s' для пользователя %d", corpus, chatID)
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "Неизвестный корпус"), 3*time.Second)
		return
	}
	mu.RUnlock()

	if len(scheduleMap) == 0 {
		log.Printf("Расписание корпуса %s не найдено для пользователя %d", corpus, chatID)
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "Расписание не найдено."), 3*time.Second)
		return
	}

	type item struct{ *ScheduleItem }
	var items []item
	for _, it := range scheduleMap {
		items = append(items, item{it})
	}
	sort.Slice(items, func(i, j int) bool {
		// Используем ActualDate если доступна, иначе Date
		dateI := items[i].Date
		if !items[i].ActualDate.IsZero() {
			dateI = items[i].ActualDate
		}
		dateJ := items[j].Date
		if !items[j].ActualDate.IsZero() {
			dateJ = items[j].ActualDate
		}
		return dateI.Before(dateJ)
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

	ticker := time.NewTicker(60 * time.Millisecond)
	defer ticker.Stop()

	for i, it := range items {
		<-ticker.C

		// Для дня недели ВСЕГДА используем Date (дата из URL)
		// Для отображения числа используем ActualDate если есть коррекция
		weekdayDate := it.Date
		displayDate := it.Date
		if !it.ActualDate.IsZero() {
			displayDate = it.ActualDate
		}

		weekday := weekdays[weekdayDate.Weekday()]
		caption := fmt.Sprintf("%s — %02d.%02d.%d", weekday, displayDate.Day(), displayDate.Month(), displayDate.Year())

		var msg tgbotapi.Chattable
		if it.FileID != "" {
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileID(it.FileID))
			photo.Caption = caption
			msg = photo
			log.Printf("Отправка по FileID: %s -> chat %d (%s) [элемент %d/%d]", it.FileID, chatID, caption, i+1, len(items))
		} else {
			uniqueURL := fmt.Sprintf("%s?send_cb=%d", it.URL, time.Now().UnixNano())
			photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(uniqueURL))
			photo.Caption = caption
			msg = photo
			log.Printf("FileID отсутствует, отправка по URL: %s -> chat %d [элемент %d/%d]", it.URL, chatID, i+1, len(items))
		}

		if _, err := enqueueUserSend(msg, 20*time.Second); err != nil {
			_ = enqueueFireAndForget(msg, true)
			log.Printf("Ошибка отправки фото (chat %d): %v (перешли в fire-and-forget)", chatID, err)
		}
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
			tgbotapi.NewKeyboardButton("Генерация"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Поддержка и предложения"),
		),
	)
	msg.ReplyMarkup = keyboard
	if _, err := enqueueUserSend(msg, 5*time.Second); err != nil {
		log.Printf("sendStartMessage enqueue err: %v. Попытка прямой отправки.", err)
		if _, e := bot.Send(msg); e != nil {
			log.Printf("sendStartMessage fallback bot.Send err: %v", e)
		}
	}
}

func sendSupportMessage(chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "По вопросам поддержки: @podkmt")
	if _, err := enqueueUserSend(msg, 5*time.Second); err != nil {
		log.Printf("sendSupportMessage enqueue err: %v. Попытка прямой отправки.", err)
		if _, e := bot.Send(msg); e != nil {
			log.Printf("sendSupportMessage fallback bot.Send err: %v", e)
		}
	}
}

func sendStatsToAdmin(chatID int64) {
	ctx := context.Background()
	users, err := getUsers(ctx)
	if err != nil {
		log.Printf("Ошибка получения пользователей для статистики: %v", err)
		_, _ = enqueueUserSend(tgbotapi.NewMessage(chatID, "Ошибка получения статистики."), 3*time.Second)
		return
	}

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
	if _, err := enqueueUserSend(msg, 5*time.Second); err != nil {
		log.Printf("Ошибка отправки статистики админу %d: %v", chatID, err)
	}
}

func uploadAndGetFileID(item *ScheduleItem) string {
	uploadURL := fmt.Sprintf("%s?upload_cache_bust_scrape=%d", item.URL, time.Now().UnixNano())
	const adminChatID = int64(6436017953) // Замените на ваш чат ID для кэширования

	photo := tgbotapi.NewPhoto(adminChatID, tgbotapi.FileURL(uploadURL))
	photo.DisableNotification = true
	photo.Caption = fmt.Sprintf("[Кэширование] %s", item.URL)

	msg, err := enqueueUploadSend(photo, 35*time.Second)
	if err != nil {
		log.Printf("Ошибка загрузки фото в Telegram для кэширования %s: %v", item.URL, err)
		return ""
	}
	if msg == nil || len(msg.Photo) == 0 {
		log.Printf("Telegram вернул сообщение без фото для %s. Ответ: %+v", item.URL, msg)
		return ""
	}

	fileID := msg.Photo[len(msg.Photo)-1].FileID
	log.Printf("Загружено и закэшировано фото %s -> FileID: %s", item.URL, fileID)

	// Удаляем сообщение с кэшем, чтобы не засорять чат
	go func(chatID int64, messageID int) {
		_, _ = bot.Request(tgbotapi.NewDeleteMessage(chatID, messageID))
	}(adminChatID, msg.MessageID)

	return fileID
}

// Обновленная функция скрапинга с умной коррекцией дат
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

	re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?_korpus_([av])\.jpe?g(?:\?.*)?$`)

	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		src := e.Attr("src")
		srcClean := strings.Split(src, "?")[0]
		log.Printf("Обработка изображения: %s", src)
		matches := re.FindStringSubmatch(srcClean)
		if len(matches) < 5 {
			log.Printf("Изображение не соответствует паттерну: %s", srcClean)
			return
		}
		day, _ := strconv.Atoi(matches[1])
		month, _ := strconv.Atoi(matches[2])
		yearStr := matches[3]
		corpus := strings.ToLower(matches[4])

		year := time.Now().Year()
		if yearStr != "" {
			if parsedYear, err := strconv.Atoi(yearStr); err == nil {
				year = parsedYear
			}
		}

		urlDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
		fullURL := strings.TrimRight(baseSiteURL, "/") + path.Clean("/"+strings.TrimLeft(srcClean, "/"))

		// Применяем умную коррекцию даты ВСЕГДА
		correctedDate := smartDateCorrection(urlDate, srcClean)
		
		item := &ScheduleItem{
			URL:        fullURL,
			Date:       urlDate,        // Оригинальная дата из URL
			ActualDate: correctedDate,  // Скорректированная дата
			IsValidURL: urlDate.Equal(correctedDate), // false если была коррекция
		}

		var oldCache map[string]*ScheduleItem
		if corpus == "a" {
			oldCache = oldScheduleA
		} else {
			oldCache = oldScheduleB
		}
		if oldItem, exists := oldCache[item.URL]; exists && oldItem.FileID != "" {
			item.FileID = oldItem.FileID
			log.Printf("FileID для %s взят из кэша: %s", item.URL, item.FileID)
		}

		displayDate := correctedDate // Всегда используем скорректированную дату для отображения
		statusText := ""
		if !item.IsValidURL {
			statusText = " [Дата скорректирована]"
		}

		if corpus == "a" {
			if _, exists := tempScheduleA[item.URL]; exists {
				log.Printf("ВНИМАНИЕ: Дубликат URL для корпуса А: %s", item.URL)
			}
			tempScheduleA[item.URL] = item
			log.Printf("Найдено фото корпуса А: %s (%02d.%02d.%d)%s",
				item.URL, displayDate.Day(), displayDate.Month(), displayDate.Year(), statusText)
		} else {
			if _, exists := tempScheduleB[item.URL]; exists {
				log.Printf("ВНИМАНИЕ: Дубликат URL для корпуса Б: %s", item.URL)
			}
			tempScheduleB[item.URL] = item
			log.Printf("Найдено фото корпуса Б: %s (%02d.%02d.%d)%s",
				item.URL, displayDate.Day(), displayDate.Month(), displayDate.Year(), statusText)
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
	log.Printf("Скрапинг HTML завершен. Найдено: корпус А = %d, корпус Б = %d", len(tempScheduleA), len(tempScheduleB))

	log.Println("Начинаем загрузку новых изображений в Telegram...")
	uploadStart := time.Now()

	newItemsA := findNewItems(tempScheduleA, oldScheduleA)
	newItemsB := findNewItems(tempScheduleB, oldScheduleB)

	log.Printf("Найдено новых изображений для загрузки: A=%d, B=%d", len(newItemsA), len(newItemsB))

	var wgUpload sync.WaitGroup
	semaphore := make(chan struct{}, 4)

	for _, item := range newItemsA {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()
				it.FileID = uploadAndGetFileID(it)
			}(item)
		}
	}
	for _, item := range newItemsB {
		if item.FileID == "" {
			wgUpload.Add(1)
			go func(it *ScheduleItem) {
				defer wgUpload.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()
				it.FileID = uploadAndGetFileID(it)
			}(item)
		}
	}
	wgUpload.Wait()

	log.Printf("Загрузка новых изображений завершена за %v", time.Since(uploadStart))

	// === НАЧАЛО БЛОКА OCR ===
	log.Println("Начинаем OCR для новых расписаний...")
	ocrStart := time.Now()

	// Ограничиваем количество одновременных OCR-запросов
	ocrSemaphore := make(chan struct{}, 2) // Максимум 2 параллельных OCR
	var wgOCR sync.WaitGroup

	// Обрабатываем OCR для новых изображений корпуса А
	for _, item := range newItemsA {
		wgOCR.Add(1)
		go func(it *ScheduleItem) {
			defer wgOCR.Done()
			ocrSemaphore <- struct{}{}
			defer func() { <-ocrSemaphore }()

			if err := processScheduleOCR(it, "A"); err != nil {
				log.Printf("Ошибка OCR для %s: %v", it.URL, err)
			}
		}(item)
	}

	// Обрабатываем OCR для новых изображений корпуса Б
	for _, item := range newItemsB {
		wgOCR.Add(1)
		go func(it *ScheduleItem) {
			defer wgOCR.Done()
			ocrSemaphore <- struct{}{}
			defer func() { <-ocrSemaphore }()

			if err := processScheduleOCR(it, "B"); err != nil {
				log.Printf("Ошибка OCR для %s: %v", it.URL, err)
			}
		}(item)
	}

	wgOCR.Wait()
	log.Printf("OCR завершён для %d изображений за %v", len(newItemsA)+len(newItemsB), time.Since(ocrStart))
	// === КОНЕЦ БЛОКА OCR ===

	mu.Lock()
	newScheduleA := make(map[string]*ScheduleItem, len(tempScheduleA))
	newScheduleB := make(map[string]*ScheduleItem, len(tempScheduleB))
	for k, v := range tempScheduleA {
		newScheduleA[k] = v
	}
	for k, v := range tempScheduleB {
		newScheduleB[k] = v
	}
	scheduleA = newScheduleA
	scheduleB = newScheduleB
	mu.Unlock()
	log.Printf("Глобальные мапы расписаний обновлены. Корпус А: %d записей, Корпус Б: %d записей", len(scheduleA), len(scheduleB))

	// Выводим все URL для отладки
	log.Println("=== Список всех URL корпуса А ===")
	for url, item := range scheduleA {
		log.Printf("  - %s (Date=%s, ActualDate=%s)", url, item.Date.Format("2006-01-02"), item.ActualDate.Format("2006-01-02"))
	}
	log.Println("=== Список всех URL корпуса Б ===")
	for url, item := range scheduleB {
		log.Printf("  - %s (Date=%s, ActualDate=%s)", url, item.Date.Format("2006-01-02"), item.ActualDate.Format("2006-01-02"))
	}

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

// Универсальная функция для вызова Gemini API с поддержкой изображений
func callGeminiWithRequest(reqBody GRequest) (string, error) {
	key := os.Getenv("GEMINI_API_KEY")
	if key == "" {
		return "", fmt.Errorf("GEMINI_API_KEY не задан")
	}

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	select {
	case <-geminiLimiter:
		log.Printf("Получен токен для Gemini API")
	case <-timeout.C:
		return "", fmt.Errorf("превышен лимит запросов к Gemini API")
	}

	b, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", geminiURL, bytes.NewBuffer(b))
	if err != nil { return "", err }
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", key)

	client := &http.Client{Timeout: 90 * time.Second} // Увеличено для OCR
	resp, err := client.Do(req)
	if err != nil { return "", err }
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Gemini API %d: %s", resp.StatusCode, string(body))
	}

	var gr GResponse
	if err := json.Unmarshal(body, &gr); err != nil { return "", err }
	if gr.Error != nil { return "", fmt.Errorf("Gemini error: %s", gr.Error.Message) }
	if len(gr.Candidates) == 0 || len(gr.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("Gemini вернул пустой ответ")
	}

	return gr.Candidates[0].Content.Parts[0].Text, nil
}

func processScheduleOCR(item *ScheduleItem, corpus string) error {
	startTime := time.Now()

	// 1. Скачать изображение
	resp, err := http.Get(item.URL)
	if err != nil {
		logOCRError(item.URL, "download_failed", err)
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	imageBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logOCRError(item.URL, "read_failed", err)
		return fmt.Errorf("read failed: %w", err)
	}

	// 2. Кодировать в base64
	imageB64 := base64.StdEncoding.EncodeToString(imageBytes)

	// 3. Вызвать Gemini Vision API
	reqBody := GRequest{
		SystemInstruction: &GContent{Parts: []GPart{{Text: "Отвечай только валидным JSON, без комментариев."}}},
		Contents: []GContent{{
			Parts: []GPart{
				{InlineData: &InlineData{MimeType: "image/jpeg", Data: imageB64}},
				{Text: geminiSchedulePrompt},
			},
		}},
	}

	jsonResponse, err := callGeminiWithRequest(reqBody)
	if err != nil {
		logOCRError(item.URL, "gemini_failed", err)
		return fmt.Errorf("gemini error: %w", err)
	}

	// 4. Парсинг JSON
	var geminiResp GeminiScheduleResponse
	if err := json.Unmarshal([]byte(jsonResponse), &geminiResp); err != nil {
		logOCRError(item.URL, "json_parse_failed", err)
		return fmt.Errorf("json parse error: %w", err)
	}

	// 5. Проверка на ошибку
	if geminiResp.Metadata.Error != "" {
		log.Printf("Gemini не смог распознать %s: %s", item.URL, geminiResp.Metadata.Error)
		logOCRFailure(item.URL, item.Date, corpus, 0, 0.0, time.Since(startTime))
		return fmt.Errorf("recognition failed: %s", geminiResp.Metadata.Error)
	}

	// 6. Вставка в БД
	ctx := context.Background()
	successCount := 0

	for _, entry := range geminiResp.Schedule {
		// Валидация
		if entry.Group == "" || entry.Subject == "" || entry.PairNumber < 1 || entry.PairNumber > 6 {
			log.Printf("Пропускаем невалидную запись: %+v", entry)
			continue
		}

		// Определение статуса
		status := "verified"
		if entry.Confidence < 0.6 {
			status = "needs_review"
		} else if entry.Confidence < 0.8 {
			status = "partial"
		}

		// Вставка
		_, err := db.Exec(ctx, `
			INSERT INTO schedule_parsed
				(image_url, date, corpus, group_name, pair_number, subject, teacher, room,
				 confidence_score, status, raw_json, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
			ON CONFLICT (date, corpus, group_name, pair_number)
			DO UPDATE SET
				subject = EXCLUDED.subject,
				teacher = EXCLUDED.teacher,
				room = EXCLUDED.room,
				confidence_score = EXCLUDED.confidence_score,
				status = EXCLUDED.status,
				raw_json = EXCLUDED.raw_json,
				updated_at = now()
		`, item.URL, item.Date, corpus, entry.Group, entry.PairNumber,
		   entry.Subject, entry.Teacher, entry.Room, entry.Confidence, status, jsonResponse)

		if err != nil {
			log.Printf("Ошибка вставки в БД: %v", err)
			continue
		}
		successCount++
	}

	// 7. Логирование
	logOCRSuccess(item.URL, item.Date, corpus, successCount, geminiResp.Metadata.AverageConfidence, time.Since(startTime))

	log.Printf("OCR завершён: %s → %d/%d записей, avg=%.2f, time=%v",
		item.URL, successCount, len(geminiResp.Schedule), geminiResp.Metadata.AverageConfidence, time.Since(startTime))

	return nil
}

func logOCRSuccess(url string, date time.Time, corpus string, records int, confidence float64, duration time.Duration) {
	ctx := context.Background()
	_, _ = db.Exec(ctx, `
		INSERT INTO ocr_logs (image_url, date, corpus, status, confidence_avg, records_extracted, processing_time_ms)
		VALUES ($1, $2, $3, 'success', $4, $5, $6)
	`, url, date, corpus, confidence, records, duration.Milliseconds())
}

func logOCRFailure(url string, date time.Time, corpus string, records int, confidence float64, duration time.Duration) {
	ctx := context.Background()
	_, _ = db.Exec(ctx, `
		INSERT INTO ocr_logs (image_url, date, corpus, status, confidence_avg, records_extracted, processing_time_ms)
		VALUES ($1, $2, $3, 'failed', $4, $5, $6)
	`, url, date, corpus, confidence, records, duration.Milliseconds())
}

func logOCRError(url string, errorType string, err error) {
	ctx := context.Background()
	_, _ = db.Exec(ctx, `
		INSERT INTO ocr_logs (image_url, status, error_message)
		VALUES ($1, $2, $3)
	`, url, "failed", fmt.Sprintf("%s: %v", errorType, err))
}