package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const (
	telegramToken = "8066179082:AAHAhf67ZlR1A_rZGUR-xe8nCj70sv43C80"
	// Исправлено: Убраны лишние пробелы
	url = "https://kmtko.my1.ru/index/raspisanie_zanjatij_ochno/0-403"
	// Путь для вебхука
	webhookPath = "/webhook"
)

// ScheduleData хранит информацию о расписании для конкретного корпуса
// (Не используется в текущей реализации, можно удалить)
// type ScheduleData struct {
// 	ImageURL string
// 	Date     time.Time
// }

// Хранилище для изображений корпуса А и Б
var scheduleA = make(map[string]time.Time) // URL -> Date
var scheduleB = make(map[string]time.Time) // URL -> Date
var mu sync.RWMutex

var uniqueUsers = make(map[int64]string)
var bot *tgbotapi.BotAPI // Глобальная переменная для бота

func main() {
	var err error
	bot, err = tgbotapi.NewBotAPI(telegramToken)
	if err != nil {
		log.Fatalf("Ошибка при создании бота: %v", err)
	}
	log.Printf("Авторизован как: %s", bot.Self.UserName)

	// --- Настройка вебхука ---
	// Получаем базовый URL сервиса от Render или используем localhost для тестирования
	externalURL := os.Getenv("RENDER_EXTERNAL_URL")
	if externalURL == "" {
		// Для локального тестирования можно задать вручную или использовать localhost
		// ВАЖНО: Для локального тестирования вебхуков нужен туннель (ngrok, localtunnel и т.д.)
		externalURL = "http://localhost:8080" // Замените на ваш локальный адрес при тестировании
		log.Println("RENDER_EXTERNAL_URL не найден, использую localhost для тестирования. Для продакшена это не сработает.")
	}
	webhookURL := externalURL + webhookPath

    // Регистрируем вебхук у Telegram
    wh, err := tgbotapi.NewWebhook(webhookURL) // В v5 NewWebhook возвращает только WebhookConfig
    // Исправлено: Используем bot.Request для отправки конфигурации вебхука
    _, err = bot.Request(wh)
    if err != nil {
        log.Fatalf("Ошибка при установке вебхука: %v", err)
    }
    log.Printf("Вебхук установлен на: %s", webhookURL)
	// Запуск скрапинга в отдельной горутине
	go func() {
		for {
			scrapeImages()
			// Уменьшаем интервал для более быстрого обновления при тестировании
			// В продакшене можно вернуть 30 минут
			time.Sleep(10 * time.Minute) // Пауза между проверками
		}
	}()

	// --- Настройка HTTP-сервера ---
	// Обработчик для пути вебхука
	http.HandleFunc(webhookPath, handleWebhook)
	// Обработчик для корневого пути (для проверки и пинга)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Бот запущен и слушает вебхуки на /webhook"))
			log.Println("Получен запрос на корневой путь")
		} else {
			// Если путь неизвестен, возвращаем 404
			http.NotFound(w, r)
		}
	})

	// Получаем порт из переменной окружения PORT, которую Render предоставляет,
	// или используем 8080 по умолчанию для локального тестирования.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Используем 8080 вместо 10000 для локального тестирования
	}

	log.Printf("HTTP-сервер запущен на порту %s", port)
	// Запускаем HTTP-сервер. Это блокирующая операция.
	log.Fatal(http.ListenAndServe(":"+port, nil))
	// --- Конец настройки HTTP-сервера ---
}

// handleWebhook обрабатывает входящие обновления от Telegram
func handleWebhook(w http.ResponseWriter, r *http.Request) {
	var update tgbotapi.Update
	// Декодируем JSON из тела POST-запроса в структуру Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		log.Printf("Ошибка декодирования обновления: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Обрабатываем обновление в отдельной горутине, чтобы не блокировать обработку следующих запросов
	go processUpdate(update)

	// Отвечаем Telegram, что запрос принят (обычно 200 OK достаточно)
	w.WriteHeader(http.StatusOK)
}

// processUpdate - это логика обработки одного обновления (перенос из цикла for)
func processUpdate(update tgbotapi.Update) {
	// Предполагая, что вы хотите сохранять пользователей
	// ВАЖНО: loadUsers() был отключен, поэтому uniqueUsers будет пустым при перезапуске
	// если вы снова включите loadUsers, убедитесь, что saveUsers работает корректно
	// с учетом эфемерной файловой системы Render.
	countUniqueUsers(bot, update)

	// Обработка команды /start
	if update.Message != nil && update.Message.IsCommand() && update.Message.Command() == "start" {
		log.Printf("Пользователь %d запросил /start", update.Message.Chat.ID)
		sendStartMessage(bot, update.Message.Chat.ID)
	} else if update.Message != nil {
		// Обработка текстовых сообщений (кнопок)
		switch update.Message.Text {
		case "Расписание А":
			log.Printf("Пользователь %d запросил расписание А", update.Message.Chat.ID)
			sendSchedule(bot, update.Message.Chat.ID, "A")
		case "Расписание Б":
			log.Printf("Пользователь %d запросил расписание Б", update.Message.Chat.ID)
			sendSchedule(bot, update.Message.Chat.ID, "B")
		case "Поддержка и предложения":
			log.Printf("Пользователь %d запросил поддержку/предложения", update.Message.Chat.ID)
			sendSupportMessage(bot, update.Message.Chat.ID)
		}
	}
	// ... обработка других типов обновлений (CallbackQuery и т.д., если нужно) ...
}

// scrapeImages парсит страницу и обновляет расписания для корпусов А и Б
func scrapeImages() {
	log.Println("Начинаем скрапинг...")

	// Очищаем старые данные
	mu.Lock()
	scheduleA = make(map[string]time.Time)
	scheduleB = make(map[string]time.Time)
	log.Println("Очищаем старые данные расписаний")
	mu.Unlock()

	c := colly.NewCollector()

	// Ищем все изображения расписания по паттерну в src
	c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
		imageSrc := e.Attr("src")
		altText := e.Attr("alt")
		log.Printf("Найдено изображение расписания: src='%s', alt='%s'", imageSrc, altText)

		// Извлекаем информацию из пути к файлу
		// Паттерн: /1Raspisanie/DD.MM[_YYYY]_korpus_[a|v].jpg
		// Год в названии файла игнорируется, всегда используется текущий год
		// Исправлено: Добавлено \.jpe?g в регулярное выражение, чтобы охватить и .jpeg
		re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.\d{4})?_korpus_([av])\.jpe?g$`)
		matches := re.FindStringSubmatch(imageSrc)

		if len(matches) != 4 {
			log.Printf("URL изображения не соответствует ожидаемому формату: %s", imageSrc)
			return
		}

		dayStr := matches[1]   // "5" или "05"
		monthStr := matches[2] // "9" или "09"
		corpusLetter := strings.ToLower(matches[3]) // "a" или "v"

		// Парсим день и месяц
		day, errD := strconv.Atoi(dayStr)
		month, errM := strconv.Atoi(monthStr)
		if errD != nil || errM != nil {
			log.Printf("Ошибка при парсинге дня или месяца из '%s.%s': %v, %v", dayStr, monthStr, errD, errM)
			return
		}

		// Создаем дату с текущим годом
		now := time.Now()
		loc := time.Local // Или time.FixedZone("MSK", 3*60*60)
		// time.Date(год, месяц, день, час, минута, секунда, наносекунда, локация)
		imageDate := time.Date(now.Year(), time.Month(month), day, 0, 0, 0, 0, loc)

		// Формируем абсолютный URL
		// Исправлено: Убраны лишние пробелы из baseURL
		baseURL := "https://kmtko.my1.ru"
		if !strings.HasPrefix(imageSrc, "/") {
			imageSrc = "/" + imageSrc
		}
		// Исправлено: Правильное формирование URL без лишних пробелов
		fullImageURL := strings.TrimRight(baseURL, "/") + imageSrc

		// Определяем корпус и сохраняем
		mu.Lock()
		if corpusLetter == "a" {
			scheduleA[fullImageURL] = imageDate
			log.Printf("Добавлено/обновлено расписание корпуса А: %s (дата: %v)", fullImageURL, imageDate.Format("2006-01-02"))
		} else if corpusLetter == "v" { // Исправлено: 'v' для корпуса "В"
			scheduleB[fullImageURL] = imageDate
			log.Printf("Добавлено/обновлено расписание корпуса Б: %s (дата: %v)", fullImageURL, imageDate.Format("2006-01-02"))
		} else {
			log.Printf("Неизвестный корпус '%s' в URL: %s", corpusLetter, fullImageURL)
		}
		mu.Unlock()
	})

	c.OnRequest(func(r *colly.Request) {
		log.Printf("Посещаем страницу: %s", r.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Printf("Ошибка при запросе %s: %v", r.Request.URL, err)
	})

	// Исправлено: Обработка ошибки от Visit
	err := c.Visit(url)
	if err != nil {
		log.Printf("Ошибка при посещении страницы: %v", err)
		return
	}

	// Логируем содержимое после скрапинга
	mu.RLock()
	log.Printf("Скрапинг завершён. Найдено расписаний: А - %d, Б - %d", len(scheduleA), len(scheduleB))
	mu.RUnlock()
}

// sendSchedule отправляет пользователю изображения расписания для указанного корпуса
func sendSchedule(bot *tgbotapi.BotAPI, chatID int64, corpus string) {
	var scheduleMap map[string]time.Time
	var corpusName string

	switch strings.ToUpper(corpus) {
	case "A":
		scheduleMap = scheduleA
		corpusName = "корпуса А"
	case "B": // Исправлено: 'B' для корпуса "Б"
		scheduleMap = scheduleB
		corpusName = "корпуса Б"
	default:
		msg := tgbotapi.NewMessage(chatID, "Неизвестный корпус.")
		bot.Send(msg)
		return
	}

	mu.RLock()
	defer mu.RUnlock()

	if len(scheduleMap) == 0 {
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("Расписание для %s не найдено.", corpusName))
		bot.Send(msg)
		log.Printf("Для чата %d расписание %s не найдено", chatID, corpusName)
		return
	}

	// Отправляем все найденные изображения для этого корпуса
	for imageURL, imageDate := range scheduleMap {
		log.Printf("Отправляем изображение %s пользователю %d", imageURL, chatID)
		sendImageToTelegram(bot, chatID, imageURL, imageDate)
	}
}

// sendImageToTelegram отправляет одно изображение в Telegram
func sendImageToTelegram(bot *tgbotapi.BotAPI, chatID int64, imageURL string, imageDate time.Time) {
	// Локализация дней недели и месяцев
	daysOfWeek := map[time.Weekday]string{
		time.Monday:    "Понедельник",
		time.Tuesday:   "Вторник",
		time.Wednesday: "Среда",
		time.Thursday:  "Четверг",
		time.Friday:    "Пятница",
		time.Saturday:  "Суббота",
		time.Sunday:    "Воскресенье",
	}
	months := map[time.Month]string{
		time.January:   "января",
		time.February:  "февраля",
		time.March:     "марта",
		time.April:     "апреля",
		time.May:       "мая",
		time.June:      "июня",
		time.July:      "июля",
		time.August:    "августа",
		time.September: "сентября",
		time.October:   "октября",
		time.November:  "ноября",
		time.December:  "декабря",
	}

	// Форматируем дату красиво, включая день недели
	var dateStr string
	weekdayStr := daysOfWeek[imageDate.Weekday()]

	// Всегда показываем дату в текущем году
	dateStr = fmt.Sprintf("%s, %d %s", weekdayStr, imageDate.Day(), months[imageDate.Month()])

	// Исправлено: Используем NewPhoto с FileURL для v5
	msg := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(imageURL))
	msg.Caption = fmt.Sprintf("Расписание на %s", dateStr)
	_, err := bot.Send(msg)
	if err != nil {
		log.Printf("Ошибка при отправке изображения в чат %d (URL: %s): %v", chatID, imageURL, err)
	} else {
		log.Printf("Изображение %s успешно отправлено пользователю %d", imageURL, chatID)
	}
}

// sendStartMessage отправляет приветственное сообщение с клавиатурой
func sendStartMessage(bot *tgbotapi.BotAPI, chatID int64) {
	msg := tgbotapi.NewMessage(chatID, "Привет! Я бот, который отправляет изображения расписания.")

	// Создание клавиатуры с кнопками
	// Размещаем кнопки в две строки
	keyboard := tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Расписание А"),
			tgbotapi.NewKeyboardButton("Расписание Б"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Поддержка и предложения"), // Новая кнопка
		),
	)
	msg.ReplyMarkup = keyboard

	_, err := bot.Send(msg)
	if err != nil {
		log.Printf("Ошибка при отправке стартового сообщения: %v", err)
	} else {
		log.Printf("Стартовое сообщение успешно отправлено пользователю %d", chatID)
	}
}

// sendSupportMessage отправляет пользователю информацию о поддержке
func sendSupportMessage(bot *tgbotapi.BotAPI, chatID int64) {
	// Отправляем сообщение с юзернеймом поддержки
	msg := tgbotapi.NewMessage(chatID, "По вопросам поддержки и предложений обращайтесь к @podkmt")
	_, err := bot.Send(msg)
	if err != nil {
		log.Printf("Ошибка при отправке сообщения поддержки пользователю %d: %v", chatID, err)
	} else {
		log.Printf("Сообщение поддержки успешно отправлено пользователю %d", chatID)
	}
}

// --- Функции для работы с пользователями ---
// (Функции loadUsers, saveUsers, countUniqueUsers остаются без изменений)
// ВАЖНО: loadUsers() был отключен ранее. Если вы снова его включите,
// помните о проблеме с эфемерной файловой системой Render.
func loadUsers() {
	data, err := ioutil.ReadFile("USER1.TXT")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("Файл USER1.TXT не найден, будет создан новый.")
			return
		}
		log.Printf("Ошибка при чтении файла USER1.TXT: %v", err)
		return
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			log.Printf("Некорректная строка в файле USER1.TXT: %s", line)
			continue
		}
		userId, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			log.Printf("Некорректный айди пользователя в файле USER1.TXT: %s", parts[0])
			continue
		}
		uniqueUsers[userId] = parts[1]
	}
	log.Printf("Загружено %d уникальных пользователей из USER1.TXT", len(uniqueUsers))
}

func saveUsers() {
	data := ""
	for userId, username := range uniqueUsers {
		data += fmt.Sprintf("%d:%s\n", userId, username)
	}
	err := ioutil.WriteFile("USER1.TXT", []byte(data), 0644)
	if err != nil {
		log.Printf("Ошибка при записи файла USER1.TXT: %v", err)
	} else {
		log.Printf("Файл USER1.TXT успешно сохранен с %d пользователями.", len(uniqueUsers))
	}
}

func countUniqueUsers(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	if update.Message == nil || update.Message.From == nil {
		return // Игнорируем обновления без сообщения или пользователя
	}
	userId := int64(update.Message.From.ID)
	username := update.Message.From.UserName
	mu.Lock()
	if _, ok := uniqueUsers[userId]; !ok {
		uniqueUsers[userId] = username
		log.Printf("Новый пользователь добавлен: ID=%d, Username=%s", userId, username)
		saveUsers() // Сохраняем при добавлении нового пользователя
	} else {
		// Опционально: обновляем имя пользователя, если оно изменилось
		if uniqueUsers[userId] != username {
			uniqueUsers[userId] = username
			log.Printf("Имя пользователя обновлено: ID=%d, Username=%s", userId, username)
			saveUsers()
		}
	}
	mu.Unlock()
}