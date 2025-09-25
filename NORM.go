package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "errors"
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
    baseSiteURL = "https://kmtko.my1.ru"
    targetPath  = "/index/raspisanie_zanjatij_ochno/0-403"
    adminCommandStats = "/stats"
)

// User struct
type User struct {
    ID              int64
    Username        sql.NullString
    FirstSeen       time.Time
    LastSeen        time.Time
    PreferredCorpus sql.NullString
}

type ScheduleItem struct {
    URL        string
    Date       time.Time
    ActualDate time.Time  // Добавляем поле для фактической даты из контента
    FileID     string
    IsValidURL bool       // Флаг валидности URL
}

var (
    bot               *tgbotapi.BotAPI
    db                *pgxpool.Pool
    mu                sync.RWMutex
    scheduleA         = make(map[string]*ScheduleItem)
    scheduleB         = make(map[string]*ScheduleItem)
    lastScrapeSuccess = false
    adminUserID int64 = 535803934 // замените на реальный 
)

// Добавляем функции для валидации и коррекции дат
func parseActualDateFromContent(content string) (time.Time, error) {
    // Ищем паттерн даты в заголовке расписания
    patterns := []string{
        `(\w+)\s+—\s+(\d{1,2})\.(\d{1,2})\.(\d{4})\s+г\.`,           // "пятница — 26.09.2025 г."
        `(\w+)\s+(\d{1,2})\.(\d{1,2})\.(\d{4})\s+г\.`,               // "пятница 26.09.2025 г."
        `(\d{1,2})\.(\d{1,2})\.(\d{4})`,                             // "26.09.2025"
    }
    
    for _, pattern := range patterns {
        re := regexp.MustCompile(pattern)
        matches := re.FindStringSubmatch(content)
        
        if len(matches) >= 4 {
            var day, month, year int
            var err error
            
            if len(matches) == 5 { // с днем недели
                day, err = strconv.Atoi(matches[2])
                if err != nil { continue }
                month, err = strconv.Atoi(matches[3])
                if err != nil { continue }
                year, err = strconv.Atoi(matches[4])
                if err != nil { continue }
            } else { // только дата
                day, err = strconv.Atoi(matches[1])
                if err != nil { continue }
                month, err = strconv.Atoi(matches[2])
                if err != nil { continue }
                year, err = strconv.Atoi(matches[3])
                if err != nil { continue }
            }
            
            return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
        }
    }
    
    return time.Time{}, errors.New("actual date not found in content")
}

func validateScheduleDate(urlDate, actualDate time.Time) bool {
    if actualDate.IsZero() {
        return true // Если не удалось получить фактическую дату, считаем URL валидным
    }
    
    // Проверяем разницу - если больше 7 дней, URL устарел
    diff := actualDate.Sub(urlDate)
    absDiff := diff
    if absDiff < 0 {
        absDiff = -absDiff
    }
    
    return absDiff.Hours() < 168 // 7 дней
}

func correctScheduleURL(originalURL string, actualDate time.Time) string {
    if actualDate.IsZero() {
        return originalURL
    }
    
    // Извлекаем компоненты URL
    re := regexp.MustCompile(`/1Raspisanie/(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?(_korpus_[av]\.jpe?g)`)
    matches := re.FindStringSubmatch(originalURL)
    
    if len(matches) < 4 {
        return originalURL
    }
    
    // Формируем новый URL с корректной датой
    newURL := strings.Replace(originalURL, matches[0], 
        fmt.Sprintf("/1Raspisanie/%02d.%02d.%d%s", 
            actualDate.Day(), 
            actualDate.Month(), 
            actualDate.Year(),
            matches[4]), 1)
    
    log.Printf("URL скорректирован: %s -> %s", originalURL, newURL)
    return newURL
}

// Новые типы и очереди отправки
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

// Обновляем структуру таблицы для хранения фактических дат
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
    _, err := db.Exec(ctx, `
    CREATE TABLE IF NOT EXISTS users (
      id BIGINT PRIMARY KEY,
      username TEXT,
      first_seen TIMESTAMPTZ DEFAULT now(),
      last_seen TIMESTAMPTZ DEFAULT now(),
      preferred_corpus TEXT
    );
    `)
    return err
}

// Обновляем функции загрузки и сохранения кэша
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
        if _, err := tx.Exec(ctx, "insert_schedule", item.URL, "a", item.Date, actualDate, item.FileID, item.IsValidURL); err != nil {
            return fmt.Errorf("tx.Exec INSERT A: %w", err)
        }
    }
    for _, item := range scheduleB {
        var actualDate interface{}
        if !item.ActualDate.IsZero() {
            actualDate = item.ActualDate
        }
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

    if update.Message != nil && update.Message.IsCommand() {
        switch update.Message.Command() {
        case "start":
            sendStartMessage(update.Message.Chat.ID)
        case "stats":
            if update.Message.From != nil && update.Message.From.ID == adminUserID {
                go sendStatsToAdmin(update.Message.Chat.ID)
            } else {
                _, _ = enqueueUserSend(tgbotapi.NewMessage(update.Message.Chat.ID, "У вас нет доступа к этой команде."), 3*time.Second)
            }
        }
    } else if update.Message != nil && update.Message.Text != "" {
        switch update.Message.Text {
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
        default:
            _, _ = enqueueUserSend(tgbotapi.NewMessage(update.Message.Chat.ID, "Выберите кнопку на клавиатуре или напишите команду /start"), 3*time.Second)
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
        
        // Используем фактическую дату для отображения, если доступна
        displayDate := it.Date
        if !it.ActualDate.IsZero() {
            displayDate = it.ActualDate
        }
        
        weekday := weekdays[displayDate.Weekday()]
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

// Обновляем основную функцию скрапинга
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
    
    // Добавляем переменную для хранения HTML контента страницы
    var pageContent string

    c.OnHTML("html", func(e *colly.HTMLElement) {
        pageContent = e.Text
    })

    c.OnHTML(`img[src*="/1Raspisanie/"]`, func(e *colly.HTMLElement) {
        src := e.Attr("src")
        srcClean := strings.Split(src, "?")[0]
        matches := re.FindStringSubmatch(srcClean)
        if len(matches) < 5 {
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

        item := &ScheduleItem{
            URL:        fullURL,
            Date:       urlDate,
            IsValidURL: true,
        }

        // Пытаемся извлечь фактическую дату из контента страницы
        if actualDate, err := parseActualDateFromContent(pageContent); err == nil {
            item.ActualDate = actualDate
            
            // Проверяем валидность URL
            if !validateScheduleDate(urlDate, actualDate) {
                item.IsValidURL = false
                // Корректируем URL
                correctedURL := correctScheduleURL(fullURL, actualDate)
                if correctedURL != fullURL {
                    item.URL = correctedURL
                    item.Date = actualDate // Используем фактическую дату как основную
                }
                log.Printf("Обнаружено расхождение дат для корпуса %s: URL=%s, Actual=%s", 
                    strings.ToUpper(corpus), urlDate.Format("02.01.2006"), actualDate.Format("02.01.2006"))
            }
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

        displayDate := item.Date
        if !item.ActualDate.IsZero() {
            displayDate = item.ActualDate
        }

        if corpus == "a" {
            tempScheduleA[item.URL] = item
            log.Printf("Найдено фото корпуса А: %s (%02d.%02d.%d)%s", 
                item.URL, displayDate.Day(), displayDate.Month(), displayDate.Year(),
                func() string { if !item.IsValidURL { return " [URL скорректирован]" }; return "" }())
        } else {
            tempScheduleB[item.URL] = item
            log.Printf("Найдено фото корпуса Б: %s (%02d.%02d.%d)%s", 
                item.URL, displayDate.Day(), displayDate.Month(), displayDate.Year(),
                func() string { if !item.IsValidURL { return " [URL скорректирован]" }; return "" }())
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
