package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
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
)

// OpenRouter API keys
var openRouterKeys = []string{
	// ЧИТАЙ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ, ЕСЛИ ЗАДАНЫ; иначе используем встроенный список как fallback
}

func getConfiguredKeys() []string {
	// Позволяет задавать ключи через OPENROUTER_KEYS (через запятую)
	if env := os.Getenv("OPENROUTER_KEYS"); env != "" {
		parts := strings.Split(env, ",")
		var res []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				res = append(res, p)
			}
		}
		if len(res) > 0 {
			return res
		}
	}
	return []string{
		"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",
		"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",
		"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",
		"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",
	}
}

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

// OpenRouter API structures
type OpenRouterRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OpenRouterResponse struct {
	Choices []Choice `json:"choices"`
	Error   *APIError `json:"error,omitempty"`
}

type Choice struct {
	Message Message `json:"message"`
}

type APIError struct {
	Message string `json:"message"`
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
)

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

// Функции для работы с OpenRouter API
func getRandomAPIKey() string {
	keys := getConfiguredKeys()
	return keys[rand.Intn(len(keys))]
}

func callOpenRouterAPI(userMessage string) (string, error) {
	apiKey := getRandomAPIKey()
	
	reqBody := OpenRouterRequest{
		Model: "google/gemini-2.0-flash-exp:free",
		Messages: []Message{{Role: "user", Content: userMessage}},
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("ошибка сериализации JSON: %v", err)
	}

	req, err := http.NewRequest("POST", "https://openrouter.ai/api/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("ошибка создания запроса: %v", err)
	}
	// Требуемые заголовки по документации OpenRouter
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	// Используем корректные имена заголовков: Referer и X-Title
	req.Header.Set("Referer", "https://github.com/zefri1/kmtbot")
	req.Header.Set("X-Title", "KMT Schedule Bot")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("ошибка выполнения запроса: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("ошибка чтения ответа: %v", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return "", fmt.Errorf("неавторизован (401): %s", string(body))
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API вернул код %d: %s", resp.StatusCode, string(body))
	}

	var apiResp OpenRouterResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return "", fmt.Errorf("ошибка парсинга JSON ответа: %v", err)
	}
	if apiResp.Error != nil {
		return "", fmt.Errorf("ошибка API: %s", apiResp.Error.Message)
	}
	if len(apiResp.Choices) == 0 {
		return "", fmt.Errorf("API не вернул результат")
	}
	return apiResp.Choices[0].Message.Content, nil
}

// Остальной код без изменений...
