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

// OpenRouter API keys (rotated header + base URL per latest docs)
var openRouterKeys = []string{
	"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",
	"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",
	"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",
	"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",
}

// Whitelist admin user for unlimited generation
var unlimitedUserIDs = map[int64]bool{
	535803934: true,
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
	ActualDate time.Time
	FileID     string
	IsValidURL bool
}

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

type Choice struct { Message Message `json:"message"` }

type APIError struct { Message string `json:"message"` }

var (
	bot *tgbotapi.BotAPI
	db  *pgxpool.Pool
	mu  sync.RWMutex

	scheduleA         = make(map[string]*ScheduleItem)
	scheduleB         = make(map[string]*ScheduleItem)
	lastScrapeSuccess = false
	adminUserID int64 = 535803934

	userStates      = make(map[int64]string)
	userStatesMutex sync.RWMutex
)

// --- OpenRouter helpers ---
func getRandomAPIKey() string { return openRouterKeys[rand.Intn(len(openRouterKeys))] }

func callOpenRouterAPI(userMessage string) (string, error) {
	apiKey := getRandomAPIKey()

	reqBody := OpenRouterRequest{
		Model: "google/gemini-2.0-flash-exp:free",
		Messages: []Message{{Role: "user", Content: userMessage}},
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil { return "", fmt.Errorf("json marshal: %w", err) }

	// Per OpenRouter docs, prefer router.openrouter.ai and include Referer/X-Title
	req, err := http.NewRequest("POST", "https://router.openrouter.ai/api/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil { return "", fmt.Errorf("new request: %w", err) }
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("HTTP-Referer", "https://github.com/zefri1/kmtbot")
	req.Header.Set("X-Title", "KMT Schedule Bot")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return "", fmt.Errorf("do: %w", err) }
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 401 {
		return "", fmt.Errorf("невалидный ключ или заголовки (401): %s", string(body))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API %d: %s", resp.StatusCode, string(body))
	}
	var apiResp OpenRouterResponse
	if err := json.Unmarshal(body, &apiResp); err != nil { return "", fmt.Errorf("unmarshal: %w", err) }
	if apiResp.Error != nil { return "", fmt.Errorf("API error: %s", apiResp.Error.Message) }
	if len(apiResp.Choices) == 0 { return "", fmt.Errorf("empty choices") }
	return apiResp.Choices[0].Message.Content, nil
}

// --- Limits ---
func setUserState(userID int64, state string) { userStatesMutex.Lock(); userStates[userID] = state; userStatesMutex.Unlock() }
func getUserState(userID int64) string       { userStatesMutex.RLock(); s := userStates[userID]; userStatesMutex.RUnlock(); return s }
func clearUserState(userID int64)            { userStatesMutex.Lock(); delete(userStates, userID); userStatesMutex.Unlock() }

func checkAndUpdateUserLimit(userID int64) (bool, int, error) {
	if unlimitedUserIDs[userID] { // unlimited for whitelisted ids
		return true, 999, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	now := time.Now()
	moscowTZ, _ := time.LoadLocation("Europe/Moscow")
	nowMoscow := now.In(moscowTZ)
	resetTime := time.Date(nowMoscow.Year(), nowMoscow.Month(), nowMoscow.Day(), limitResetHour, 0, 0, 0, moscowTZ)
	if nowMoscow.Before(resetTime) { resetTime = resetTime.AddDate(0, 0, -1) }

	var user User
	err := db.QueryRow(ctx, "SELECT id, COALESCE(chat_requests,0), COALESCE(last_reset_date, now()) FROM users WHERE id=$1", userID).
		Scan(&user.ID, &user.ChatRequests, &user.LastResetDate)
	if err != nil {
		if err == sql.ErrNoRows {
			_, err = db.Exec(ctx, `INSERT INTO users (id, chat_requests, last_reset_date) VALUES ($1, 1, $2)
			ON CONFLICT (id) DO UPDATE SET chat_requests=1, last_reset_date=$2`, userID, resetTime)
			return err == nil, dailyRequestLimit - 1, err
		}
		return false, 0, err
	}
	if user.LastResetDate.Before(resetTime) {
		_, err = db.Exec(ctx, "UPDATE users SET chat_requests=1, last_reset_date=$1 WHERE id=$2", resetTime, userID)
		return err == nil, dailyRequestLimit - 1, err
	}
	if user.ChatRequests >= dailyRequestLimit { return false, 0, nil }
	_, err = db.Exec(ctx, "UPDATE users SET chat_requests = chat_requests + 1 WHERE id=$1", userID)
	remaining := dailyRequestLimit - user.ChatRequests - 1
	if remaining < 0 { remaining = 0 }
	return err == nil, remaining, err
}

// --- the rest of file remains same below ---
