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
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Gemini minimal chat wrapper ---
const geminiModel = "gemini-2.5-flash"
const geminiURL = "https://generativelanguage.googleapis.com/v1beta/models/" + geminiModel + ":generateContent"

// Gemini request/response types

type GPart struct {
	Text string `json:"text,omitempty"`
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

func geminiAPIKey() string {
	if v := os.Getenv("GEMINI_API_KEY"); v != "" {
		return v
	}
	// fallback: allow OPENAI style env variable provided by user
	return os.Getenv("GOOGLE_API_KEY")
}

// callGemini generates a reply using Gemini 2.5 Flash
func callGemini(prompt string) (string, error) {
	key := geminiAPIKey()
	if key == "" {
		return "", fmt.Errorf("GEMINI_API_KEY не задан")
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

// --- интеграция с существующим кодом бота (выдержка) ---

// handleChatbotQuestion теперь использует Gemini
func handleChatbotQuestion(chatID, userID int64, question string) {
	log.Printf("Пользователь %d задал вопрос чат-боту: %s", userID, question)
	_, _ = bot.Send(tgbotapi.NewMessage(chatID, "🔄 Обрабатываю ваш запрос, пожалуйста подождите..."))

	answer, err := callGemini(question)
	if err != nil {
		log.Printf("Gemini error: %v", err)
		_, _ = bot.Send(tgbotapi.NewMessage(chatID, "❌ Ошибка генерации ответа. Проверьте ключ GEMINI_API_KEY и повторите попытку."))
		return
	}
	if len(answer) > 4000 { answer = answer[:4000] + "...\n\n[Ответ сокращен из-за ограничений Telegram]" }
	msg := tgbotapi.NewMessage(chatID, answer)
	_, _ = bot.Send(msg)
}

// Ниже оставлены только объявления, чтобы компилить этот отдельный файл как патч поверх проекта
var (
	bot *tgbotapi.BotAPI
)
