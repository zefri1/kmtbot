// Gemini OCR structures
// Добавить после структур Gemini API

// Поддержка изображений для Gemini Vision

type InlineData struct {
    MimeType string `json:"mimeType"`
    Data     string `json:"data"` // base64
}

type GPart struct {
    Text       string      `json:"text,omitempty"`
    InlineData *InlineData `json:"inline_data,omitempty"`
}

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
