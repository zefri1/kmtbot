type InlineData struct {
    MimeType string `json:"mimeType"`
    Data     string `json:"data"` // base64
}

type GPart struct {
    Text       string      `json:"text,omitempty"`
    InlineData *InlineData `json:"inline_data,omitempty"`
}
