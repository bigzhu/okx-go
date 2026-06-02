package models

import (
	"encoding/json"
	"testing"
)

func TestCandle_UnmarshalJSON_FullCandle(t *testing.T) {
	data := []byte(`["1597026383085","3.721","3.743","3.677","3.708","8422410","22698348.04","12698348.04","0"]`)

	var candle Candle
	err := json.Unmarshal(data, &candle)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if candle.TS != "1597026383085" {
		t.Errorf("TS = %q, want %q", candle.TS, "1597026383085")
	}
	if candle.O != "3.721" {
		t.Errorf("O = %q, want %q", candle.O, "3.721")
	}
	if candle.H != "3.743" {
		t.Errorf("H = %q, want %q", candle.H, "3.743")
	}
	if candle.L != "3.677" {
		t.Errorf("L = %q, want %q", candle.L, "3.677")
	}
	if candle.C != "3.708" {
		t.Errorf("C = %q, want %q", candle.C, "3.708")
	}
	if candle.Vol != "8422410" {
		t.Errorf("Vol = %q, want %q", candle.Vol, "8422410")
	}
	if candle.VolCcy != "22698348.04" {
		t.Errorf("VolCcy = %q, want %q", candle.VolCcy, "22698348.04")
	}
	if candle.VolCcyQuote != "12698348.04" {
		t.Errorf("VolCcyQuote = %q, want %q", candle.VolCcyQuote, "12698348.04")
	}
	if candle.Confirm != "0" {
		t.Errorf("Confirm = %q, want %q", candle.Confirm, "0")
	}
}

func TestCandle_UnmarshalJSON_IndexCandle(t *testing.T) {
	data := []byte(`["1597026383085","3.721","3.743","3.677","3.708","1"]`)

	var candle Candle
	err := json.Unmarshal(data, &candle)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if candle.TS != "1597026383085" {
		t.Errorf("TS = %q, want %q", candle.TS, "1597026383085")
	}
	if candle.O != "3.721" {
		t.Errorf("O = %q, want %q", candle.O, "3.721")
	}
	if candle.H != "3.743" {
		t.Errorf("H = %q, want %q", candle.H, "3.743")
	}
	if candle.L != "3.677" {
		t.Errorf("L = %q, want %q", candle.L, "3.677")
	}
	if candle.C != "3.708" {
		t.Errorf("C = %q, want %q", candle.C, "3.708")
	}
	if candle.Confirm != "1" {
		t.Errorf("Confirm = %q, want %q", candle.Confirm, "1")
	}
	if candle.Vol != "" {
		t.Errorf("Vol = %q, want empty string", candle.Vol)
	}
	if candle.VolCcy != "" {
		t.Errorf("VolCcy = %q, want empty string", candle.VolCcy)
	}
	if candle.VolCcyQuote != "" {
		t.Errorf("VolCcyQuote = %q, want empty string", candle.VolCcyQuote)
	}
}

func TestCandle_UnmarshalJSON_TooShort(t *testing.T) {
	data := []byte(`["1597026383085","3.721","3.743"]`)

	var candle Candle
	err := json.Unmarshal(data, &candle)
	if err == nil {
		t.Fatal("expected error for short array, got nil")
	}
}

func TestCandle_UnmarshalJSON_Array(t *testing.T) {
	data := []byte(`[["1597026383085","3.721","3.743","3.677","3.708","8422410","22698348.04","12698348.04","0"],["1597026383086","3.722","3.744","3.678","3.709","8422411","22698349.04","12698349.04","1"]]`)

	var candles []Candle
	err := json.Unmarshal(data, &candles)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(candles) != 2 {
		t.Fatalf("len(candles) = %d, want 2", len(candles))
	}

	if candles[0].TS != "1597026383085" {
		t.Errorf("candles[0].TS = %q, want %q", candles[0].TS, "1597026383085")
	}
	if candles[0].C != "3.708" {
		t.Errorf("candles[0].C = %q, want %q", candles[0].C, "3.708")
	}
	if candles[0].Confirm != "0" {
		t.Errorf("candles[0].Confirm = %q, want %q", candles[0].Confirm, "0")
	}

	if candles[1].TS != "1597026383086" {
		t.Errorf("candles[1].TS = %q, want %q", candles[1].TS, "1597026383086")
	}
	if candles[1].C != "3.709" {
		t.Errorf("candles[1].C = %q, want %q", candles[1].C, "3.709")
	}
	if candles[1].Confirm != "1" {
		t.Errorf("candles[1].Confirm = %q, want %q", candles[1].Confirm, "1")
	}
}
