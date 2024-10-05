package main

type User struct {
	ID         string   `json:"id"`
	Topic      []string `json:"topic"`
	Difficulty []int    `json:"difficulty"`
	New        bool     `json:"add_cancel"`
}
