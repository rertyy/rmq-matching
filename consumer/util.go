package main

import (
	"encoding/json"
	"fmt"
)

func SerialiseUser(user User) string {
	data, err := json.Marshal(user)
	if err != nil {
		fmt.Println("Error serialising user: ", err)
		return ""
	}
	return string(data)
}

func DeserialiseUser(data string) User {
	var user User
	err := json.Unmarshal([]byte(data), &user)
	if err != nil {
		fmt.Println("Error deserialising user: ", err)
	}
	return user
}
