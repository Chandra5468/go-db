package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const version = "1.0.1"

type (
	// Logger interface{
	// 	Fatal(string, ...interface{})
	// 	Error(string, ...interface{})
	// 	Warn(string, ...interface{})
	// 	Info(string, ...interface{})
	// 	Debug(string, ...interface{})
	// 	Trace(string, ...interface{})

	// }
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		// log Logger
	}
)

// type Options struct{
// 	Logger
// }

func New(dir string) (*Driver, error) {
	dir = filepath.Clean(dir)
	// opts := Options{}

	// if options != nil {
	// 	opts = options
	// }

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
	}

	if _, err := os.Stat(dir); err != nil {
		log.Printf("Using %s (database already exists) \n", dir)
		return &driver, nil
	}

	log.Printf("Creating the database at %s \n", dir)

	return &driver, os.MkdirAll(dir, 0755) // giving access permission 0755
}

func stat(path string) (fi os.FileInfo, err error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}

	return fi, err
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to save record")
	}
	if resource == "" {
		return fmt.Errorf("missing resource - unable to save record (no name)")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")

	if err := encoder.Encode(v); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to read record")
	}
	if resource == "" {
		return fmt.Errorf("missing resource - unable to read record")
	}

	record := filepath.Join(d.dir, collection, resource+".json")

	file, err := os.Open(record)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(v)
}

func (d *Driver) StreamAll(collection string) (<-chan User, <-chan error) {
	out := make(chan User)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)

		dir := filepath.Join(d.dir, collection)
		files, err := os.ReadDir(dir)
		if err != nil {
			errc <- err
			return
		}

		for _, file := range files {
			fpath := filepath.Join(dir, file.Name())
			f, err := os.Open(fpath)
			if err != nil {
				errc <- err
				return
			}

			var u User
			dec := json.NewDecoder(f)
			if err := dec.Decode(&u); err != nil {
				f.Close()
				errc <- err
				return
			}
			f.Close()

			out <- u
		}
	}()

	return out, errc
}

// func (d *Driver) Delete() {

// }

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"

	db, err := New(dir)

	if err != nil {
		log.Println("error ", err)
	}

	employees := []User{
		{"John", "23", "9101910191", "Myrl Tech", Address{"bangalore", "karnataka", "india", "509101"}},
		{"Bon", "23", "9101910191", "Gugul Tech", Address{"bangalore", "karnataka", "india", "509101"}},
		{"Don", "23", "9101910191", "Bulbul Tech", Address{"bangalore", "karnataka", "india", "509101"}},
		{"Mon", "23", "9101910191", "Juljul Tech", Address{"bangalore", "karnataka", "india", "509101"}},
		{"Kon", "23", "9101910191", "dul Tech", Address{"bangalore", "karnataka", "india", "509101"}},
	}

	for _, value := range employees {
		db.Write("users", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	users, errs := db.StreamAll("users")

	for user := range users {
		fmt.Println("Streamed user:", user)
	}
	if err := <-errs; err != nil {
		log.Println("Stream error:", err)
	}

	// fmt.Println(records) // These are in json. Do unmarshalling to understand through structs

	// allUsers := []User{}

	// for _, f := range users {
	// 	employeeFound := User{}

	// 	if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
	// 		log.Println("Error ", err)
	// 	}

	// 	allUsers = append(allUsers, employeeFound)
	// }

	// fmt.Println("All users data is ", allUsers)

	// if err := db.Delete("user", "john"); err != nil {
	// 	fmt.Println("error for delete is ", err)
	// }

}
