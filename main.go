package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"strconv"
	"time"
)

type Product struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
}

var (
	redisClient *redis.Client
	db          *sql.DB
)

func init() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
}

func createProduct(conn *pgx.Conn, product Product) error {
	_, err := conn.Exec(context.Background(), "INSERT INTO products (id, name, description, price) VALUES ($1, $2, $3, $4)",
		product.ID, product.Name, product.Description, product.Price)
	if err != nil {
		return err
	}
	return nil
}

func insertProducts(conn *pgx.Conn, product Product) {
	products := []Product{
		{ID: 1, Name: "Product 1", Description: "Description 1", Price: 10.99},
		{ID: 2, Name: "Product 2", Description: "Description 2", Price: 20.49},
	}

	for _, p := range products {
		err := createProduct(conn, p)
		if err != nil {
			log.Println("Error creating product:", err)
		}
	}
}

func getProductByIDHandler(w http.ResponseWriter, r *http.Request) {
	// Extract product ID from request URL
	idStr := r.URL.Path[len("/products/"):]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid product ID", http.StatusBadRequest)
		return
	}

	cachedData, err := redisClient.Get(r.Context(), "product:"+strconv.Itoa(id)).Result()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(cachedData))
		return
	}

	product := getProductFromDB(id)
	if product == nil {
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}

	jsonData, err := json.Marshal(product)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	err = redisClient.Set(r.Context(), "product:"+strconv.Itoa(id), jsonData, 10*time.Minute).Err()
	if err != nil {
		log.Println("Error caching product data:", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}

func getProductFromDB(id int) *Product {
	row := db.QueryRow("SELECT id, name, description, price FROM products WHERE id = $1", id)

	var product Product
	if err := row.Scan(&product.ID, &product.Name, &product.Description, &product.Price); err != nil {
		return nil
	}

	return &product
}

func main() {
	const (
		connString = "postgresql://postgres:1234@localhost:5432/postgres"
	)

	var err error
	db, err = sql.Open("postgres", connString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/products/", getProductByIDHandler)

	log.Println("Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
