package repository

import (
	"api-kafka/internal/entity"
	"database/sql"
	"log"
)

type ProductRepository struct {
	DB *sql.DB
}

func NewProductRepositoryMysql(db *sql.DB) *ProductRepository {
	return &ProductRepository{DB: db}
}

func (r *ProductRepository) Create(product *entity.Product) error {
	statement, err := r.DB.Prepare("INSERT INTO products (id, name, price) VALUES (?, ?, ?)")
	if err != nil {
		log.Println(err)
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(product.ID, product.Name, product.Price)
	if err != nil {
		return err
	}

	return nil
}

func (r *ProductRepository) FindAll() ([]*entity.Product, error) {
	rows, err := r.DB.Query("SELECT * FROM products")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []*entity.Product
	for rows.Next() {
		var product entity.Product

		err = rows.Scan(&product.ID, &product.Name, &product.Price)
		if err != nil {
			return nil, err
		}

		products = append(products, &product)
	}

	return products, nil
}
