package datastore

import (
	"log"

	"github.com/jmoiron/sqlx"

	//imported so pq can register its drivers with sql
	_ "github.com/lib/pq"
)

//M is a map
type M map[string]interface{}

//Store holds all database collections
type Store struct {
	DB          *sqlx.DB
	Collections map[string]*Collection
}

func exec(q string, db *sqlx.DB) error {
	if _, err := db.Exec(q); err != nil {
		log.Println("error with query: " + q)
		log.Println(err)
		return err
	}
	return nil
}

//NewConnection creates a sql database connection and contentful client
func NewConnection(uri string, seed func(db *sqlx.DB) error) (*Store, error) {
	conn, err := sqlx.Open("postgres", uri)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	err = seed(conn)
	if err != nil {
		log.Println("seed error")
		return nil, err
	}

	return &Store{
		DB: conn,
	}, nil
}

func (s *Store) AddCollection(tableName string) *Collection {
	if s.Collections == nil {
		s.Collections = make(map[string]*Collection)
	}
	if coll, ok := s.Collections[tableName]; ok {
		return coll
	}
	s.Collections[tableName] = NewCollection(s.DB, tableName)
	return s.Collections[tableName]
}
