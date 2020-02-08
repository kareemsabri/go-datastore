package datastore

import (
	"log"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

const BeforeSelect = "bSELECT"
const BeforeInsert = "bINSERT"
const AfterSelect = "aSELECT"
const AfterInsert = "aINSERT"

//Collection holds a table scoped database connection
type Collection struct {
	DB        *sqlx.DB
	Table     string
	Callbacks map[string][]func(q *Query)
}

type Query struct {
	C         *Collection
	queryType string
	criteria  M
	limitTo   int
	orderBy   []string
	lastID    int
	err       error
}

func NewCollection(db *sqlx.DB, tableName string) *Collection {
	return &Collection{
		db,
		tableName,
		make(map[string][]func(q *Query)),
	}
}

func (q *Query) Limit(n int) *Query {
	q.limitTo = n
	return q
}

func (q *Query) Order(by []string) *Query {
	q.orderBy = by
	return q
}

func (q *Query) GetLastID() int {
	return q.lastID
}

func (q *Query) Run(dest interface{}) error {
	if cbs, ok := q.C.Callbacks["b"+q.queryType]; ok {
		for _, cb := range cbs {
			cb(q)
		}
	}

	if q.queryType == "SELECT" {
		q.err = q.C.doFind(dest, q.criteria, q.limitTo, q.orderBy)
	} else if q.queryType == "INSERT" {
		if id, e := q.C.doInsert(q.criteria); e != nil {
			q.lastID = -1
			q.err = e
		} else {
			q.lastID = int(id)
		}
	}

	if cbs, ok := q.C.Callbacks["a"+q.queryType]; ok {
		for _, cb := range cbs {
			cb(q)
		}
	}

	return q.err
}

func (c *Collection) AddCallback(key string, cb func(q *Query)) {
	if _, ok := c.Callbacks[key]; !ok {
		c.Callbacks[key] = []func(q *Query){cb}
	} else {
		c.Callbacks[key] = append(c.Callbacks[key], cb)
	}
}

func (c *Collection) Find(criteria M) *Query {
	return &Query{
		c,
		"SELECT",
		criteria,
		0,
		nil,
		-1,
		nil,
	}
}

func (c *Collection) FindOne(criteria M) *Query {
	return c.Find(criteria).Limit(1)
}

func (c *Collection) Insert(from M) *Query {
	return &Query{
		c,
		"INSERT",
		from,
		0,
		nil,
		-1,
		nil,
	}
}

func (c *Collection) doFind(dest interface{}, criteria M, limit int, orderBy []string) error {
	qp := make([]string, 0, len(criteria))
	values := make([]interface{}, 0, len(criteria))
	for k := range criteria {
		qp = append(qp, k+"=?")
		values = append(values, criteria[k])
	}
	query := "SELECT * FROM " + c.Table
	if len(criteria) > 0 {
		query = query + " WHERE " + strings.Join(qp, " AND ")
	}
	if limit > 0 {
		query = query + " LIMIT " + strconv.Itoa(limit)
	}
	if len(orderBy) > 0 {
		query += " ORDER BY " + strings.Join(orderBy, ", ")
	}
	log.Println(query)
	log.Println(criteria)
	if limit == 1 {
		return c.DB.Get(dest, c.DB.Rebind(query), values...)
	}
	return c.DB.Select(dest, c.DB.Rebind(query), values...)
}

func (c *Collection) doInsert(from M) (int64, error) {
	fields := make([]string, 0, len(from))
	placeholders := make([]string, 0, len(from))
	values := make([]interface{}, 0, len(from))
	for k, v := range from {
		fields = append(fields, k)
		placeholders = append(placeholders, "?")
		values = append(values, v)
	}

	query := `INSERT INTO ` + c.Table + `
	(` + strings.Join(fields, ",") + `)
	VALUES
	(` + strings.Join(placeholders, ",") + `) 
	RETURNING id`

	var id int64
	query = c.DB.Rebind(query)
	err := c.DB.QueryRow(query, values...).Scan(&id)
	if err != nil {
		log.Println("sql error: " + err.Error())
		return -1, err
	}

	return id, err
}

func (c *Collection) Truncate() error {
	_, err := c.DB.Exec(`TRUNCATE ` + c.Table + ` RESTART IDENTITY`)
	if err != nil {
		_, err = c.DB.Exec(`DELETE FROM ` + c.Table)
	}
	if err != nil {
		log.Println("truncate error: " + err.Error())
	}
	return err
}
