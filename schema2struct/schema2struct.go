//Create definitions from the databas
//mainly code from https://github.com/Masterminds/structable/schema2struct
package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	"github.com/urfave/cli"

	_ "github.com/go-sql-driver/mysql"
)

const Usage = `Read a schema and generate Structable structs.`

const structTemplate = `
// {{.StructName}} maps to database table {{.TableName}}
type {{.StructName}} struct{
    {{range .Fields}}{{.}}
    {{end}}
}`

var tag = "db"

type structDesc struct {
	StructName string
	TableName  string
	Fields     []string
}

func main() {
	app := cli.NewApp()
	app.Name = "schema2struct"
	app.Usage = Usage
	app.Action = importTables
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "driver,d",
			Value: "mysql",
			Usage: "The name of the SQL driver to use.",
		},
		cli.StringFlag{
			Name:  "tables,t",
			Value: "",
			Usage: "The list of tables to generate, comma separated. If none specified, the entire schema is used.",
		},
		cli.StringFlag{
			Name:  "file,f",
			Value: "",
			Usage: "The file to send the output.",
		},
		cli.StringFlag{
			Name:  "user,u",
			Value: "root",
			Usage: "The user of the DB.",
		},
		cli.StringFlag{
			Name:  "password,p",
			Value: "",
			Usage: "The password of the Database.",
		},
		cli.StringFlag{
			Name:  "database,D",
			Value: "",
			Usage: "Database to use.",
		},
		cli.StringFlag{
			Name:  "host,H",
			Value: "127.0.0.1",
			Usage: "Connect to host.",
		},
		cli.IntFlag{
			Name:  "port,P",
			Value: 3306,
			Usage: "Port number to use for connection.",
		},
	}
	app.Run(os.Args)
}

func driver(c *cli.Context) string {
	return c.String("driver")
}

func user(c *cli.Context) string {
	return c.String("user")
}

func password(c *cli.Context) string {
	return c.String("password")
}

func host(c *cli.Context) string {
	return c.String("host")
}

func port(c *cli.Context) int {
	return c.Int("port")
}

func schema(c *cli.Context) string {
	return c.String("database")
}

func dest(c *cli.Context) io.Writer {
	if out := c.String("file"); out != "" {
		f, err := os.Create(out)
		if err != nil {
			panic(err)
		}
		return f
	}
	return os.Stdout
}

func conn(c *cli.Context) string {
	var conn string
	if driver(c) == "mysql" {
		conn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user(c), password(c), host(c), port(c), schema(c))
	}
	return conn
}

func tableList(c *cli.Context) []string {
	z := c.String("tables")
	if z != "" {
		return strings.Split(z, ",")
	}
	return []string{}
}

func cxdie(c *cli.Context, err error) {
	fmt.Fprintf(os.Stderr, "Failed to connect to %s (type: %s): %s", conn(c), driver(c), err)
	os.Exit(1)
}

func importTables(c *cli.Context) {
	ttt := template.Must(template.New("st").Parse(structTemplate))
	cxn, err := sql.Open(driver(c), conn(c))
	if err != nil {
		cxdie(c, err)
	}
	// Many drivers defer connections until the first statement. We test
	// that here.
	if err := cxn.Ping(); err != nil {
		cxdie(c, err)
	}
	defer cxn.Close()

	out := dest(c)

	tables := tableList(c)

	if len(tables) == 0 {
		tables, err = listAllTables(cxn, schema(c), driver(c))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot fetch list of tables: %s\n", err)
			os.Exit(2)
		}
	}

	for _, t := range tables {
		f, err := importTable(cxn, schema(c), t, driver(c))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to import table %s: %s", t, err.Error())
		}

		ttt.Execute(out, f)
	}
}

type column struct {
	Name, DataType string
	Max            int64
}

func listAllTables(cxn *sql.DB, schema string, driver string) ([]string, error) {
	var query string
	if driver == "mysql" {
		query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?"
	}
	rows, err := cxn.Query(query, schema)
	res := []string{}
	if err != nil {
		return res, err
	}

	for rows.Next() {
		var s string
		rows.Scan(&s)
		res = append(res, s)
	}

	return res, nil
}

func importTable(cxn *sql.DB, schema string, tbl string, driver string) (*structDesc, error) {
	pks, err := primaryKeyField(cxn, schema, tbl, driver)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting primary keys: %s", err)
	}
	var query string
	switch driver {
	case "mysql":
		query = "SELECT column_name, data_type, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = ? AND table_name = ?"
	case "postgres":
		query = "SELECT column_name, data_type, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = $1 AND table_name = $2"
	}
	rows, err := cxn.Query(query, schema, tbl)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ff := []string{}
	for rows.Next() {
		c := &column{}
		var lenth sql.NullInt64
		if err := rows.Scan(&c.Name, &c.DataType, &lenth); err != nil {
			return nil, err
		}
		c.Max = lenth.Int64
		switch driver {
		case "mysql":
			ff = append(ff, structFieldMySQL(cxn, pks, c, schema, tbl))
		}
	}
	sd := &structDesc{
		StructName: goName(tbl),
		TableName:  tbl,
		Fields:     ff,
	}
	return sd, nil
}

func primaryKeyField(cxn *sql.DB, schema string, tbl string, driver string) ([]string, error) {
	var query string
	switch driver {
	case "mysql":
		query = "SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS c LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t USING(constraint_name, constraint_schema, table_name) WHERE c.constraint_schema = ? AND  c.table_name = ? AND t.constraint_type = 'PRIMARY KEY'"
	}
	rows, err := cxn.Query(query, schema, tbl)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []string{}
	for rows.Next() {
		var s string
		rows.Scan(&s)
		res = append(res, s)
	}
	return res, nil
}

func autoincrementKey(cxn *sql.DB, schema string, tbl string, column string) bool {
	query := "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? AND EXTRA = 'auto_increment'"
	var num int
	if err := cxn.QueryRow(query, schema, tbl, column).Scan(&num); err != nil {
		panic(err)
	}
	return num > 0
}

func structFieldMySQL(cxn *sql.DB, pkgs []string, c *column, schema string, tbl string) string {
	tpl := "%s %s `%s:\"%s\"`"
	//gn := destutter(goName(c.Name), goName(tbl))
	gn := goName(c.Name)
	tt := goType(c.DataType)

	fTag := c.Name
	for _, p := range pkgs {
		if c.Name == p {
			fTag += ",PRIMARY_KEY"
			if autoincrementKey(cxn, schema, tbl, c.Name) {
				fTag += ",AUTO_INCREMENT"
			}
		}
	}
	return fmt.Sprintf(tpl, gn, tt, tag, fTag)
}

// Convert a SQL name to a Go name.
func goName(sqlName string) string {
	// This can definitely be done better.
	goName := strings.Replace(sqlName, "_", " ", -1)
	goName = strings.Replace(goName, ".", " ", -1)
	goName = strings.Title(goName)
	goName = strings.Replace(goName, " ", "", -1)

	return goName
}

// goType takes a SQL type and returns a string containin the name of a Go type.
//
// The goal is not to provide an exact match for every type, but to provide a
// safe Go representation of a SQL type.
//
// For some floating point SQL types, for example, we store them as strings
// so as not to lose precision while also not adding new types.
//
// The default type is string.
func goType(sqlType string) string {
	switch sqlType {
	case "smallint", "smallserial", "tinyint":
		return "int16"
	case "integer", "serial":
		return "int32"
	case "bigint", "bigserial", "int":
		return "int"
	case "real":
		return "float32"
	case "double precision":
		return "float64"
		// Because we need to preserve base-10 precision.
	case "money":
		return "string"
	case "text", "varchar", "char", "character", "character varying", "uuid":
		return "string"
	case "bytea":
		return "[]byte"
	case "boolean":
		return "bool"
	case "timezone", "timezonetz", "date", "time", "timestamp":
		return "time.Time"
	case "interval":
		return "time.Duration"
	}
	return "string"
}

func destutter(str, prefix string) string {
	return strings.TrimPrefix(str, prefix)
}
