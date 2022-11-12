package dblibgo

import (
	"database/sql"
	"strconv"

	loglibgo "github.com/BF-Moritz/log.lib.go"
	"github.com/jmoiron/sqlx"
)

// Conn Connection to DB
type Conn struct {
	db     sqlx.DB
	config ConfigType
	logger *loglibgo.Logger
}

func NewConn(config ConfigType, logger *loglibgo.Logger) (conn *Conn, err error) {
	if logger != nil {
		logger.LogDebug("dblibgo.NewConn()", "creating new connection")
	}

	params := "charset=utf8mb4&parseTime=true&columnsWithAlias=true"
	if config.NoAutoCommit {
		params += "&autocommit=false"
	}

	dataSourceName := config.User + ":" + config.Password + "@tcp(" + config.Host + ":" + strconv.Itoa(config.Port) + ")/" + config.Name + "?" + params
	masterDB, err := sqlx.Open(config.Driver, dataSourceName)
	if err != nil {
		logger.LogError("dblibgo.NewConn()", "failed to connect to database: %s", err)
		return
	}

	masterDB.SetMaxIdleConns(config.MaxIdleConns)
	masterDB.SetMaxOpenConns(config.MaxOpenConns)

	conn = &Conn{
		db:     *masterDB,
		config: config,
		logger: logger,
	}

	return
}

// Query ...
func (conn *Conn) Query(query string, namedArgs interface{}) (rows *sqlx.Rows, err error) {
	if conn.logger != nil {
		conn.logger.LogDebug("dblibgo.Query()", "<%s> with: %+v", query, namedArgs)
	}

	var args []interface{}
	query, args, err = sqlx.Named(query, namedArgs)
	if err != nil {
		if conn.logger != nil {
			conn.logger.LogError("dblibgo.Query()", "failed to convert named query/args: %s", err)
		}
		return
	}

	var expandedQuery string
	expandedQuery, args, err = sqlx.In(query, args...)
	if err != nil {
		if conn.logger != nil {
			conn.logger.LogError("dblibgo.Query()", "failed to expand slice values: %s", err)
		}
		return
	}

	rows, err = conn.db.Queryx(expandedQuery, args...)
	if err != nil && conn.logger != nil {
		conn.logger.LogError("dblibgo.Query()", "query failed: %s", err)
	}

	return
}

// QueryRow ...
func (conn *Conn) QueryRow(query string, namedArgs interface{}) (row *sqlx.Row, err error) {
	if conn.logger != nil {
		conn.logger.LogDebug("dblibgo.QueryRow()", "<%s> with: %+v", query, namedArgs)
	}

	var args []interface{}
	query, args, err = sqlx.Named(query, namedArgs)
	if err != nil {
		if conn.logger != nil {
			conn.logger.LogError("dblibgo.QueryRow()", "failed to convert named query/args: %s", err)
		}
		return
	}

	var expandedQuery string
	expandedQuery, args, err = sqlx.In(query, args...)
	if err != nil {
		if conn.logger != nil {
			conn.logger.LogError("dblibgo.QueryRow()", "failed to expand slice values: %s", err)
		}
		return
	}

	row = conn.db.QueryRowx(expandedQuery, args...)

	return
}

// Exec ...
func (conn *Conn) Exec(query string, namedArgs interface{}) (result sql.Result, err error) {
	if conn.logger != nil {
		conn.logger.LogDebug("dblibgo.Exec()", "<%s> with: %+v", query, namedArgs)
	}

	var q string
	var args []interface{}

	query, args, err = sqlx.Named(query, namedArgs)
	if err != nil {
		if conn.logger != nil {
			conn.logger.LogError("dblibgo.Exec()", "failed to convert named query/args: %s", err)
		}
		return
	}

	q, args, err = sqlx.In(query, args...)
	if err != nil {
		if conn.logger != nil {
			conn.logger.LogError("dblibgo.Exec()", "failed to expand slice values: %s", err)
		}
		return
	}

	result, err = conn.db.Exec(q, args...)
	if err != nil && conn.logger != nil {
		conn.logger.LogError("dblibgo.Exec()", "query failed: %s", err)
	}

	return
}

// Close ...
func (conn *Conn) Close() {
	if conn.logger != nil {
		conn.logger.LogDebug("dblibgo.Close()", "closing connection")
	}

	conn.db.Close()
}
