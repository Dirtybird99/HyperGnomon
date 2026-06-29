package dbbench

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

type sqliteEngine struct {
	db       *sql.DB
	insChunk *sql.Stmt // persistent full-chunk upsert, reused per batch
}

func openSqlite(dir string) (engine, error) {
	sync := "OFF"
	if durability == "durable" {
		sync = "FULL"
	}
	// WITHOUT ROWID makes the BLOB primary key the clustered B-tree, so range
	// scans on k are ordered index scans — the fair analog of bbolt's key order.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(%s)&_pragma=busy_timeout(5000)"+
		"&_pragma=cache_size(-65536)&_pragma=temp_store(memory)",
		filepath.Join(dir, "bench.sqlite"), sync)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // single connection: pragmas + WAL apply consistently
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID`); err != nil {
		db.Close()
		return nil, err
	}
	insChunk, err := db.Prepare(multiRowUpsert(sqlInsertChunk))
	if err != nil {
		db.Close()
		return nil, err
	}
	return &sqliteEngine{db: db, insChunk: insChunk}, nil
}

func (e *sqliteEngine) name() string { return "sqlite" }

// sqlInsertChunk is how many rows go into one multi-row INSERT statement. Folding
// 1000 single-row Execs into ~10 multi-row Execs cuts the per-Exec database/sql
// machinery (the dominant write-path allocation) and SQL round-trips. 100 rows =
// 200 bound params, far under sqlite's variable limit.
const sqlInsertChunk = 100

// multiRowUpsert builds an n-row "INSERT … VALUES (?,?),… ON CONFLICT DO UPDATE".
func multiRowUpsert(n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO kv(k, v) VALUES ")
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?)")
	}
	b.WriteString(" ON CONFLICT(k) DO UPDATE SET v = excluded.v")
	return b.String()
}

func (e *sqliteEngine) batchWrite(pairs []kv) error {
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	// One reusable args buffer for the full-chunk statement.
	exec := func(stmt *sql.Stmt, rows []kv) error {
		args := make([]any, 0, len(rows)*2)
		for _, p := range rows {
			args = append(args, p.k, p.v)
		}
		_, err := stmt.Exec(args...)
		return err
	}

	if len(pairs) >= sqlInsertChunk {
		full := tx.Stmt(e.insChunk) // reuse the persistent prepared statement in this tx
		for i := 0; i+sqlInsertChunk <= len(pairs); i += sqlInsertChunk {
			if err := exec(full, pairs[i:i+sqlInsertChunk]); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	if rem := len(pairs) % sqlInsertChunk; rem > 0 {
		tail, err := tx.Prepare(multiRowUpsert(rem))
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		err = exec(tail, pairs[len(pairs)-rem:])
		tail.Close()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (e *sqliteEngine) get(key []byte) ([]byte, error) {
	var v []byte
	err := e.db.QueryRow(`SELECT v FROM kv WHERE k = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return v, err
}

func (e *sqliteEngine) rangeScan(lo, hi []byte) (int, error) {
	rows, err := e.db.Query(`SELECT v FROM kv WHERE k >= ? AND k < ? ORDER BY k`, lo, hi)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v []byte
		if err := rows.Scan(&v); err != nil {
			return n, err
		}
		n++
		sink += len(v)
	}
	return n, rows.Err()
}

func (e *sqliteEngine) scanAll() (int, error) {
	rows, err := e.db.Query(`SELECT v FROM kv`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var v []byte
		if err := rows.Scan(&v); err != nil {
			return n, err
		}
		n++
		sink += len(v)
	}
	return n, rows.Err()
}

func (e *sqliteEngine) close() error {
	if e.insChunk != nil {
		e.insChunk.Close()
	}
	return e.db.Close()
}
