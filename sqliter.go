package sqliter

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type Sqliter struct {
	db                       *sql.DB
	statementQueue           map[string]StatementAndArguments
	statementQueueMutex      sync.RWMutex
	statementQueueContext    context.Context
	statementQueueCancel     context.CancelFunc
	writeStatementChan       chan (StatementAndArguments)
	writeTimeoutMilliseconds int64
	writePollingDelay        time.Duration
}

type StatementAndArguments struct {
	Id        string
	Statement *sql.Stmt
	Arguments []any
	Executed  bool
	Result    sql.Result
	Error     error
}

func Open(filename string) (*Sqliter, error) {
	return OpenWithPollingAndTimeout(filename, 5000, 50)
}

func OpenWithPollingAndTimeout(filename string, writeTimeoutMilliseconds int64, writePollingDelay time.Duration) (*Sqliter, error) {
	db, err := sql.Open("sqlite", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", filename, err)
	}

	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to exec journal_mode pragma: %w", err)
	}
	_, err = db.Exec("PRAGMA foreign_keys = 1")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to exec foreign_keys pragma: %w", err)
	}

	writeStatementChan := make(chan StatementAndArguments)
	statementQueue := make(map[string]StatementAndArguments)

	ctx := context.Background()
	statementQueueContext, statementQueueCancel := context.WithCancel(ctx)

	opened := Sqliter{
		db:                       db,
		statementQueue:           statementQueue,
		writeStatementChan:       writeStatementChan,
		statementQueueContext:    statementQueueContext,
		statementQueueCancel:     statementQueueCancel,
		writeTimeoutMilliseconds: writeTimeoutMilliseconds,
		writePollingDelay:        writePollingDelay,
	}

	go opened.doWrites()

	return &opened, nil
}

func (s *Sqliter) Close() {
	if s.db == nil {
		return
	}

	s.statementQueueCancel()
	s.db.Close()
}

func (s *Sqliter) doWrites() {
	if s.db == nil {
		return
	}

	for {
		select {
		case <-s.statementQueueContext.Done():
			return
		case data := <-s.writeStatementChan:
			s.doChanWrite(data)
		}
	}
}

func (s *Sqliter) doChanWrite(data StatementAndArguments) {
	if data.Id == "" || data.Statement == nil {
		return
	}

	result, err := data.Statement.Exec(data.Arguments...)
	data.Executed = true
	data.Result = result
	if err != nil {
		data.Error = err
	}
	s.statementQueueMutex.Lock()
	s.statementQueue[data.Id] = data
	s.statementQueueMutex.Unlock()
}

func (s *Sqliter) Exec(statement *sql.Stmt, arguments []any) error {
	if s.db == nil {
		return fmt.Errorf("db not initialized")
	}

	if statement == nil {
		return fmt.Errorf("statement cannot be nil")
	}

	newUuid, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate uuid: %w", err)
	}

	writeId := newUuid.String()
	statementAndArguments := StatementAndArguments{
		Id:        writeId,
		Statement: statement,
		Arguments: arguments,
		Executed:  false,
		Error:     nil,
	}

	s.writeStatementChan <- statementAndArguments
	results := make(chan StatementAndArguments)

	go func() {
		start := time.Now().UnixMilli()
		for {
			now := time.Now().UnixMilli()
			if now-start >= s.writeTimeoutMilliseconds {
				results <- StatementAndArguments{
					Statement: statement,
					Arguments: arguments,
					Executed:  false,
					Error:     fmt.Errorf("failed to locate queued write id %s", writeId),
				}
				break
			}

			s.statementQueueMutex.RLock()
			data, exists := s.statementQueue[writeId]
			s.statementQueueMutex.RUnlock()

			if !exists || !data.Executed {
				time.Sleep(s.writePollingDelay * time.Microsecond)
				continue
			}

			s.statementQueueMutex.Lock()
			delete(s.statementQueue, writeId)
			s.statementQueueMutex.Unlock()

			results <- data
			break
		}
	}()

	result := <-results
	if result.Error != nil {
		return fmt.Errorf("error executing write: %w", result.Error)
	}

	return nil
}

func (s *Sqliter) Prepare(query string) (*sql.Stmt, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	return s.db.Prepare(query)
}

func (s *Sqliter) Query(query string, args ...any) (*sql.Rows, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db not initialized")
	}

	return s.db.Query(query, args)
}
