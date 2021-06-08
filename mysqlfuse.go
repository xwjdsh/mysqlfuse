package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	fileMode = 0644 | uint32(syscall.S_IFREG)
	dirMode  = 0755 | uint32(syscall.S_IFDIR)
)

type FileIndex struct {
	path string
	name string
}

var tableMap sync.Map // map[string]*sync.Map, value sync.Map is map[string]*FileIndex

type MySQLRoot struct {
	fs.Inode
	dsn   string
	db    *sql.DB
	debug bool
}

func NewMySQLRoot(dsn string, db *sql.DB, debug bool) *MySQLRoot {
	return &MySQLRoot{
		dsn:   dsn,
		db:    db,
		debug: debug,
	}
}

var (
	_ = (fs.NodeOnAdder)((*MySQLRoot)(nil))
	_ = (fs.NodeLookuper)((*MySQLRoot)(nil))
	_ = (fs.NodeReaddirer)((*MySQLRoot)(nil))
	_ = (fs.NodeOpendirer)((*MySQLRoot)(nil))
)

func (r *MySQLRoot) OnAdd(ctx context.Context) {
	path := r.Path(nil)
	if r.debug {
		fmt.Printf("OnAdd: [%s]\n", path)
	}

	db, err := sql.Open("mysql", r.dsn)
	if err != nil {
		panic(err)
	}
	r.db = db
}

func (r *MySQLRoot) Opendir(ctx context.Context) syscall.Errno {
	path := r.Path(nil)
	if r.debug {
		fmt.Printf("Opendir: [%s]\n", path)
	}

	return fs.OK
}

func (r *MySQLRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	path := r.Path(nil)
	if r.debug {
		fmt.Printf("Readdir: [%s]\n", path)
	}

	var (
		mode     uint32
		elements []string
	)
	if path == "" {
		mode = dirMode
		// root dir, fetch tables
		var err error
		elements, err = r.getTables(ctx)
		if err != nil {
			return nil, syscall.ENOENT
		}

		// rebuild index map
		tableMap.Range(func(key, value interface{}) bool {
			tableMap.Delete(key)
			return true
		})
		for _, ele := range elements {
			tableMap.Store(ele, &sync.Map{})
		}
	} else {
		mode = fileMode
		// table dir, fetch records
		ids, err := r.getRecordIDs(ctx, path)
		if err != nil {
			return nil, syscall.ENOENT
		}
		for _, id := range ids {
			elements = append(elements, fmt.Sprintf("%d.sql", id))
		}
		value, _ := tableMap.Load(path)
		// if not found, panic directly
		m := value.(*sync.Map)
		m.Range(func(k, v interface{}) bool {
			m.Delete(k)
			return true
		})
		for _, ele := range elements {
			p := filepath.Join(path, ele)
			m.Store(p, &FileIndex{path: p, name: ele})
		}
	}

	list := []fuse.DirEntry{}
	for _, ele := range elements {
		d := fuse.DirEntry{
			Name: ele,
			Mode: mode,
		}
		list = append(list, d)
	}

	return fs.NewListDirStream(list), fs.OK
}

func (r *MySQLRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	currentDir := r.Path(nil)
	childPath := filepath.Join(currentDir, name)
	if r.debug {
		fmt.Printf("Lookup: [%s]\n", childPath)
	}

	var mode uint32
	if currentDir == "" {
		mode = dirMode
		// root dir
		if _, ok := tableMap.Load(name); !ok {
			return nil, syscall.ENOENT
		}
	} else {
		mode = fileMode
		v, ok := tableMap.Load(currentDir)
		if !ok {
			return nil, syscall.ENOENT
		}
		m := v.(*sync.Map)
		if _, ok := m.Load(childPath); !ok {
			return nil, syscall.ENOENT
		}
	}

	sa := fs.StableAttr{
		Mode: mode,
	}

	childNode := r.NewInode(ctx, NewMySQLRoot(r.dsn, r.db, r.debug), sa)
	return childNode, fs.OK
}

func (r *MySQLRoot) getTables(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, "SHOW tables")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

func (r *MySQLRoot) getRecordIDs(ctx context.Context, table string) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf("SELECT id FROM `%s`", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}
