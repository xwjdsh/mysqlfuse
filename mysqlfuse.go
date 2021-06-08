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

type FileIndex struct {
	path string
	name string
}

var tableMap sync.Map // map[string]*sync.Map, value sync.Map is map[string]*FileIndex

type MySQLRoot struct {
	fs.Inode
	dsn   string
	path  string
	db    *sql.DB
	debug bool
}

func NewMySQLRoot(dsn string, path string, db *sql.DB, debug bool) *MySQLRoot {
	return &MySQLRoot{
		dsn:   dsn,
		path:  path,
		db:    db,
		debug: debug,
	}
}

var _ = (fs.NodeOnAdder)((*MySQLRoot)(nil))

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

	if path == "" {
		// root dir, fetch tables
		tables, err := r.getTables(ctx)
		if err != nil {
			return nil, syscall.ENOENT
		}

		// rebuild index map
		tableMap.Range(func(key, value interface{}) bool {
			tableMap.Delete(key)
			return true
		})

		list := []fuse.DirEntry{}
		for i, table := range tables {
			d := fuse.DirEntry{
				Name: table,
				Ino:  uint64(i),
				Mode: 0755 | uint32(syscall.S_IFDIR),
			}
			list = append(list, d)
			tableMap.Store(table, &sync.Map{})
		}

		return fs.NewListDirStream(list), fs.OK
	} else {
		// table dir, fetch records
		ids, err := r.getRecordIDs(ctx, path)
		if err != nil {
			return nil, syscall.ENOENT
		}

		list := []fuse.DirEntry{}
		value, _ := tableMap.Load(path)
		m := value.(*sync.Map)
		m.Range(func(kk, vv interface{}) bool {
			m.Delete(kk)
			return true
		})

		for _, id := range ids {
			d := fuse.DirEntry{
				Name: fmt.Sprintf("%d.sql", id),
				Ino:  uint64(100 + id),
				Mode: 0644 | uint32(syscall.S_IFREG),
			}
			list = append(list, d)
			m.Store(id, &FileIndex{path: path, name: d.Name})
		}
		return fs.NewListDirStream(list), fs.OK
	}
}

func (r *MySQLRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := filepath.Join(r.Path(nil), name)
	if r.debug {
		fmt.Printf("Lookup: [%s]\n", childPath)
	}

	var childNode *fs.Inode
	tableMap.Range(func(key, value interface{}) bool {
		if childPath == key {
			// table dir
			sa := fs.StableAttr{
				Mode: 0755 | uint32(syscall.S_IFDIR),
			}
			childNode = r.NewInode(ctx, NewMySQLRoot(r.dsn, r.path, r.db, r.debug), sa)
			return false
		}

		m := value.(*sync.Map)
		m.Range(func(kk, vv interface{}) bool {
			fi := vv.(*FileIndex)
			if childPath == fi.path {
				sa := fs.StableAttr{
					Mode: 0644 | uint32(syscall.S_IFREG),
				}
				childNode = r.NewInode(ctx, NewMySQLRoot(r.dsn, r.path, r.db, r.debug), sa)
				return false
			}
			return true
		})

		return true
	})

	if childNode != nil {
		return childNode, fs.OK
	}
	return nil, syscall.ENOENT
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
