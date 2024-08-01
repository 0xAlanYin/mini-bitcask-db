package minibitcask

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

// MiniBitcask simple bitcask implementation in memory
type MiniBitcask struct {
	indexes map[string]int64 // index info in memory，key: entryKey, value: entryOffset in file
	dbFile  *DBFile          // data file
	dirPath string           // data file path
	mu      sync.RWMutex
}

// Open open a DB instance
func Open(dirPath string) (*MiniBitcask, error) {
	// if db file not exist, create it
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// load data file
	dirAbsPath, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}
	dbFile, err := NewDBFile(dirAbsPath)
	if err != nil {
		return nil, err
	}

	// build MiniBitcask instance
	db := &MiniBitcask{
		indexes: make(map[string]int64),
		dbFile:  dbFile,
		dirPath: dirAbsPath,
	}

	// load indexes from data file
	db.loadIndexes()
	return db, nil
}

func (db *MiniBitcask) loadIndexes() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		entry, err := db.dbFile.Read(offset)
		if err != nil {
			// read finish
			if err == io.EOF {
				break
			}
			return
		}

		// set index
		db.indexes[string(entry.Key)] = offset

		// if entry is deleted, delete it in memory
		if entry.Mark == DEL {
			delete(db.indexes, string(entry.Key))
		}

		offset += entry.GetSize()
	}
	return
}

func (db *MiniBitcask) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	// for safe
	db.mu.Lock()
	defer db.mu.Unlock()

	// build entry and write to file
	offset := db.dbFile.Offset
	entry := NewEntry(key, value, PUT)
	err := db.dbFile.Write(entry)
	if err != nil {
		return err
	}

	// write to memory
	db.indexes[string(key)] = offset

	return nil
}

func (db *MiniBitcask) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	// for safe
	db.mu.RLock()
	defer db.mu.RUnlock()

	// get from memory & check exist
	offset, ok := db.indexes[string(key)]
	if !ok {
		return nil, nil
	}

	// get data from file
	entry, err := db.dbFile.Read(offset)
	if err != nil && err != io.EOF { // notice check error != io.EOF
		return
	}
	return entry.Value, nil
}

func (db *MiniBitcask) Delete(key []byte) error {
	if len(key) == 0 {
		return nil
	}

	// for safe
	db.mu.Lock()
	defer db.mu.Unlock()

	// check exist in memory
	_, ok := db.indexes[string(key)]
	if !ok {
		return nil
	}

	// delete in file
	entry := NewEntry(key, nil, DEL)
	err := db.dbFile.Write(entry)
	if err != nil {
		return err
	}

	// delete in memory
	delete(db.indexes, string(key))

	return nil
}

func (db *MiniBitcask) Merge() error {
	// check data is empty
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	// read origin data file and filter valid entries
	for {
		entry, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// index in memory is latest,use to filter valid entries
		off, ok := db.indexes[string(entry.Key)]
		if ok && off == offset {
			validEntries = append(validEntries, entry)
		}
		offset += entry.GetSize()
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// create new tmp data file
	mergeDBFile, err := NewMergeDBFile(db.dirPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(mergeDBFile.File.Name()) // remove the tmp file
	}()

	// write valid entries to new tmp data file
	for _, entry := range validEntries {
		writeOff := mergeDBFile.Offset
		err = mergeDBFile.Write(entry)
		if err != nil {
			return err
		}

		// update memory index
		db.indexes[string(entry.Key)] = writeOff
	}

	// close & remove old data file
	dbFileName := db.dbFile.File.Name()
	_ = db.dbFile.File.Close()
	_ = os.Remove(dbFileName)
	_ = mergeDBFile.File.Close()

	// get new data file name
	mergeDBFileName := mergeDBFile.File.Name()
	// change tmp file to new data file
	_ = os.Rename(mergeDBFileName, filepath.Join(db.dirPath, FileName))

	newDbFile, err := NewDBFile(db.dirPath)
	if err != nil {
		return err
	}

	db.dbFile = newDbFile
	return nil
}

func (db *MiniBitcask) Close() error {
	if db.dbFile == nil {
		return ErrInvalidDBFile
	}
	return db.dbFile.File.Close()
}
