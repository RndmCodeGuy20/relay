package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func LoadFromDir(dir string) ([]Migration, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	type temp struct {
		up   string
		down string
		name string
	}

	m := map[int64]*temp{}

	for _, f := range files {
		name := f.Name()
		if f.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}

		version, base, kind, err := parseFile(name)
		if err != nil {
			return nil, err
		}

		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}

		if _, ok := m[version]; !ok {
			m[version] = &temp{name: base}
		}

		if kind == "up" {
			m[version].up = string(content)
		} else {
			m[version].down = string(content)
		}
	}

	var migrations []Migration
	for v, t := range m {
		if t.up == "" {
			return nil, fmt.Errorf("missing up migration for %d", v)
		}

		migrations = append(migrations, Migration{
			Version: v,
			Name:    t.name,
			UpSQL:   t.up,
			DownSQL: t.down,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

func parseFile(name string) (int64, string, string, error) {
	// 001_init.up.sql
	parts := strings.Split(name, "_")
	if len(parts) < 2 {
		return 0, "", "", fmt.Errorf("invalid name: %s", name)
	}

	version, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", "", err
	}

	rest := strings.Join(parts[1:], "_")
	kind := "up"
	if strings.Contains(rest, ".down.sql") {
		kind = "down"
	}

	base := strings.TrimSuffix(rest, ".up.sql")
	base = strings.TrimSuffix(base, ".down.sql")

	return version, base, kind, nil
}
