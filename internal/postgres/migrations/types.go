package migrations

type Migration struct {
	Version int64
	Name    string

	UpSQL   string
	DownSQL string
}
