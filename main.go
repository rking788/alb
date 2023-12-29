package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"github.com/davidbyttow/govips/v2/vips"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

func main() {

	//vips.Startup(nil)
	//defer vips.Shutdown()

	heic := flag.String("heic-in", "", "A HEIC file to test conversion to JPEG")
	libraryPath := flag.String("library", "", "Path to the Photos Library")
	outputPath := flag.String("output", "", "Path where the synced libraries should be written")
	flag.Parse()

	if *heic != "" {
		exportConvertedHEIC(*heic, "./output")
	} else {
		if *libraryPath == "" || *outputPath == "" {
			fmt.Printf("Forgot to specify a library or output path\n")
			os.Exit(1)
		}
		err := sync(*libraryPath, *outputPath)
		if err != nil {
			fmt.Printf("Exiting with error. [err:%s]\n", err.Error())
			os.Exit(1)
		}
	}
}

func sync(libraryPath, outputPath string) error {

	db, err := sql.Open("sqlite3", path.Join(libraryPath, "database", "Photos.sqlite?mode=ro"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	entityValue, err := findAlbumEntityValue(db)
	if err != nil {
		return err
	}

	fmt.Printf("Found album entity value. [value:%d]\n", entityValue)

	albums, err := findAlbums(db, entityValue)
	if err != nil {
		return err
	}
	for _, album := range albums {
		err = syncAlbum(db, album, libraryPath, outputPath)
		if err != nil {
			return err
		}
	}

	return nil
}

func syncAlbum(db *sql.DB, album *AlbumMetadata, libraryBase, outputBase string) error {

	fmt.Printf("Syncing %s with %d assets\n", album.Name, len(album.AssetIDs))
	// Make sure the directory exists
	dir := path.Join(outputBase, album.Name)
	_, err := os.Stat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		err = os.Mkdir(dir, 0755)
		if err != nil {
			return err
		}
	}

	// Loop over all assets
	for _, id := range album.AssetIDs {
		// if it already exists, don't overwrite
		// convert to jpeg from heic if necessary
		srcDir, srcFilename, err := findAssetLocation(db, id)
		if err != nil {
			return err
		}

		// If srcFilename has a HEIC extension, replace it, the image will be converted later
		dstFilename := srcFilename
		if strings.HasSuffix(srcFilename, ".heic") {
			dstFilename = strings.ReplaceAll(dstFilename, ".heic", ".jpeg")
		}

		outputPath := path.Join(outputBase, album.Name, dstFilename)
		info, err := os.Stat(outputPath)
		if info != nil {
			fmt.Printf("Skipping file since it already exists. [filename:%s]\n", srcFilename)
			continue
		}
		outF, err := os.Create(outputPath)
		if err != nil {
			return err
		}
		defer outF.Close()

		var src io.Reader
		inputPath := path.Join(libraryBase, "originals", srcDir, srcFilename)
		inF, err := os.Open(inputPath)
		if err != nil {
			return err
		}
		defer inF.Close()

		if strings.HasSuffix(srcFilename, "heic") {
			jpegData, _, err := convertHEIC(inF)
			if err != nil {
				return err
			}

			src = bytes.NewBuffer(jpegData)
		} else {
			// Not HEIC so just directly copy
			src = inF
		}

		_, err = io.Copy(outF, src)
		if err != nil {
			return err
		}

		err = outF.Sync()
		if err != nil {
			return err
		}
	}

	return nil
}

func findAssetLocation(db *sql.DB, id int) (string, string, error) {

	row := db.QueryRow(fmt.Sprintf(`SELECT ZDIRECTORY, ZFILENAME FROM ZASSET WHERE Z_PK = %d`, id))

	var dir, filename string
	err := row.Scan(&dir, &filename)
	if err != nil {
		return "", "", err
	}

	return dir, filename, nil
}

type AlbumMetadata struct {
	PK       int
	Name     string
	AssetIDs []int
}

func findAlbums(db *sql.DB, entityID int) ([]*AlbumMetadata, error) {

	rows, err := db.Query(fmt.Sprintf(`SELECT Z_PK, ZTITLE FROM ZGENERICALBUM WHERE Z_ENT = %d`, entityID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	albums := make([]*AlbumMetadata, 0, 10)
	for rows.Next() {
		var pk int
		var name sql.NullString

		err = rows.Scan(&pk, &name)
		if err != nil {
			return nil, err
		} else if !name.Valid || name.String == "" || strings.HasPrefix(name.String, "progress-") {
			// Sorry to all those out there that have album names like this; but it seems ilke
			// Photos.app holds some special albums that starts with 'progress-'
			continue
		}

		assetIDs, err := findAssetIDsForAlbum(db, entityID, pk)
		if err != nil {
			return nil, err
		}
		albums = append(albums, &AlbumMetadata{PK: pk, Name: name.String, AssetIDs: assetIDs})
	}

	return albums, nil
}

func findAssetIDsForAlbum(db *sql.DB, entityID, pk int) ([]int, error) {

	// Asset ID column name is something like 'Z_NNASSETS'
	columnNamePattern := regexp.MustCompile(`Z_\d+ASSETS`)
	columnNameRows, err := db.Query(fmt.Sprintf(`SELECT name FROM pragma_table_info('Z_%dASSETS')`, entityID))
	if err != nil {
		return nil, err
	}

	var columnName string
	for columnNameRows.Next() {
		var name string
		err = columnNameRows.Scan(&name)
		if err != nil {
			return nil, err
		}

		if columnNamePattern.MatchString(name) {
			columnName = name
			break
		}
	}
	if columnName == "" {
		return nil, fmt.Errorf("could not find a column name containing the asset ID values")
	}

	rows, err := db.Query(fmt.Sprintf(`SELECT %s FROM Z_%dASSETS WHERE Z_%dALBUMS = %d`, columnName, entityID, entityID, pk))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]int, 0, 100)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}

		results = append(results, id)
	}

	return results, nil
}

func findAlbumEntityValue(db *sql.DB) (int, error) {

	rows, err := db.Query("SELECT tbl_name FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	pat := regexp.MustCompile(`Z_(\d+)ASSETS`)
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return -1, err
		}

		matches := pat.FindStringSubmatch(tableName)
		if len(matches) < 2 {
			continue
		}

		return strconv.Atoi(matches[1])
	}

	return -1, nil
}

func exportConvertedHEIC(inPath string, outDir string) error {

	inF, err := os.Open(inPath)
	if err != nil {
		return err
	}
	data, _, err := convertHEIC(inF)

	filename := filepath.Base(inPath)
	ext := filepath.Ext(filename)
	newName := strings.Replace(filename, ext, ".jpg", 1)
	err = os.WriteFile(path.Join(outDir, newName), data, 0644)

	return err
}

func convertHEIC(input io.Reader) ([]byte, *vips.ImageMetadata, error) {

	img, err := vips.NewImageFromReader(input)
	if err != nil {
		return nil, nil, err
	}

	fmt.Printf("Identified file type: %+v.\n", vips.ImageTypes[img.Format()])

	params := vips.NewJpegExportParams()
	return img.ExportJpeg(params)
}
