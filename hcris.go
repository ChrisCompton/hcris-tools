package hcris-tools

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"strconv"

	"github.com/yhat/scrape"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var OutputFile *os.File
var OutputQueue []string

func getPageAndParse(url string) *html.Node {
	resp := GetPage(url)
	root := ParseContent(resp)
	return root
}

func ParseContent(response *http.Response) *html.Node {
	root, err := html.Parse(response.Body)
	Check(err)

	return root
}

func Match(n *html.Node, tag string, text string) bool {
	if n.DataAtom == atom.A && n.Parent != nil && n.Parent.Parent != nil {
		link := string(scrape.Attr(n, tag))
		matched, err := regexp.MatchString(text, link)
		Check(err)

		DebugVerbose(fmt.Sprintf("Link: %s %s\n", link, matched))

		if matched {
			DebugVerbose(fmt.Sprintf("*** Matched: %s\n", link))
			return matched
		}
	}
	return false
}

// ExtractFile - Open a zip archive for reading.
func ExtractFile(file string) {
	OutputQueue = nil

	r, err := zip.OpenReader(file)
	Check(err)

	defer r.Close()

	for _, f := range r.File {
		FileDescriptors := regexp.MustCompile("_").Split(f.Name, -1)
		FileContent := strings.ToUpper(FileDescriptors[len(FileDescriptors)-1])
		fileType := strings.ToLower(regexp.MustCompile(regexp.QuoteMeta(".")).Split(FileContent, -1)[0])

		Debug(fmt.Sprintf("Extracting from %s", f.Name))
		rc, err := f.Open()
		Check(err)

		scanner := bufio.NewScanner(rc)
		for scanner.Scan() {
			ReadCsv(fileType, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			Check(err)
		}

		rc.Close()

		if AppConfig.Settings.Output == "database" {
			WriteQueueToDb(fileType)
		} else {
			WriteQueueToFile(fileType)
		}

		//panic("Development Stop in Content.go")
	}

}

func ReadCsv(content string, data string) {
	r := csv.NewReader(strings.NewReader(data))

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		Check(err)

		// TODO: Check year to see which schema to use.
		switch content {
		case "alpha":
			DebugVerbose("ALPHA CSV OPTION")
			OutputQueue = append(OutputQueue, HandleAlpha(record))
			//go sendToElasticSearch("mcr/"+content+"/"+record[0], HandleAlphaJson(record))
			Check(err)
		case "nmrc":
			DebugVerbose("NMRC CSV OPTION")
			OutputQueue = append(OutputQueue, HandleNumeric(record))
			Check(err)
		case "rollup":
			DebugVerbose("ROLLUP CSV OPTION")
			OutputQueue = append(OutputQueue, HandleRollup(record))
			Check(err)
		case "rpt":
			DebugVerbose("RPT CSV OPTION")
			OutputQueue = append(OutputQueue, HandleReport(record))
			Check(err)
		default:
			DebugVerbose("Could Not Determine Format")
		}

		if AppConfig.Settings.Output == "database" &&
			len(OutputQueue) >= AppConfig.Store.MaxQueue {
			WriteQueueToDb(content)
		}

	}

}

func DownloadFiles(url string, path string, attr string, list string, tag string, prefix string, ext string) []string {
	var fileList []string

	if AppConfig.Settings.Output == "database" {
		SetupDb()
	}

	matchPage := func(n *html.Node) bool {
		return Match(n, attr, list)
	}

	matchFile := func(n *html.Node) bool {
		return Match(n, attr, tag)
	}

	pages := scrape.FindAll(getPageAndParse(url+"/"+path), matchPage)
	for i, page := range pages {
		//if i == 1 {
		pagePath := scrape.Attr(page, attr)
		downloadPage := url + "/" + pagePath
		DebugVerbose(fmt.Sprintf("#%2d Year: %s %s)\n", i, scrape.Text(page), pagePath))

		files := scrape.FindAll(getPageAndParse(downloadPage), matchFile)

		for j, file := range files {
			fileName := GetDataFolder() + "/" + prefix + scrape.Text(file) + ext

			if _, err := os.Stat(fileName); os.IsNotExist(err) {
				Debug(fmt.Sprintf("Downloading to %s\n", fileName))

				filePath := scrape.Attr(file, attr)

				Debug(fmt.Sprintf("\t%2d: %s\n", i, filePath))

				zipFile := GetPage(filePath)

				file, err := os.Create(fileName)
				Check(err)

				_, err = io.Copy(file, zipFile.Body)
				Check(err)

				defer zipFile.Body.Close()
				file.Close()
			}

			j++

			fileList = append(fileList, fileName)
		}
		//}
	}

	return fileList
}

func WriteQueueToFile(fileType string) error {
	OutputFile, err := os.Create(GetDataFolder() + "/mcr." + fileType + ".sql")
	Check(err)

	defer OutputFile.Close()

	result, err := OutputFile.WriteString(BuildQuery(fileType))

	Check(err)
	Debug(fmt.Sprintf("%d", result))

	OutputQueue = nil

	return err
}

func WriteQueueToDb(table string) error {
	Check(SQLiteConnect(GetDataFolder() + "/" + AppConfig.Store.File))
	defer SQLiteClose()

	Debug(fmt.Sprintf("Writing %d Records.", len(OutputQueue)))
	count, err := SQLiteExecute(BuildQuery(table))
	Debug(fmt.Sprintf("Affected: %d", count))

	OutputQueue = nil

	return err
}

func WriteResultToDb(query string) error {
	Check(SQLiteConnect(GetDataFolder() + "/" + AppConfig.Store.File))
	defer SQLiteClose()

	count, err := SQLiteExecute(query)
	Debug(fmt.Sprintf("Affected: %d", count))

	OutputQueue = nil

	return err
}

func CheckExtractFiles() error {
	var err error
	start := 1995
	now := time.Now()
	end := now.Year()

	directory := GetOutputFolder()

	for i := start; i <= end; i++ {
		if (i <= 2011) {
			if _, err := os.Stat(directory + "/hosp_" + strconv.Itoa(i) + "_ALPHA.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp_%d_ALPHA.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp_%d_ALPHA.CSV %s", directory, i))
			}

			if _, err := os.Stat(directory + "/hosp_" + strconv.Itoa(i) + "_NMRC.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp_%d_NMRC.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp_%d_NMRC.CSV", directory, i))
			}

			if _, err := os.Stat(directory + "/hosp_" + strconv.Itoa(i) + "_RPT.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp_%d_RPT.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp_%d_RPT.CSV", directory, i))
			}

			if _, err := os.Stat(directory + "/hosp_" + strconv.Itoa(i) + "_ROLLUP.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp_%d_ROLLUP.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp_%d_ROLLUP.CSV", directory, i))
			}
		}		

		if (i >= 2010) {
			if _, err := os.Stat(directory + "/hosp10_" + strconv.Itoa(i) + "_ALPHA.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp10_%d_ALPHA.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp10_%d_ALPHA.CSV", directory, i))
			}
	
			if _, err := os.Stat(directory + "/hosp10_" + strconv.Itoa(i) + "_NMRC.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp10_%d_NMRC.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp10_%d_NMRC.CSV", directory, i))
			}
	
			if _, err := os.Stat(directory + "/hosp10_" + strconv.Itoa(i) + "_RPT.CSV"); err == nil {
				Pass(fmt.Sprintf("Exists %s/hosp10_%d_RPT.CSV", directory, i))
			} else {
				Fail(fmt.Sprintf("Does NOT Exist %s/hosp10_%d_RPT.CSV", directory, i))
			}			
		}
	}

	return err
}
