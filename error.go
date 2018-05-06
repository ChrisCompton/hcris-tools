package hcris

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/fatih/color"
	"github.com/skratchdot/open-golang/open"
)

type AppMsg struct {
	Severity string
	Source   string
	Message  string
	Code     int
	Error    error
}

func Check(err error) {
	checkErr(err)
}

func Debug(msg string) {
	if AppConfig.Settings.Debug == true {
		t := time.Now()
		fmt.Printf("DEBUG [%s]: %s\n", t.Format(time.RFC1123), msg)
		WriteToLog(msg)
	}
}

func Pass(msg string) {
	t := time.Now()
	green := color.New(color.FgGreen, color.Bold).SprintFunc()
	fmt.Printf("%s [%s]: %s\n", green("PASS"), t.Format(time.RFC1123), green(msg))
	WriteToLogOK(msg)
}

func Ok(msg string) {
	t := time.Now()
	green := color.New(color.FgGreen, color.Bold).SprintFunc()
	fmt.Printf("%s [%s]: %s\n", green("OK"), t.Format(time.RFC1123), green(msg))
	WriteToLogOK(msg)
}

func Fail(msg string) {
	t := time.Now()
	red := color.New(color.FgRed, color.Bold).SprintFunc()
	fmt.Printf("%s [%s]: %s\n", red("FAIL"), t.Format(time.RFC1123), red(msg))
	WriteToLogFAIL(msg)
}

func DebugVerbose(msg string) {
	if AppConfig.Settings.Verbose == true {
		t := time.Now()
		fmt.Printf("DEBUG [%s]: ++ %s\n", t.Format(time.RFC1123), msg)
		WriteToLog(msg)
	}
}

func Error(msg AppMsg, shutdown bool) {
	var errMsg = ""
	red := color.New(color.FgGreen, color.Bold).SprintFunc()
	if msg.Error != nil {
		errMsg = msg.Error.Error()
		WriteToLogFAIL(errMsg)
	}
	t := time.Now()
	fmt.Printf("ERROR [%s] %s %d (%s): %s [%s]\n", t.Format(time.RFC1123), red(msg.Severity), msg.Code, msg.Source, red(msg.Message), errMsg)
	if shutdown == true {
		exit(msg.Code)
	}
}

func checkErr(err error) {
	if err != nil {
		Error(AppMsg{"Unhandled", "checkErr", "Generic", 99, err}, true)
	}
}

func exit(reason int) {
	fmt.Printf("EXITING! (%d)", reason)
	open.Start(GetDataFolder() + "/" + AppConfig.Settings.Logfile)
	os.Exit(reason)
}

func WriteToLog(msg string) {
	if AppConfig.Settings.Logfile != "" {
		logFile := GetDataFolder() + "/" + AppConfig.Settings.Logfile

		t := time.Now()
		msg = fmt.Sprintf("[%s]: %s<br/>", t.Format(time.RFC1123), msg)

		f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		Check(err)

		defer f.Close()

		log.SetOutput(f)
		log.Println(msg)

	}
}

func WriteToLogOK(msg string) {
	WriteToLog("<strong style='color:green'>" + msg + "</strong>")
}

func WriteToLogFAIL(msg string) {
	WriteToLog("<strong style='color:red'>" + msg + "</strong>")
}

func CheckMem(msg string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n***** %s *****\nAlloc = %v\nTotalAlloc = %v\nSys = %v\nNumGC = %v\nGCSys = %v\n\n", msg, m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024, m.NumGC, m.GCSys/1024)
}

func WatchMem() {
	go func() {
		for {
			CheckMem("Watching")
			time.Sleep(5 * time.Second)
		}
	}()
}
