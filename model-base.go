package hcris-tools

import (
	"fmt"
	"reflect"
	"strings"
)

type BaseTableReport struct {
	C1_RptRecNum       string
	C2_PrvdrCtrlTypeCd string
	C3_PrvdrNum        string
	C4_Npi             string
	C5_RptStusCd       string
	C6_FyBgnDt         string
	C7_FyEndDt         string
	C8_ProcDt          string
	C9_InitlRptSw      string
	C10_LastRptSw      string
	C11_TrnsmtlNum     string
	C12_FiNum          string
	C13_AdrVndrCd      string
	C14_FiCreatDt      string
	C15_UtilCd         string
	C16_NprDt          string
	C17_SpecInd        string
	C18_FiRcptDt       string
}

type BaseTableNumeric struct {
	C1_RptRecNum string
	C2_WkshtNum  string
	C3_LineNum   string
	C4_ClmnNum   string
	C5_ItmValNum string
}

type BaseTableAlpha struct {
	C1_RptRecNum         string `json:"C1_RptRecNum"`
	C2_WkshtNum          string `json:"C2_WkshtNum"`
	C3_LineNum           string `json:"C3_LineNum"`
	C4_ClmnNum           string `json:"C4_ClmnNum"`
	C5_ItmAlphnmrcItmTxt string `json:"C5_ItmAlphnmrcItmTxt"`
}

type BaseTableRollup struct {
	C1_RptRecNum string
	C2_Label     string
	C3_Item      string
}

func BuildQuery(table string) string {
	var query string
	var querytable string

	fieldList := GetFieldList(table)

	Debug(fmt.Sprintf("Building Query For: %s", table))

	switch table {
	case "alpha":
		querytable = "ALPHA"
	case "nmrc":
		querytable = "NUMERIC"
	case "rollup":
		querytable = "ROLLUP"
	case "rpt":
		querytable = "REPORT"
	default:
		Error(AppMsg{"Fatal", "BuildQuery", "Could determine table type.", 11, nil}, true)
	}

	query = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", querytable, strings.Join(fieldList, ","), strings.Join(OutputQueue, ", "))

	OutputQueue = nil

	return query
}

func GetFieldList(table string) []string {
	var fieldList []string

	Debug(fmt.Sprintf("Getting Field List For: %s", table))

	switch table {
	case "alpha":
		var tablestruct BaseTableAlpha
		for i := 0; i < 5; i++ {
			fieldList = append(fieldList, reflect.ValueOf(&tablestruct).Elem().Type().Field(i).Name)
		}
	case "nmrc":
		var tablestruct BaseTableNumeric
		for i := 0; i < 5; i++ {
			fieldList = append(fieldList, reflect.ValueOf(&tablestruct).Elem().Type().Field(i).Name)
		}
	case "rollup":
		var tablestruct BaseTableRollup
		for i := 0; i < 3; i++ {
			fieldList = append(fieldList, reflect.ValueOf(&tablestruct).Elem().Type().Field(i).Name)
		}
	case "rpt":
		var tablestruct BaseTableReport
		for i := 0; i < 18; i++ {
			fieldList = append(fieldList, reflect.ValueOf(&tablestruct).Elem().Type().Field(i).Name)
		}
	default:
		Error(AppMsg{"Fatal", "FieldList", "Could determine table type.", 11, nil}, true)
	}

	return fieldList
}

func HandleAlpha(record []string) string {
	var table BaseTableAlpha
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_WkshtNum = record[1]
	table.C3_LineNum = record[2]
	table.C4_ClmnNum = record[3]
	table.C5_ItmAlphnmrcItmTxt = record[4]

	var fieldList []string
	for i := 0; i < 5; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "(" +
		table.C1_RptRecNum + ",'" +
		table.C2_WkshtNum + "','" +
		table.C3_LineNum + "','" +
		table.C4_ClmnNum + "','" +
		table.C5_ItmAlphnmrcItmTxt + "'" +
		")"

	return query
}

func HandleNumeric(record []string) string {
	var table BaseTableNumeric
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_WkshtNum = record[1]
	table.C3_LineNum = record[2]
	table.C4_ClmnNum = record[3]
	table.C5_ItmValNum = record[4]

	var fieldList []string
	for i := 0; i < 5; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "(" +
		table.C1_RptRecNum + ",'" +
		table.C2_WkshtNum + "','" +
		table.C3_LineNum + "','" +
		table.C4_ClmnNum + "','" +
		table.C5_ItmValNum + "'" +
		")"

	return query
}

func HandleRollup(record []string) string {
	var table BaseTableRollup
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_Label = record[1]
	table.C3_Item = record[2]

	var fieldList []string
	for i := 0; i < 3; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "(" +
		table.C1_RptRecNum + ",'" +
		table.C2_Label + "','" +
		table.C3_Item + "'" +
		")"

	return query
}

func HandleReport(record []string) string {
	var table BaseTableReport
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_PrvdrCtrlTypeCd = record[1]
	table.C3_PrvdrNum = record[2]
	table.C4_Npi = record[3]
	table.C5_RptStusCd = record[4]
	table.C6_FyBgnDt = record[5]
	table.C7_FyEndDt = record[6]
	table.C8_ProcDt = record[7]
	table.C9_InitlRptSw = record[8]
	table.C10_LastRptSw = record[9]
	table.C11_TrnsmtlNum = record[10]
	table.C12_FiNum = record[11]
	table.C13_AdrVndrCd = record[12]
	table.C14_FiCreatDt = record[13]
	table.C15_UtilCd = record[14]
	table.C16_NprDt = record[15]
	table.C17_SpecInd = record[16]
	table.C18_FiRcptDt = record[17]

	query = "(" +
		table.C1_RptRecNum + ",'" +
		table.C2_PrvdrCtrlTypeCd + "','" +
		table.C3_PrvdrNum + "','" +
		table.C4_Npi + "','" +
		table.C5_RptStusCd + "','" +
		table.C6_FyBgnDt + "','" +
		table.C7_FyEndDt + "','" +
		table.C8_ProcDt + "','" +
		table.C9_InitlRptSw + "','" +
		table.C10_LastRptSw + "','" +
		table.C11_TrnsmtlNum + "','" +
		table.C12_FiNum + "','" +
		table.C13_AdrVndrCd + "','" +
		table.C14_FiCreatDt + "','" +
		table.C15_UtilCd + "','" +
		table.C16_NprDt + "','" +
		table.C17_SpecInd + "','" +
		table.C18_FiRcptDt + "'" +
		")"

	return query
}

func HandleAlphaLong(record []string) string {
	var table BaseTableAlpha
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_WkshtNum = record[1]
	table.C3_LineNum = record[2]
	table.C4_ClmnNum = record[3]
	table.C5_ItmAlphnmrcItmTxt = record[4]

	var fieldList []string
	for i := 0; i < 5; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `ALPHA` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_WkshtNum + "','" +
		table.C3_LineNum + "','" +
		table.C4_ClmnNum + "','" +
		table.C5_ItmAlphnmrcItmTxt + "'" +
		");"

	return query
}

func HandleNumericLong(record []string) string {
	var table BaseTableNumeric
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_WkshtNum = record[1]
	table.C3_LineNum = record[2]
	table.C4_ClmnNum = record[3]
	table.C5_ItmValNum = record[4]

	var fieldList []string
	for i := 0; i < 5; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `NUMERIC` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_WkshtNum + "','" +
		table.C3_LineNum + "','" +
		table.C4_ClmnNum + "','" +
		table.C5_ItmValNum + "'" +
		");"

	return query
}

func HandleRollupLong(record []string) string {
	var table BaseTableRollup
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_Label = record[1]
	table.C3_Item = record[2]

	var fieldList []string
	for i := 0; i < 3; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `ROLLUP` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_Label + "','" +
		table.C3_Item + "'" +
		");"

	return query
}

func HandleReportLong(record []string) string {
	var table BaseTableReport
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_PrvdrCtrlTypeCd = record[1]
	table.C3_PrvdrNum = record[2]
	table.C4_Npi = record[3]
	table.C5_RptStusCd = record[4]
	table.C6_FyBgnDt = record[5]
	table.C7_FyEndDt = record[6]
	table.C8_ProcDt = record[7]
	table.C9_InitlRptSw = record[8]
	table.C10_LastRptSw = record[9]
	table.C11_TrnsmtlNum = record[10]
	table.C12_FiNum = record[11]
	table.C13_AdrVndrCd = record[12]
	table.C14_FiCreatDt = record[13]
	table.C15_UtilCd = record[14]
	table.C16_NprDt = record[15]
	table.C17_SpecInd = record[16]
	table.C18_FiRcptDt = record[17]

	var fieldList []string
	for i := 0; i < 18; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `REPORT` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_PrvdrCtrlTypeCd + "','" +
		table.C3_PrvdrNum + "','" +
		table.C4_Npi + "','" +
		table.C5_RptStusCd + "','" +
		table.C6_FyBgnDt + "','" +
		table.C7_FyEndDt + "','" +
		table.C8_ProcDt + "','" +
		table.C9_InitlRptSw + "','" +
		table.C10_LastRptSw + "','" +
		table.C11_TrnsmtlNum + "','" +
		table.C12_FiNum + "','" +
		table.C13_AdrVndrCd + "','" +
		table.C14_FiCreatDt + "','" +
		table.C15_UtilCd + "','" +
		table.C16_NprDt + "','" +
		table.C17_SpecInd + "','" +
		table.C18_FiRcptDt + "'" +
		");"

	return query
}

func HandleAlphaJson(record []string) BaseTableAlpha {
	var table BaseTableAlpha
	//var json string

	table.C1_RptRecNum = record[0]
	table.C2_WkshtNum = record[1]
	table.C3_LineNum = record[2]
	table.C4_ClmnNum = record[3]
	table.C5_ItmAlphnmrcItmTxt = record[4]
	/*
		var fieldList []string
		for i := 0; i < 5; i++ {
			fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
		}

		json = "{" +
			`"` + fieldList[0] + `": "` + table.C1_RptRecNum + `",` +
			`"` + fieldList[1] + `": "` + table.C2_WkshtNum + `",` +
			`"` + fieldList[2] + `": "` + table.C3_LineNum + `",` +
			`"` + fieldList[3] + `": "` + table.C4_ClmnNum + `",` +
			`"` + fieldList[4] + `": "` + table.C5_ItmAlphnmrcItmTxt + `"` +
			"}"
	*/
	return table
}

func HandleNumericJson(record []string) string {
	var table BaseTableNumeric
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_WkshtNum = record[1]
	table.C3_LineNum = record[2]
	table.C4_ClmnNum = record[3]
	table.C5_ItmValNum = record[4]

	var fieldList []string
	for i := 0; i < 5; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `NUMERIC` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_WkshtNum + "','" +
		table.C3_LineNum + "','" +
		table.C4_ClmnNum + "','" +
		table.C5_ItmValNum + "'" +
		");"

	return query
}

func HandleRollupJson(record []string) string {
	var table BaseTableRollup
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_Label = record[1]
	table.C3_Item = record[2]

	var fieldList []string
	for i := 0; i < 3; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `ROLLUP` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_Label + "','" +
		table.C3_Item + "'" +
		");"

	return query
}

func HandleReportJson(record []string) string {
	var table BaseTableReport
	var query string

	table.C1_RptRecNum = record[0]
	table.C2_PrvdrCtrlTypeCd = record[1]
	table.C3_PrvdrNum = record[2]
	table.C4_Npi = record[3]
	table.C5_RptStusCd = record[4]
	table.C6_FyBgnDt = record[5]
	table.C7_FyEndDt = record[6]
	table.C8_ProcDt = record[7]
	table.C9_InitlRptSw = record[8]
	table.C10_LastRptSw = record[9]
	table.C11_TrnsmtlNum = record[10]
	table.C12_FiNum = record[11]
	table.C13_AdrVndrCd = record[12]
	table.C14_FiCreatDt = record[13]
	table.C15_UtilCd = record[14]
	table.C16_NprDt = record[15]
	table.C17_SpecInd = record[16]
	table.C18_FiRcptDt = record[17]

	var fieldList []string
	for i := 0; i < 18; i++ {
		fieldList = append(fieldList, reflect.ValueOf(&table).Elem().Type().Field(i).Name)
	}

	query = "INSERT INTO `REPORT` " +
		"(" + strings.Join(fieldList, ",") + ") VALUES " +
		"(" +
		table.C1_RptRecNum + ",'" +
		table.C2_PrvdrCtrlTypeCd + "','" +
		table.C3_PrvdrNum + "','" +
		table.C4_Npi + "','" +
		table.C5_RptStusCd + "','" +
		table.C6_FyBgnDt + "','" +
		table.C7_FyEndDt + "','" +
		table.C8_ProcDt + "','" +
		table.C9_InitlRptSw + "','" +
		table.C10_LastRptSw + "','" +
		table.C11_TrnsmtlNum + "','" +
		table.C12_FiNum + "','" +
		table.C13_AdrVndrCd + "','" +
		table.C14_FiCreatDt + "','" +
		table.C15_UtilCd + "','" +
		table.C16_NprDt + "','" +
		table.C17_SpecInd + "','" +
		table.C18_FiRcptDt + "'" +
		");"

	return query
}

func SetupDb() error {
	query := "CREATE TABLE `ALPHA` " +
		"(`C1_RptRecNum` INTEGER(11) NOT NULL," +
		"`C2_WkshtNum` VARCHAR(7) NULL," +
		"`C3_LineNum` VARCHAR(5) NULL," +
		"`C4_ClmnNum` VARCHAR(5) NULL," +
		"`C5_ItmAlphnmrcItmTxt` VARCHAR(40) NULL," +
		"`created` DATE NULL);"

	Check(WriteResultToDb(query))

	query = "CREATE TABLE `NUMERIC` " +
		"(`C1_RptRecNum` INTEGER(11) NOT NULL," +
		"`C2_WkshtNum` VARCHAR(7) NULL," +
		"`C3_LineNum` VARCHAR(5) NULL," +
		"`C4_ClmnNum` VARCHAR(5) NULL," +
		"`C5_ItmValNum` VARCHAR(40) NULL," +
		"`created` DATE NULL);"

	Check(WriteResultToDb(query))

	query = "CREATE TABLE `ROLLUP` " +
		"(`C1_RptRecNum` INTEGER(11) NOT NULL," +
		"`C2_Label` VARCHAR(30) NULL," +
		"`C3_Item` NUMBER NULL," +
		"`created` DATE NULL);"

	Check(WriteResultToDb(query))

	query = "CREATE TABLE `REPORT` " +
		"(`C1_RptRecNum` INTEGER(11) NOT NULL," +
		"`C2_PrvdrCtrlTypeCd` VARCHAR(2) NULL," +
		"`C3_PrvdrNum` VARCHAR(6) NULL," +
		"`C4_Npi` NUMBER NULL," +
		"`C5_RptStusCd` VARCHAR(1) NULL," +
		"`C6_FyBgnDt` DATE NULL," +
		"`C7_FyEndDt` DATE NULL," +
		"`C8_ProcDt` DATE NULL," +
		"`C9_InitlRptSw` VARCHAR(1) NULL," +
		"`C10_LastRptSw` VARCHAR(1) NULL," +
		"`C11_TrnsmtlNum` VARCHAR(2) NULL," +
		"`C12_FiNum` VARCHAR(5) NULL," +
		"`C13_AdrVndrCd` VARCHAR(1) NULL," +
		"`C14_FiCreatDt` DATE NULL," +
		"`C15_UtilCd` VARCHAR(1) NULL," +
		"`C16_NprDt` DATE NULL," +
		"`C17_SpecInd` VARCHAR(1) NULL," +
		"`C18_FiRcptDt` DATE NULL," +
		"`created` DATE NULL);"

	Check(WriteResultToDb(query))

	return nil
}
