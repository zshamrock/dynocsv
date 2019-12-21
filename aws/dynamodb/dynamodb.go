package dynamodb

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	awssessions "github.com/zshamrock/dynocsv/aws"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
)

const (
	columnsSeparator    = ","
	setValuesSeparator  = ","
	setOpenSymbol       = "["
	setCloseSymbol      = "]"
	listValuesSeparator = ","
	listOpenSymbol      = "["
	listCloseSymbol     = "]"
)

type QueryParams struct {
	Hash           string
	Sort           string
	SortGt         string
	SortGe         string
	SortLt         string
	SortLe         string
	SortBeginsWith string
	SortBetween    []string
}

func (qp *QueryParams) HashQueryString(key *dynamodb.KeySchemaElement, outParams map[string]string) string {
	outParams[":hash1"] = qp.Hash
	return aws.StringValue(key.AttributeName) + " = :hash1"
}

func (qp *QueryParams) SortQueryString(key *dynamodb.KeySchemaElement, outParams map[string]string) string {
	var s string
	if len(qp.Sort) != 0 {
		s = " = :sort1"
		outParams[":sort1"] = qp.Sort
	} else if len(qp.SortGt) != 0 {
		s = " > :sort1"
		outParams[":sort1"] = qp.SortGt
	} else if len(qp.SortGe) != 0 {
		s = " >= :sort1"
		outParams[":sort1"] = qp.SortGe
	} else if len(qp.SortLt) != 0 {
		s = " < :sort1"
		outParams[":sort1"] = qp.SortLt
	} else if len(qp.SortLe) != 0 {
		s = " <= :sort1"
		outParams[":sort1"] = qp.SortLe
	} else if len(qp.SortBeginsWith) != 0 {
		s = " BEGINS WITH :sort1"
		outParams[":sort1"] = qp.SortBeginsWith
	} else if len(qp.SortBetween) != 0 {
		s = " BETWEEN :sort1 AND :sort2"
		outParams[":sort1"] = qp.SortBetween[0]
		outParams[":sort2"] = qp.SortBetween[1]
	}
	return aws.StringValue(key.AttributeName) + s
}

func (qp *QueryParams) isEmpty() bool {
	return len(qp.Hash) == 0
}

func (qp *QueryParams) hasSort() bool {
	return len(qp.Sort) != 0 || len(qp.SortGt) != 0 || len(qp.SortGe) != 0 || len(qp.SortLt) != 0 ||
		len(qp.SortLe) != 0 || len(qp.SortBeginsWith) != 0 || len(qp.SortBetween) != 0
}

func (qp *QueryParams) KeyConditionExpression(key []*dynamodb.KeySchemaElement, outParams map[string]string) *string {
	hashQueryString := qp.HashQueryString(key[0], outParams)
	if qp.hasSort() {
		return aws.String(hashQueryString + " AND " + qp.SortQueryString(key[1], outParams))
	}
	return aws.String(hashQueryString)
}

func ExportToCSV(profile string, table string, qp *QueryParams, columns string, skipColumns string, limit uint, w io.Writer) []string {
	svc := dynamodb.New(awssessions.GetSession(profile))
	writer := csv.NewWriter(w)
	attributesSet := make(map[string]bool)
	attributes := make([]string, 0)
	if columns != "" {
		attributes = strings.Split(columns, columnsSeparator)
		_ = writer.Write(attributes)
	}
	skipAttributes := make(map[string]bool)
	if skipColumns != "" {
		for _, attr := range strings.Split(skipColumns, columnsSeparator) {
			skipAttributes[attr] = true
		}
	}
	var err error
	if qp.isEmpty() {
		err = scanPages(svc, table, columns, limit, attributes, skipAttributes, attributesSet, writer)
	} else {
		err = queryPages(svc, table, &QueryParams{}, columns, limit, attributes, skipAttributes, attributesSet, writer)
	}
	if err != nil {
		log.Panic(err)
	}
	return attributes
}

func scanPages(
	svc *dynamodb.DynamoDB,
	table string,
	columns string,
	limit uint,
	attributes []string,
	skipAttributes map[string]bool,
	attributesSet map[string]bool,
	writer *csv.Writer) error {

	processed := 0
	// do not sort user defined columns
	sorted := columns != ""
	scan := dynamodb.ScanInput{TableName: aws.String(table)}
	if limit > 0 {
		scan.Limit = aws.Int64(int64(limit))
	}
	err := svc.ScanPages(&scan,
		func(page *dynamodb.ScanOutput, lastPage bool) bool {
			for _, item := range page.Items {
				records := make(map[string]string)
				for k, av := range item {
					value, handled := getValue(av)
					if !handled {
						continue
					}
					if columns == "" {
						if _, skip := skipAttributes[k]; !skip && !attributesSet[k] {
							attributesSet[k] = true
							attributes = append(attributes, k)
						}
					}
					records[k] = value
				}
				if !sorted {
					sort.Slice(attributes, func(i, j int) bool {
						return attributes[i] < attributes[j]
					})
					sorted = true
				}
				orderedRecords := make([]string, 0, len(attributes))
				for _, attr := range attributes {
					if value, ok := records[attr]; ok {
						orderedRecords = append(orderedRecords, value)
					} else {
						orderedRecords = append(orderedRecords, "")
					}
				}
				_ = writer.Write(orderedRecords)
				processed++
				if limit > 0 && processed == int(limit) {
					writer.Flush()
					return false
				}
			}
			writer.Flush()
			return !lastPage
		})
	return err
}

func queryPages(svc *dynamodb.DynamoDB, table string, qp *QueryParams, columns string, limit uint, attributes []string, skipAttributes map[string]bool, attributesSet map[string]bool, writer *csv.Writer) error {

	processed := 0
	// do not sort user defined columns
	sorted := columns != ""
	desc, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		log.Panicf("error fetching table %s description %v", table, err)
	}
	outputParams := make(map[string]string)
	query := dynamodb.QueryInput{TableName: aws.String(table), KeyConditionExpression: qp.KeyConditionExpression(desc.Table.KeySchema, outputParams)}
	if limit > 0 {
		query.Limit = aws.Int64(int64(limit))
	}
	err = svc.QueryPages(&query,
		func(page *dynamodb.QueryOutput, lastPage bool) bool {
			for _, item := range page.Items {
				records := make(map[string]string)
				for k, av := range item {
					value, handled := getValue(av)
					if !handled {
						continue
					}
					if columns == "" {
						if _, skip := skipAttributes[k]; !skip && !attributesSet[k] {
							attributesSet[k] = true
							attributes = append(attributes, k)
						}
					}
					records[k] = value
				}
				if !sorted {
					sort.Slice(attributes, func(i, j int) bool {
						return attributes[i] < attributes[j]
					})
					sorted = true
				}
				orderedRecords := make([]string, 0, len(attributes))
				for _, attr := range attributes {
					if value, ok := records[attr]; ok {
						orderedRecords = append(orderedRecords, value)
					} else {
						orderedRecords = append(orderedRecords, "")
					}
				}
				_ = writer.Write(orderedRecords)
				processed++
				if limit > 0 && processed == int(limit) {
					writer.Flush()
					return false
				}
			}
			writer.Flush()
			return !lastPage
		})
	return err
}

func getValue(av *dynamodb.AttributeValue) (string, bool) {
	switch {
	case av.BOOL != nil:
		return strconv.FormatBool(aws.BoolValue(av.BOOL)), true
	case av.N != nil:
		return aws.StringValue(av.N), true
	case av.S != nil:
		return aws.StringValue(av.S), true
	case av.M != nil:
		return processMap(av)
	case av.SS != nil:
		return processSet(av.SS), true
	case av.NS != nil:
		return processSet(av.NS), true
	case av.L != nil:
		return processList(av.L), true
	default:
		return "", false
	}
}

func processMap(av *dynamodb.AttributeValue) (string, bool) {
	data := make(map[string]string)
	for k, v := range av.M {
		value, handled := getValue(v)
		if handled {
			data[k] = value
		}
	}
	b, err := json.Marshal(data)
	if err != nil {
		return "", false
	}
	return string(b), true
}

func processSet(values []*string) string {
	data := make([]string, 0, len(values))
	for _, v := range values {
		data = append(data, aws.StringValue(v))
	}
	return buildOutput(data, setOpenSymbol, setCloseSymbol, setValuesSeparator)
}

func processList(values []*dynamodb.AttributeValue) string {
	data := make([]string, 0, len(values))
	for _, v := range values {
		value, _ := getValue(v)
		data = append(data, value)
	}
	return buildOutput(data, listOpenSymbol, listCloseSymbol, listValuesSeparator)
}

func buildOutput(data []string, openSymbol string, closeSymbol string, separator string) string {
	return fmt.Sprint(openSymbol, strings.Join(data, separator), closeSymbol)
}
