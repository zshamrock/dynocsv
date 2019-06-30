package dynamodb

import (
	"encoding/csv"
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
	columnsSeparator = ","
)

func ExportToCSV(profile string, table string, columns string, limit uint, w io.Writer) []string {
	svc := dynamodb.New(awssessions.GetSession(profile))
	writer := csv.NewWriter(w)
	attributesSet := make(map[string]bool)
	attributes := make([]string, 0)
	if columns != "" {
		attributes = strings.Split(columns, columnsSeparator)
		_ = writer.Write(attributes)
	}
	scan := dynamodb.ScanInput{TableName: aws.String(table)}
	if limit > 0 {
		scan.Limit = aws.Int64(int64(limit))
	}
	processed := 0
	// do not sort user defined columns
	sorted := columns != ""
	err := svc.ScanPages(&scan,
		func(page *dynamodb.ScanOutput, lastPage bool) bool {
			for _, item := range page.Items {
				records := make(map[string]string)
				for k, av := range item {
					value := getValue(av)
					if value == nil {
						continue
					}
					if columns == "" {
						if !attributesSet[k] {
							attributesSet[k] = true
							attributes = append(attributes, k)
						}
					}
					records[k] = aws.StringValue(value)
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
	if err != nil {
		log.Panic(err)
	}
	return attributes
}

func getValue(av *dynamodb.AttributeValue) *string {
	switch {
	case av.BOOL != nil:
		return aws.String(strconv.FormatBool(aws.BoolValue(av.BOOL)))
	case av.N != nil:
		return av.N
	case av.S != nil:
		return av.S
	default:
		return nil
	}
}
