package dynamodb

import (
	"encoding/csv"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	awssessions "github.com/zshamrock/dynocsv/aws"
	"io"
	"log"
	"strconv"
	"strings"
)

var svc *dynamodb.DynamoDB

const (
	columnsSeparator = ","
)

func init() {
	svc = dynamodb.New(awssessions.GetSession())
}

func ExportToCSV(table string, columns string, w io.Writer) []string {
	writer := csv.NewWriter(w)
	attributesSet := make(map[string]bool)
	attributes := make([]string, 0)
	if columns != "" {
		attributes = strings.Split(columns, columnsSeparator)
		_ = writer.Write(attributes)
	}
	err := svc.ScanPages(&dynamodb.ScanInput{TableName: aws.String(table)},
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
				orderedRecords := make([]string, 0, len(attributes))
				for _, attr := range attributes {
					if value, ok := records[attr]; ok {
						orderedRecords = append(orderedRecords, value)
					} else {
						orderedRecords = append(orderedRecords, "")
					}
				}
				_ = writer.Write(orderedRecords)
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
