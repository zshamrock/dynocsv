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
	columnsSeparator   = ","
	setValuesSeparator = ","
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

/*
switch {
	case len(av.B) != 0:
		return d.decodeBinary(av.B, v)
	case av.BOOL != nil:
		return d.decodeBool(av.BOOL, v)
	case len(av.BS) != 0:
		return d.decodeBinarySet(av.BS, v)
	case len(av.L) != 0:
		return d.decodeList(av.L, v)
	case len(av.M) != 0:
		return d.decodeMap(av.M, v)
	case av.N != nil:
		return d.decodeNumber(av.N, v, fieldTag)
	case len(av.NS) != 0:
		return d.decodeNumberSet(av.NS, v)
	case av.S != nil:
		return d.decodeString(av.S, v, fieldTag)
	case len(av.SS) != 0:
		return d.decodeStringSet(av.SS, v)
	}
*/

func getValue(av *dynamodb.AttributeValue) *string {
	switch {
	case av.BOOL != nil:
		return aws.String(strconv.FormatBool(aws.BoolValue(av.BOOL)))
	case av.N != nil:
		return av.N
	case av.S != nil:
		return av.S
	case len(av.M) != 0:
		data := make(map[string]string)
		for k, v := range av.M {
			value := getValue(v)
			if value != nil {
				data[k] = aws.StringValue(value)
			}
		}
		b, err := json.Marshal(data)
		if err != nil {
			return nil
		}
		return aws.String(string(b))
	case len(av.SS) != 0:
		return processStringSet(av.SS)
	case len(av.NS) != 0:
		return processNumberSet(av.NS)
	default:
		return nil
	}
}

func processStringSet(values []*string) *string {
	data := make([]string, 0, len(values))
	for _, v := range values {
		data = append(data, aws.StringValue(v))
	}
	return aws.String(fmt.Sprint("[", strings.Join(data, setValuesSeparator), "]"))
}

func processNumberSet(values []*string) *string {
	data := make([]float64, 0, len(values))
	for _, v := range values {
		f, _ := strconv.ParseFloat(aws.StringValue(v), 64)
		data = append(data, f)
	}
	return aws.String(strings.Join(strings.Fields(fmt.Sprint(data)), setValuesSeparator))
}
