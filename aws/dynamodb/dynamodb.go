package dynamodb

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
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

// QueryParams represents the query params set by the user, either hash or hash and sort.
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

func (qp *QueryParams) hashKeyConditionBuilder(
	key *dynamodb.KeySchemaElement, definitions map[string]string) expression.KeyConditionBuilder {
	attributeName := aws.StringValue(key.AttributeName)
	return expression.KeyEqual(
		expression.Key(attributeName),
		expression.Value(parse(qp.Hash, definitions[attributeName])))
}

func (qp *QueryParams) sortKeyConditionBuilder(
	key *dynamodb.KeySchemaElement, definitions map[string]string) expression.KeyConditionBuilder {
	attributeName := aws.StringValue(key.AttributeName)
	kb := expression.Key(attributeName)
	attributeType := definitions[attributeName]
	if len(qp.Sort) != 0 {
		return expression.KeyEqual(kb, expression.Value(parse(qp.Sort, attributeType)))
	} else if len(qp.SortGt) != 0 {
		return expression.KeyGreaterThan(kb, expression.Value(parse(qp.SortGt, attributeType)))
	} else if len(qp.SortGe) != 0 {
		return expression.KeyGreaterThanEqual(kb, expression.Value(parse(qp.SortGe, attributeType)))
	} else if len(qp.SortLt) != 0 {
		return expression.KeyLessThan(kb, expression.Value(parse(qp.SortLt, attributeType)))
	} else if len(qp.SortLe) != 0 {
		return expression.KeyLessThanEqual(kb, expression.Value(parse(qp.SortLe, attributeType)))
	} else if len(qp.SortBeginsWith) != 0 {
		return expression.KeyBeginsWith(kb, qp.SortBeginsWith)
	} else if len(qp.SortBetween) != 0 {
		return expression.KeyBetween(
			kb,
			expression.Value(parse(qp.SortBetween[0], attributeType)),
			expression.Value(parse(qp.SortBetween[1], attributeType)))
	}
	log.Panic("unsupported sort key operation")
	return expression.KeyConditionBuilder{}
}

func parse(attributeValue string, attributeType string) interface{} {
	var err error = nil
	var value interface{} = ""
	switch attributeType {
	case dynamodb.ScalarAttributeTypeS:
		value = attributeValue
	case dynamodb.ScalarAttributeTypeN:
		value, err = strconv.ParseInt(attributeValue, 10, 64)
	case dynamodb.ScalarAttributeTypeB:
		value, err = strconv.ParseBool(attributeValue)
	}
	if err != nil {
		log.Panicf("failed to parse %s into the corresponding type %s: %v", value, attributeType, err)
	}
	return value
}

func (qp *QueryParams) isEmpty() bool {
	return len(qp.Hash) == 0
}

func (qp *QueryParams) hasSort() bool {
	return len(qp.Sort) != 0 || len(qp.SortGt) != 0 || len(qp.SortGe) != 0 || len(qp.SortLt) != 0 ||
		len(qp.SortLe) != 0 || len(qp.SortBeginsWith) != 0 || len(qp.SortBetween) != 0
}

func (qp *QueryParams) keyConditionExpression(
	keys []*dynamodb.KeySchemaElement, definitions []*dynamodb.AttributeDefinition) expression.Expression {
	definitionsMapping := make(map[string]string)
	for _, definition := range definitions {
		definitionsMapping[aws.StringValue(definition.AttributeName)] = aws.StringValue(definition.AttributeType)
	}
	hashKeyConditionBuilder := qp.hashKeyConditionBuilder(findHashKey(keys), definitionsMapping)
	keyConditionBuilder := hashKeyConditionBuilder
	if qp.hasSort() {
		keyConditionBuilder = hashKeyConditionBuilder.And(qp.sortKeyConditionBuilder(findRangeKey(keys), definitionsMapping))
	}
	expr, err := expression.NewBuilder().WithKeyCondition(keyConditionBuilder).Build()
	if err != nil {
		log.Panicf("failed to build query expression due to %v", err)
	}
	return expr
}

func findHashKey(keys []*dynamodb.KeySchemaElement) *dynamodb.KeySchemaElement {
	return findKeyByType(keys, dynamodb.KeyTypeHash)
}

func findRangeKey(keys []*dynamodb.KeySchemaElement) *dynamodb.KeySchemaElement {
	return findKeyByType(keys, dynamodb.KeyTypeRange)
}

func findKeyByType(keys []*dynamodb.KeySchemaElement, keyType string) *dynamodb.KeySchemaElement {
	for _, key := range keys {
		if aws.StringValue(key.KeyType) == keyType {
			return key
		}
	}
	return nil
}

// ExportToCSV exports the result of the scan or query from the table into the corresponding CSV file using provided
// table and other settings.
func ExportToCSV(profile string, table string, qp *QueryParams, columns string, skipColumns string, limit uint, w io.Writer) []string {
	svc := dynamodb.New(awssessions.GetSession(profile))
	writer := csv.NewWriter(w)
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
		attributes, err = scanPages(svc, table, columns, limit, attributes, skipAttributes, writer)
	} else {
		attributes, err = queryPages(svc, table, qp, columns, limit, attributes, skipAttributes, writer)
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
	writer *csv.Writer) ([]string, error) {

	processed := 0
	// do not sort user defined columns
	sorted := columns != ""
	scan := dynamodb.ScanInput{TableName: aws.String(table)}
	if limit > 0 {
		scan.Limit = aws.Int64(int64(limit))
	}
	attributesSet := make(map[string]bool)
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
	return attributes, err
}

func queryPages(
	svc *dynamodb.DynamoDB,
	table string,
	qp *QueryParams,
	columns string,
	limit uint,
	attributes []string,
	skipAttributes map[string]bool,
	writer *csv.Writer) ([]string, error) {

	processed := 0
	// do not sort user defined columns
	sorted := columns != ""
	desc, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		log.Panicf("error fetching table %s description %v", table, err)
	}
	expr := qp.keyConditionExpression(desc.Table.KeySchema, desc.Table.AttributeDefinitions)
	query := dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values()}
	if limit > 0 {
		query.Limit = aws.Int64(int64(limit))
	}
	attributesSet := make(map[string]bool)
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
	return attributes, err
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
