package dynamodb

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
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

type writerBuffer struct {
	flushed bool
	limit   int
	buffer  [][]string
}

func (wb *writerBuffer) flush(writer *csv.Writer, attributes []string) {
	_ = writer.Write(attributes)
	if wb.flushed {
		_ = writer.WriteAll(wb.buffer)
	} else {
		// If buffer has not been flushed, the previous stored records might not be in sync with the latest attributes
		// count, i.e. extra blank values have to be appended to each of such records
		for _, records := range wb.buffer {
			for len(records) != len(attributes) {
				records = append(records, "")
			}
			_ = writer.Write(records)
		}
	}
	wb.flushed = true
	wb.buffer = nil
}

var wb = &writerBuffer{flushed: false, limit: 1000, buffer: make([][]string, 0, 100)}
var forceAttributesStdout = false

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
func ExportToCSV(
	profile string, table string, index string, qp *QueryParams, columns string, skipColumns string, limit uint, w io.Writer) ([]string, bool) {
	svc := dynamodb.New(awssessions.GetSession(profile))
	writer := csv.NewWriter(w)
	attributes := make([]string, 0)
	if columns != "" {
		attributes = strings.Split(columns, columnsSeparator)
		_ = writer.Write(attributes)
		// Consider if columns are set do not use buffer and flush all directly to the writer
		wb.flushed = true
	}
	skipAttributes := make(map[string]bool)
	if skipColumns != "" {
		for _, attr := range strings.Split(skipColumns, columnsSeparator) {
			skipAttributes[attr] = true
		}
	}
	var desc *dynamodb.TableDescription
	attributesSet := make(map[string]bool)
	if columns == "" {
		output, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
		if err != nil {
			log.Panicf("error fetching table %s description %v", table, err)
		}
		desc = output.Table
		attributes, attributesSet = defineBaselineAttributes(
			svc, desc, desc.GlobalSecondaryIndexes, index, skipAttributes)
	}
	var err error
	if qp.isEmpty() {
		attributes, err = scanPages(svc, table, columns, limit, attributes, skipAttributes, attributesSet, writer)
	} else {
		attributes, err = queryPages(
			svc, desc, table, index, qp, columns, limit, attributes, skipAttributes, attributesSet, writer)
	}
	if err != nil {
		log.Panic(err)
	}
	return attributes, forceAttributesStdout
}

func scanPages(
	svc *dynamodb.DynamoDB,
	table string,
	columns string,
	limit uint,
	attributes []string,
	skipAttributes map[string]bool,
	attributesSet map[string]bool,
	writer *csv.Writer) ([]string, error) {

	scan := dynamodb.ScanInput{TableName: aws.String(table)}
	if limit > 0 {
		scan.Limit = aws.Int64(int64(limit))
	}
	processed := 0
	err := svc.ScanPages(&scan,
		func(page *dynamodb.ScanOutput, lastPage bool) bool {
			done := false
			attributes, attributesSet, processed, done = process(
				page.Items, columns, attributes, skipAttributes, attributesSet, limit, processed, lastPage, writer)
			return !done
		})
	return attributes, err
}

func queryPages(
	svc *dynamodb.DynamoDB,
	desc *dynamodb.TableDescription,
	table string,
	index string,
	qp *QueryParams,
	columns string,
	limit uint,
	attributes []string,
	skipAttributes map[string]bool,
	attributesSet map[string]bool,
	writer *csv.Writer) ([]string, error) {

	if desc == nil {
		output, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
		if err != nil {
			log.Panicf("error fetching table %s description %v", table, err)
		}
		desc = output.Table
	}

	var keySchema = desc.KeySchema
	if index != "" {
		indexes := desc.GlobalSecondaryIndexes
		for _, idx := range indexes {
			if aws.StringValue(idx.IndexName) == index {
				keySchema = idx.KeySchema
				break
			}
		}
	}
	expr := qp.keyConditionExpression(keySchema, desc.AttributeDefinitions)
	query := dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values()}
	if index != "" {
		query.IndexName = aws.String(index)
	}
	if limit > 0 {
		query.Limit = aws.Int64(int64(limit))
	}
	processed := 0
	err := svc.QueryPages(&query,
		func(page *dynamodb.QueryOutput, lastPage bool) bool {
			done := false
			attributes, attributesSet, processed, done = process(
				page.Items, columns, attributes, skipAttributes, attributesSet, limit, processed, lastPage, writer)
			return !done
		})
	return attributes, err
}

// Gets one single item from the table (using scan), and build the baseline attributes out of there, where table's
// primary keys are coming first, then all indexes' (by alphabetical order) keys next, and all the rest detected
// attributes from the scan result by the alphabetical order.
// Although if the index is set, then index's attributes will come first, then table's next, and all remaining indexes'
// after.
func defineBaselineAttributes(
	svc dynamodbiface.DynamoDBAPI,
	table *dynamodb.TableDescription,
	indexes []*dynamodb.GlobalSecondaryIndexDescription,
	index string,
	skipAttributes map[string]bool) ([]string, map[string]bool) {

	attributes := make([]string, 0)
	attributesSet := make(map[string]bool)
	// sort indexes alphabetically by name
	sort.Slice(indexes, func(i, j int) bool {
		return aws.StringValue(indexes[i].IndexName) < aws.StringValue(indexes[j].IndexName)
	})
	for _, i := range indexes {
		// if index is defined, i.e. not "", then append its key attributes first
		if aws.StringValue(i.IndexName) == index {
			attributes, attributesSet = appendKeyAttributes(i.KeySchema, attributes, attributesSet, skipAttributes)
			break
		}
	}
	attributes, attributesSet = appendKeyAttributes(table.KeySchema, attributes, attributesSet, skipAttributes)
	for _, i := range indexes {
		if aws.StringValue(i.IndexName) == index {
			// if index is defined, i.e. not "", then we already processed it, so continue on other indexes
			continue
		}
		attributes, attributesSet = appendKeyAttributes(i.KeySchema, attributes, attributesSet, skipAttributes)
	}
	scan := dynamodb.ScanInput{TableName: table.TableName, Limit: aws.Int64(1)}
	if index != "" {
		scan.IndexName = aws.String(index)
	}
	output, err := svc.Scan(&scan)
	if err != nil {
		log.Panicf("error fetching table %s item %v", aws.StringValue(table.TableName), err)
	}
	items := output.Items
	if len(items) == 0 {
		return []string{}, map[string]bool{}
	}
	item := items[0]
	restAttributes := make([]string, 0)
	for k, av := range item {
		_, handled := getValue(av)
		if !handled {
			continue
		}
		if shouldAppendAttribute(k, attributesSet, skipAttributes) {
			attributesSet[k] = true
			restAttributes = append(restAttributes, k)
		}
	}
	sort.Slice(restAttributes, func(i, j int) bool {
		return restAttributes[i] < restAttributes[j]
	})
	attributes = append(attributes, restAttributes...)
	return attributes, attributesSet
}

func appendKeyAttributes(
	keys []*dynamodb.KeySchemaElement,
	attributes []string,
	attributesSet map[string]bool,
	skipAttributes map[string]bool) ([]string, map[string]bool) {

	hash := aws.StringValue(findHashKey(keys).AttributeName)
	if shouldAppendAttribute(hash, attributesSet, skipAttributes) {
		attributes = append(attributes, hash)
		attributesSet[hash] = true
	}
	sortKey := findRangeKey(keys)
	if sortKey != nil {
		//noinspection GoImportUsedAsName
		sort := aws.StringValue(sortKey.AttributeName)
		if shouldAppendAttribute(sort, attributesSet, skipAttributes) {
			attributes = append(attributes, sort)
			attributesSet[sort] = true
		}
	}
	return attributes, attributesSet
}

func shouldAppendAttribute(name string, attributesSet map[string]bool, skipAttributes map[string]bool) bool {
	return !skipAttributes[name] && !attributesSet[name]
}

func process(
	items []map[string]*dynamodb.AttributeValue,
	columns string,
	attributes []string,
	skipAttributes map[string]bool,
	attributesSet map[string]bool,
	limit uint,
	processed int,
	lastPage bool,
	writer *csv.Writer) ([]string, map[string]bool, int, bool) {
	for _, item := range items {
		records := make(map[string]string)
		for k, av := range item {
			value, handled := getValue(av)
			if !handled {
				continue
			}
			if columns == "" {
				if shouldAppendAttribute(k, attributesSet, skipAttributes) {
					attributesSet[k] = true
					if wb.flushed {
						forceAttributesStdout = true
					}
					attributes = append(attributes, k)
				}
			}
			records[k] = value
		}
		orderedRecords := make([]string, 0, len(attributes))
		for _, attr := range attributes {
			if value, ok := records[attr]; ok {
				orderedRecords = append(orderedRecords, value)
			} else {
				orderedRecords = append(orderedRecords, "")
			}
		}
		if wb.flushed {
			_ = writer.Write(orderedRecords)
		} else {
			wb.buffer = append(wb.buffer, orderedRecords)
			if len(wb.buffer) >= wb.limit {
				wb.flush(writer, attributes)
			}
		}
		processed++
		if limit > 0 && processed == int(limit) {
			if wb.flushed {
				writer.Flush()
			} else {
				wb.flush(writer, attributes)
			}
			return attributes, attributesSet, processed, true
		}
	}
	if lastPage && !wb.flushed {
		wb.flush(writer, attributes)
	}
	writer.Flush()
	return attributes, attributesSet, processed, lastPage
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
