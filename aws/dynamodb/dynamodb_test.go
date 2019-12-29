package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"reflect"
	"testing"
)

func TestGetValue(t *testing.T) {
	type args struct {
		av *dynamodb.AttributeValue
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "get positive number value",
			args: args{av: &dynamodb.AttributeValue{N: aws.String("10")}},
			want: "10",
		},
		{
			name: "get negative number value",
			args: args{av: &dynamodb.AttributeValue{N: aws.String("-10")}},
			want: "-10",
		},
		{
			name: "get positive floating point number value",
			args: args{av: &dynamodb.AttributeValue{N: aws.String("3.14")}},
			want: "3.14",
		},
		{
			name: "get negative floating point number value",
			args: args{av: &dynamodb.AttributeValue{N: aws.String("-3.14")}},
			want: "-3.14",
		},
		{
			name: "get true boolean value",
			args: args{av: &dynamodb.AttributeValue{BOOL: aws.Bool(true)}},
			want: "true",
		},
		{
			name: "get false boolean value",
			args: args{av: &dynamodb.AttributeValue{BOOL: aws.Bool(false)}},
			want: "false",
		},
		{
			name: "get empty string value",
			args: args{av: &dynamodb.AttributeValue{S: aws.String("")}},
			want: "",
		},
		{
			name: "get not empty string value",
			args: args{av: &dynamodb.AttributeValue{S: aws.String("Hippo")}},
			want: "Hippo",
		},
		{
			name: "get empty map value",
			args: args{av: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}}},
			want: "{}",
		},
		{
			name: "get not empty map value",
			args: args{av: &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{"x": {S: aws.String("y")}}}},
			want: `{"x":"y"}`,
		},
		{
			name: "get empty list value",
			args: args{av: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}},
			want: "[]",
		},
		{
			name: "get not empty list value",
			args: args{av: &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{{S: aws.String("x")}}}},
			want: "[x]",
		},
		{
			name: "get empty string set value",
			args: args{av: &dynamodb.AttributeValue{SS: []*string{}}},
			want: "[]",
		},
		{
			name: "get not empty string set value",
			args: args{av: &dynamodb.AttributeValue{SS: []*string{aws.String("x")}}},
			want: "[x]",
		},
		{
			name: "get empty number set value",
			args: args{av: &dynamodb.AttributeValue{NS: []*string{}}},
			want: "[]",
		},
		{
			name: "get not empty number set value",
			args: args{av: &dynamodb.AttributeValue{NS: []*string{aws.String("10")}}},
			want: "[10]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := getValue(tt.args.av); got != tt.want {
				t.Errorf("getValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessMap(t *testing.T) {
	type args struct {
		value *dynamodb.AttributeValue
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty map",
			args: args{value: &dynamodb.AttributeValue{
				M: map[string]*dynamodb.AttributeValue{}}},
			want: "{}",
		},
		{
			name: "not empty map",
			args: args{value: &dynamodb.AttributeValue{
				M: map[string]*dynamodb.AttributeValue{
					"type":    {S: aws.String("animal")},
					"name":    {S: aws.String("Hippo")},
					"weight":  {N: aws.String("56.78")},
					"friends": {L: []*dynamodb.AttributeValue{{S: aws.String("Zebra")}, {S: aws.String("Giraffe")}}},
					"family": {M: map[string]*dynamodb.AttributeValue{
						"wife": {S: aws.String("Pretty Hippo")},
						"kid":  {S: aws.String("Smart Kid")}},
					},
					"hobbies": {SS: []*string{aws.String("swimming"), aws.String("sleeping")}},
				}}},
			want: `{"family":"{\"kid\":\"Smart Kid\",\"wife\":\"Pretty Hippo\"}","friends":"[Zebra,Giraffe]","hobbies":"[swimming,sleeping]","name":"Hippo","type":"animal","weight":"56.78"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, ok := processMap(tt.args.value); ok && got != tt.want {
				t.Errorf("processMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessNumberSet(t *testing.T) {
	type args struct {
		values []*string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty number set",
			args: args{values: []*string{}},
			want: "[]",
		},
		{
			name: "single entry set",
			args: args{values: []*string{aws.String("10")}},
			want: "[10]",
		},
		{
			name: "multiple different entries set",
			args: args{values: []*string{aws.String("42.2"), aws.String("-19"), aws.String("7.5"), aws.String("3.14")}},
			want: "[42.2,-19,7.5,3.14]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processSet(tt.args.values); got != tt.want {
				t.Errorf("processNumberSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessStringSet(t *testing.T) {
	type args struct {
		values []*string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty number set",
			args: args{values: []*string{}},
			want: "[]",
		},
		{
			name: "single entry set",
			args: args{values: []*string{aws.String("Hello")}},
			want: "[Hello]",
		},
		{
			name: "multiple different entries set",
			args: args{values: []*string{aws.String("Giraffe"), aws.String("Hippo"), aws.String("Zebra"), aws.String("3.14")}},
			want: "[Giraffe,Hippo,Zebra,3.14]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processSet(tt.args.values); got != tt.want {
				t.Errorf("processSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessStringList(t *testing.T) {
	type args struct {
		values []*dynamodb.AttributeValue
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty list",
			args: args{values: []*dynamodb.AttributeValue{}},
			want: "[]",
		},
		{
			name: "single entry list",
			args: args{values: []*dynamodb.AttributeValue{{N: aws.String("10")}}},
			want: "[10]",
		},
		{
			name: "multiple different entries list",
			args: args{values: []*dynamodb.AttributeValue{
				{N: aws.String("10")},
				{S: aws.String("Zebra")},
				{BOOL: aws.Bool(true)},
				{B: []byte{}},
			}},
			want: "[10,Zebra,true,]",
		},
		{
			name: "multiple different composite entries list",
			args: args{values: []*dynamodb.AttributeValue{
				{N: aws.String("10")},
				{S: aws.String("Zebra")},
				{BOOL: aws.Bool(true)},
				{B: []byte{}},
				{L: []*dynamodb.AttributeValue{
					{N: aws.String("10")},
					{N: aws.String("3.14")},
				}},
				{NS: []*string{aws.String("5"), aws.String("3")}},
				{SS: []*string{aws.String("Giraffe"), aws.String("Hippo"), aws.String("Zebra"), aws.String("3.14")}},
				{L: []*dynamodb.AttributeValue{
					{L: []*dynamodb.AttributeValue{
						{L: []*dynamodb.AttributeValue{
							{N: aws.String("-7")},
							{NS: []*string{aws.String("1"), aws.String("3")}},
							{SS: []*string{aws.String("Hippo")}},
						}},
					}},
				}},
			}},
			want: "[10,Zebra,true,,[10,3.14],[5,3],[Giraffe,Hippo,Zebra,3.14],[[[-7,[1,3],[Hippo]]]]]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processList(tt.args.values); got != tt.want {
				t.Errorf("processList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryParamsHasSort(t *testing.T) {
	type fields struct {
		Hash           string
		Sort           string
		SortGt         string
		SortGe         string
		SortLt         string
		SortLe         string
		SortBeginsWith string
		SortBetween    []string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{name: "no hash either sort keys", fields: fields{}, want: false},
		{name: "only hash key", fields: fields{Hash: "1"}, want: false},
		{name: "sort", fields: fields{Hash: "1", Sort: "2"}, want: true},
		{name: "sort gt", fields: fields{Hash: "1", SortGt: "2"}, want: true},
		{name: "sort ge", fields: fields{Hash: "1", SortGe: "2"}, want: true},
		{name: "sort lt", fields: fields{Hash: "1", SortLt: "2"}, want: true},
		{name: "sort le", fields: fields{Hash: "1", SortLe: "2"}, want: true},
		{name: "sort begins with", fields: fields{Hash: "1", SortBeginsWith: "2"}, want: true},
		{name: "sort between", fields: fields{Hash: "1", SortBetween: []string{"2", "3"}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp := &QueryParams{
				Hash:           tt.fields.Hash,
				Sort:           tt.fields.Sort,
				SortGt:         tt.fields.SortGt,
				SortGe:         tt.fields.SortGe,
				SortLt:         tt.fields.SortLt,
				SortLe:         tt.fields.SortLe,
				SortBeginsWith: tt.fields.SortBeginsWith,
				SortBetween:    tt.fields.SortBetween,
			}
			if got := qp.hasSort(); got != tt.want {
				t.Errorf("hasSort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryParamsIsEmpty(t *testing.T) {
	type fields struct {
		Hash           string
		Sort           string
		SortGt         string
		SortGe         string
		SortLt         string
		SortLe         string
		SortBeginsWith string
		SortBetween    []string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{name: "no hash either sort keys", fields: fields{}, want: true},
		{name: "only hash key", fields: fields{Hash: "1"}, want: false},
		{name: "hash and sort", fields: fields{Hash: "1", Sort: "2"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp := &QueryParams{
				Hash:           tt.fields.Hash,
				Sort:           tt.fields.Sort,
				SortGt:         tt.fields.SortGt,
				SortGe:         tt.fields.SortGe,
				SortLt:         tt.fields.SortLt,
				SortLe:         tt.fields.SortLe,
				SortBeginsWith: tt.fields.SortBeginsWith,
				SortBetween:    tt.fields.SortBetween,
			}
			if got := qp.isEmpty(); got != tt.want {
				t.Errorf("isEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryParamsHashKeyConditionBuilder(t *testing.T) {
	type fields struct {
		Hash           string
		Sort           string
		SortGt         string
		SortGe         string
		SortLt         string
		SortLe         string
		SortBeginsWith string
		SortBetween    []string
	}
	type args struct {
		key         *dynamodb.KeySchemaElement
		definitions map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   expression.KeyConditionBuilder
	}{
		{
			name:   "hash/S",
			fields: fields{Hash: "value1"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute1"),
				KeyType:       aws.String(dynamodb.KeyTypeHash)},
				definitions: map[string]string{"Attribute1": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyEqual(expression.Key("Attribute1"), expression.Value("value1")),
		},
		{
			name:   "hash/N",
			fields: fields{Hash: "1529665668588"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute1"),
				KeyType:       aws.String(dynamodb.KeyTypeHash)},
				definitions: map[string]string{"Attribute1": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyEqual(expression.Key("Attribute1"), expression.Value(int64(1529665668588))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp := &QueryParams{
				Hash:           tt.fields.Hash,
				Sort:           tt.fields.Sort,
				SortGt:         tt.fields.SortGt,
				SortGe:         tt.fields.SortGe,
				SortLt:         tt.fields.SortLt,
				SortLe:         tt.fields.SortLe,
				SortBeginsWith: tt.fields.SortBeginsWith,
				SortBetween:    tt.fields.SortBetween,
			}
			if cond := qp.hashKeyConditionBuilder(tt.args.key, tt.args.definitions); !reflect.DeepEqual(cond, tt.want) {
				t.Errorf("hashKeyConditionBuilder() = %v, want %v", cond, tt.want)
			}
		})
	}
}

func TestQueryParamsSortQueryString(t *testing.T) {
	type fields struct {
		Hash           string
		Sort           string
		SortGt         string
		SortGe         string
		SortLt         string
		SortLe         string
		SortBeginsWith string
		SortBetween    []string
	}
	type args struct {
		key         *dynamodb.KeySchemaElement
		definitions map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   expression.KeyConditionBuilder
	}{
		{
			name:   "sort/S",
			fields: fields{Hash: "value1", Sort: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyEqual(expression.Key("Attribute2"), expression.Value("value2")),
		},
		{
			name:   "sort gt/S",
			fields: fields{Hash: "value1", SortGt: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyGreaterThan(expression.Key("Attribute2"), expression.Value("value2")),
		},
		{
			name:   "sort ge/S",
			fields: fields{Hash: "value1", SortGe: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyGreaterThanEqual(expression.Key("Attribute2"), expression.Value("value2")),
		},
		{
			name:   "sort lt/S",
			fields: fields{Hash: "value1", SortLt: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyLessThan(expression.Key("Attribute2"), expression.Value("value2")),
		},
		{
			name:   "sort le/S",
			fields: fields{Hash: "value1", SortLe: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyLessThanEqual(expression.Key("Attribute2"), expression.Value("value2")),
		},
		{
			name:   "sort begins with/S",
			fields: fields{Hash: "value1", SortBeginsWith: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyBeginsWith(expression.Key("Attribute2"), "value2"),
		},
		{
			name:   "sort between/S",
			fields: fields{Hash: "value1", SortBetween: []string{"value2", "value3"}},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeS}},
			want: expression.KeyBetween(expression.Key("Attribute2"), expression.Value("value2"), expression.Value("value3")),
		},

		{
			name:   "sort/N",
			fields: fields{Hash: "value1", Sort: "1529665668588"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyEqual(expression.Key("Attribute2"), expression.Value(int64(1529665668588))),
		},
		{
			name:   "sort gt/N",
			fields: fields{Hash: "value1", SortGt: "1529665668588"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyGreaterThan(expression.Key("Attribute2"), expression.Value(int64(1529665668588))),
		},
		{
			name:   "sort ge/N",
			fields: fields{Hash: "value1", SortGe: "1529665668588"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyGreaterThanEqual(expression.Key("Attribute2"), expression.Value(int64(1529665668588))),
		},
		{
			name:   "sort lt/N",
			fields: fields{Hash: "value1", SortLt: "1529665668588"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyLessThan(expression.Key("Attribute2"), expression.Value(int64(1529665668588))),
		},
		{
			name:   "sort le/N",
			fields: fields{Hash: "value1", SortLe: "1529665668588"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyLessThanEqual(expression.Key("Attribute2"), expression.Value(int64(1529665668588))),
		},
		{
			name:   "sort between/N",
			fields: fields{Hash: "value1", SortBetween: []string{"1529665592540", "1529665668588"}},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				definitions: map[string]string{"Attribute2": dynamodb.ScalarAttributeTypeN}},
			want: expression.KeyBetween(expression.Key("Attribute2"), expression.Value(int64(1529665592540)), expression.Value(int64(1529665668588))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp := &QueryParams{
				Hash:           tt.fields.Hash,
				Sort:           tt.fields.Sort,
				SortGt:         tt.fields.SortGt,
				SortGe:         tt.fields.SortGe,
				SortLt:         tt.fields.SortLt,
				SortLe:         tt.fields.SortLe,
				SortBeginsWith: tt.fields.SortBeginsWith,
				SortBetween:    tt.fields.SortBetween,
			}
			if cond := qp.sortKeyConditionBuilder(tt.args.key, tt.args.definitions); !reflect.DeepEqual(cond, tt.want) {
				t.Errorf("sortKeyConditionBuilder() = %v, want %v", cond, tt.want)
			}
		})
	}
}

func TestQueryParamsKeyConditionExpression(t *testing.T) {
	type fields struct {
		Hash           string
		Sort           string
		SortGt         string
		SortGe         string
		SortLt         string
		SortLe         string
		SortBeginsWith string
		SortBetween    []string
	}
	type args struct {
		key         []*dynamodb.KeySchemaElement
		definitions []*dynamodb.AttributeDefinition
	}
	createExpression := func(cond expression.KeyConditionBuilder) expression.Expression {
		expr, _ := expression.NewBuilder().WithKeyCondition(cond).Build()
		return expr
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   expression.Expression
	}{
		{
			name:   "hash/S",
			fields: fields{Hash: "value1"},
			args: args{key: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("Attribute1"),
				KeyType:       aws.String(dynamodb.KeyTypeHash)}},
				definitions: []*dynamodb.AttributeDefinition{
					{AttributeName: aws.String("Attribute1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
				}},
			want: createExpression(expression.KeyEqual(expression.Key("Attribute1"), expression.Value("value1"))),
		},
		{
			name:   "hash/N",
			fields: fields{Hash: "1529665668588"},
			args: args{key: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("Attribute1"),
				KeyType:       aws.String(dynamodb.KeyTypeHash)}},
				definitions: []*dynamodb.AttributeDefinition{
					{AttributeName: aws.String("Attribute1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeN)},
				}},
			want: createExpression(expression.KeyEqual(expression.Key("Attribute1"), expression.Value(int64(1529665668588)))),
		},
		{
			name:   "hash/S and sort/S",
			fields: fields{Hash: "value1", Sort: "value2"},
			args: args{key: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("Attribute1"),
					KeyType:       aws.String(dynamodb.KeyTypeHash)},
				{
					AttributeName: aws.String("Attribute2"),
					KeyType:       aws.String(dynamodb.KeyTypeRange)}},
				definitions: []*dynamodb.AttributeDefinition{
					{AttributeName: aws.String("Attribute1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
					{AttributeName: aws.String("Attribute2"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
				}},
			want: createExpression(
				expression.KeyEqual(expression.Key("Attribute1"), expression.Value("value1")).And(
					expression.KeyEqual(expression.Key("Attribute2"), expression.Value("value2")))),
		},
		{
			name:   "hash/N and sort/N",
			fields: fields{Hash: "1529665592540", Sort: "1529665668588"},
			args: args{key: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("Attribute1"),
					KeyType:       aws.String(dynamodb.KeyTypeHash)},
				{
					AttributeName: aws.String("Attribute2"),
					KeyType:       aws.String(dynamodb.KeyTypeRange)}},
				definitions: []*dynamodb.AttributeDefinition{
					{AttributeName: aws.String("Attribute1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeN)},
					{AttributeName: aws.String("Attribute2"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeN)},
				}},
			want: createExpression(
				expression.KeyEqual(expression.Key("Attribute1"), expression.Value(int64(1529665592540))).And(
					expression.KeyEqual(expression.Key("Attribute2"), expression.Value(int64(1529665668588))))),
		},
		{
			name:   "hash/S and sort between/N",
			fields: fields{Hash: "value1", SortBetween: []string{"1529665592540", "1529665668588"}},
			args: args{key: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("Attribute1"),
					KeyType:       aws.String(dynamodb.KeyTypeHash)},
				{
					AttributeName: aws.String("Attribute2"),
					KeyType:       aws.String(dynamodb.KeyTypeRange)}},
				definitions: []*dynamodb.AttributeDefinition{
					{AttributeName: aws.String("Attribute1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
					{AttributeName: aws.String("Attribute2"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeN)},
				}},
			want: createExpression(
				expression.KeyEqual(expression.Key("Attribute1"), expression.Value("value1")).And(
					expression.KeyBetween(expression.Key("Attribute2"),
						expression.Value(int64(1529665592540)), expression.Value(int64(1529665668588))))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qp := &QueryParams{
				Hash:           tt.fields.Hash,
				Sort:           tt.fields.Sort,
				SortGt:         tt.fields.SortGt,
				SortGe:         tt.fields.SortGe,
				SortLt:         tt.fields.SortLt,
				SortLe:         tt.fields.SortLe,
				SortBeginsWith: tt.fields.SortBeginsWith,
				SortBetween:    tt.fields.SortBetween,
			}
			if expr := qp.keyConditionExpression(tt.args.key, tt.args.definitions); aws.StringValue(expr.KeyCondition()) != aws.StringValue(tt.want.KeyCondition()) &&
				!reflect.DeepEqual(expr.Values(), tt.want.Values()) &&
				!reflect.DeepEqual(expr.Names(), tt.want.Names()) {
				t.Errorf("keyConditionExpression() = %v, %v, %v, want %v, %v, %v",
					aws.StringValue(expr.KeyCondition()), expr.Values(), expr.Names(),
					aws.StringValue(tt.want.KeyCondition()), tt.want.Values(), tt.want.Names())
			}
		})
	}
}

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (m mockDynamoDBClient) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	switch aws.StringValue(input.TableName) {
	case "t1":
		return &dynamodb.ScanOutput{Items: []map[string]*dynamodb.AttributeValue{
			{
				"Z":  {S: aws.String("z")},
				"B":  {S: aws.String("b")},
				"C":  {S: aws.String("c")},
				"A":  {S: aws.String("a")},
				"Id": {S: aws.String("id")},
			},
		}}, nil
	case "t2":
		return &dynamodb.ScanOutput{Items: []map[string]*dynamodb.AttributeValue{
			{
				"Z":   {S: aws.String("z")},
				"B":   {S: aws.String("b")},
				"C":   {S: aws.String("c")},
				"A":   {S: aws.String("a")},
				"Id1": {S: aws.String("id1")},
				"Id2": {S: aws.String("id2")},
			},
		}}, nil
	case "t3":
		return &dynamodb.ScanOutput{Items: []map[string]*dynamodb.AttributeValue{
			{
				"Z":  {S: aws.String("z")},
				"B":  {S: aws.String("b")},
				"C":  {S: aws.String("c")},
				"A":  {S: aws.String("a")},
				"Id": {S: aws.String("id")},
				"T1": {S: aws.String("t1")},
			},
		}}, nil
	case "t4":
		return &dynamodb.ScanOutput{Items: []map[string]*dynamodb.AttributeValue{
			{
				"Z":  {S: aws.String("z")},
				"B":  {S: aws.String("b")},
				"C":  {S: aws.String("c")},
				"A":  {S: aws.String("a")},
				"Id": {S: aws.String("id")},
				"T1": {S: aws.String("t1")},
				"T2": {S: aws.String("t2")},
			},
		}}, nil
	case "t5":
		return &dynamodb.ScanOutput{Items: []map[string]*dynamodb.AttributeValue{
			{
				"Z":  {S: aws.String("z")},
				"B":  {S: aws.String("b")},
				"C":  {S: aws.String("c")},
				"A":  {S: aws.String("a")},
				"Id": {S: aws.String("id")},
				"T1": {S: aws.String("t1")},
				"T2": {S: aws.String("t2")},
				"T3": {S: aws.String("t3")},
			},
		}}, nil
	case "t6":
		return &dynamodb.ScanOutput{Items: []map[string]*dynamodb.AttributeValue{
			{
				"Z":  {S: aws.String("z")},
				"B":  {S: aws.String("b")},
				"C":  {S: aws.String("c")},
				"A":  {S: aws.String("a")},
				"Id": {S: aws.String("id")},
				"T1": {S: aws.String("t1")},
				"T2": {S: aws.String("t2")},
				"T3": {S: aws.String("t3")},
				"T4": {S: aws.String("t4")},
			},
		}}, nil
	}
	return nil, nil
}

func TestDefineBaselineAttributes(t *testing.T) {
	type args struct {
		svc            dynamodbiface.DynamoDBAPI
		table          *dynamodb.TableDescription
		indexes        []*dynamodb.GlobalSecondaryIndexDescription
		index          string
		skipAttributes map[string]bool
	}
	tests := []struct {
		name  string
		args  args
		want1 []string
		want2 map[string]bool
	}{
		{
			name: "table with only hash key and no indexes",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t1"),
					KeySchema: []*dynamodb.KeySchemaElement{{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)}}},
				indexes:        []*dynamodb.GlobalSecondaryIndexDescription{},
				index:          "",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"Id", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with hash and sort keys and no indexes",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t2"),
					KeySchema: []*dynamodb.KeySchemaElement{
						{AttributeName: aws.String("Id1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
						{AttributeName: aws.String("Id2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
				},
				indexes:        []*dynamodb.GlobalSecondaryIndexDescription{},
				index:          "",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"Id1", "Id2", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id1": true, "Id2": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with only hash key and one index with hash key",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t3"),
					KeySchema: []*dynamodb.KeySchemaElement{{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)}}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)}},
					}},
				index:          "",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"Id", "T1", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with only hash key and one index with hash and sort keys",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t4"),
					KeySchema: []*dynamodb.KeySchemaElement{{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)}}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
					}},
				index:          "",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"Id", "T1", "T2", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "T2": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with only hash key and one index with hash and sort keys sorted by index",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t4"),
					KeySchema: []*dynamodb.KeySchemaElement{{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)}}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
					}},
				index:          "i1",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"T1", "T2", "Id", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "T2": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with only hash key and 2 indexes with hash and sort keys",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t5"),
					KeySchema: []*dynamodb.KeySchemaElement{{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)}}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
					},
					{
						IndexName: aws.String("i2"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T3"), KeyType: aws.String(dynamodb.KeyTypeHash)}},
					}},
				index:          "",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"Id", "T1", "T2", "T3", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "T2": true, "T3": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with only hash key and 2 indexes with hash and sort keys sorted by index",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t5"),
					KeySchema: []*dynamodb.KeySchemaElement{{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)}}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
					},
					{
						IndexName: aws.String("i2"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T3"), KeyType: aws.String(dynamodb.KeyTypeHash)}},
					}},
				index:          "i2",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"T3", "Id", "T1", "T2", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "T2": true, "T3": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with hash and sort keys, 2 indexes with hash and sort keys, table and index sharing same keys",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t6"),
					KeySchema: []*dynamodb.KeySchemaElement{
						{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)},
						{AttributeName: aws.String("T4"), KeyType: aws.String(dynamodb.KeyTypeRange)},
					}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
					},
					{
						IndexName: aws.String("i2"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T3"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T4"), KeyType: aws.String(dynamodb.KeyTypeRange)},
						},
					}},
				index:          "",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"Id", "T4", "T1", "T2", "T3", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "T2": true, "T3": true, "T4": true, "A": true, "B": true, "C": true, "Z": true},
		},
		{
			name: "table with hash and sort keys, 2 indexes with hash and sort keys, table and index sharing same " +
				"keys sorted by index",
			args: args{
				svc: mockDynamoDBClient{},
				table: &dynamodb.TableDescription{
					TableName: aws.String("t6"),
					KeySchema: []*dynamodb.KeySchemaElement{
						{AttributeName: aws.String("Id"), KeyType: aws.String(dynamodb.KeyTypeHash)},
						{AttributeName: aws.String("T4"), KeyType: aws.String(dynamodb.KeyTypeRange)},
					}},
				indexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: aws.String("i1"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T2"), KeyType: aws.String(dynamodb.KeyTypeRange)}},
					},
					{
						IndexName: aws.String("i2"),
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: aws.String("T3"), KeyType: aws.String(dynamodb.KeyTypeHash)},
							{AttributeName: aws.String("T4"), KeyType: aws.String(dynamodb.KeyTypeRange)},
						},
					}},
				index:          "i1",
				skipAttributes: map[string]bool{},
			},
			want1: []string{"T1", "T2", "Id", "T4", "T3", "A", "B", "C", "Z"},
			want2: map[string]bool{"Id": true, "T1": true, "T2": true, "T3": true, "T4": true, "A": true, "B": true, "C": true, "Z": true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := defineBaselineAttributes(tt.args.svc, tt.args.table, tt.args.indexes, tt.args.index, tt.args.skipAttributes)
			if !reflect.DeepEqual(got, tt.want1) {
				t.Errorf("defineBaselineAttributes() got = %v, want %v", got, tt.want1)
			}
			if !reflect.DeepEqual(got1, tt.want2) {
				t.Errorf("defineBaselineAttributes() got1 = %v, want %v", got1, tt.want2)
			}
		})
	}
}
