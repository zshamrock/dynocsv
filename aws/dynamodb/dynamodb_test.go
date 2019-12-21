package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func TestQueryParamsHashQueryString(t *testing.T) {
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
		key       *dynamodb.KeySchemaElement
		outParams map[string]string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		want          string
		outParamsWant map[string]string
	}{
		{name: "hash",
			fields: fields{Hash: "value1"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute1"),
				KeyType:       aws.String(dynamodb.KeyTypeHash)},
				outParams: make(map[string]string)},
			want:          "Attribute1 = :hash1",
			outParamsWant: map[string]string{":hash1": "value1"}},
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
			if got := qp.HashQueryString(tt.args.key, tt.args.outParams); got != tt.want {
				t.Errorf("HashQueryString() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.args.outParams, tt.outParamsWant) {
				t.Errorf("HashQueryString out params don't match() = %v, want %v", tt.args.outParams, tt.outParamsWant)
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
		key       *dynamodb.KeySchemaElement
		outParams map[string]string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		want          string
		outParamsWant map[string]string
	}{
		{name: "sort",
			fields: fields{Hash: "value1", Sort: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 = :sort1",
			outParamsWant: map[string]string{":sort1": "value2"}},
		{name: "sort gt",
			fields: fields{Hash: "value1", SortGt: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 > :sort1",
			outParamsWant: map[string]string{":sort1": "value2"}},
		{name: "sort ge",
			fields: fields{Hash: "value1", SortGe: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 >= :sort1",
			outParamsWant: map[string]string{":sort1": "value2"}},
		{name: "sort lt",
			fields: fields{Hash: "value1", SortLt: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 < :sort1",
			outParamsWant: map[string]string{":sort1": "value2"}},
		{name: "sort le",
			fields: fields{Hash: "value1", SortLe: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 <= :sort1",
			outParamsWant: map[string]string{":sort1": "value2"}},
		{name: "sort begins with",
			fields: fields{Hash: "value1", SortBeginsWith: "value2"},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 BEGINS WITH :sort1",
			outParamsWant: map[string]string{":sort1": "value2"}},
		{name: "sort between",
			fields: fields{Hash: "value1", SortBetween: []string{"value2", "value3"}},
			args: args{key: &dynamodb.KeySchemaElement{
				AttributeName: aws.String("Attribute2"),
				KeyType:       aws.String(dynamodb.KeyTypeRange)},
				outParams: make(map[string]string)},
			want:          "Attribute2 BETWEEN :sort1 AND :sort2",
			outParamsWant: map[string]string{":sort1": "value2", ":sort2": "value3"}},
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
			if got := qp.SortQueryString(tt.args.key, tt.args.outParams); got != tt.want {
				t.Errorf("SortQueryString() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.args.outParams, tt.outParamsWant) {
				t.Errorf("SortQueryString out params don't match() = %v, want %v", tt.args.outParams, tt.outParamsWant)
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
		key       []*dynamodb.KeySchemaElement
		outParams map[string]string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		want          *string
		outParamsWant map[string]string
	}{
		{name: "hash",
			fields: fields{Hash: "value1"},
			args: args{key: []*dynamodb.KeySchemaElement{{
				AttributeName: aws.String("Attribute1"),
				KeyType:       aws.String(dynamodb.KeyTypeHash)}},
				outParams: make(map[string]string)},
			want:          aws.String("Attribute1 = :hash1"),
			outParamsWant: map[string]string{":hash1": "value1"}},
		{name: "hash and sort",
			fields: fields{Hash: "value1", Sort: "value2"},
			args: args{key: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("Attribute1"),
					KeyType:       aws.String(dynamodb.KeyTypeHash)},
				{
					AttributeName: aws.String("Attribute2"),
					KeyType:       aws.String(dynamodb.KeyTypeRange)}},
				outParams: make(map[string]string)},
			want:          aws.String("Attribute1 = :hash1 AND Attribute2 = :sort1"),
			outParamsWant: map[string]string{":hash1": "value1", ":sort1": "value2"}},
		{name: "hash and sort between",
			fields: fields{Hash: "value1", SortBetween: []string{"value2", "value3"}},
			args: args{key: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("Attribute1"),
					KeyType:       aws.String(dynamodb.KeyTypeHash)},
				{
					AttributeName: aws.String("Attribute2"),
					KeyType:       aws.String(dynamodb.KeyTypeRange)}},
				outParams: make(map[string]string)},
			want:          aws.String("Attribute1 = :hash1 AND Attribute2 BETWEEN :sort1 AND :sort2"),
			outParamsWant: map[string]string{":hash1": "value1", ":sort1": "value2", ":sort2": "value3"}},
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
			if got := qp.KeyConditionExpression(tt.args.key, tt.args.outParams); aws.StringValue(got) != aws.StringValue(tt.want) {
				t.Errorf("KeyConditionExpression() = %v, want %v", aws.StringValue(got), aws.StringValue(tt.want))
			}
			if !reflect.DeepEqual(tt.args.outParams, tt.outParamsWant) {
				t.Errorf("KeyConditionExpression out params don't match() = %v, want %v", tt.args.outParams, tt.outParamsWant)
			}
		})
	}
}
