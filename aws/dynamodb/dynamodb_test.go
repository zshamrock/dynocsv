package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"testing"
)

// TODO: Implement
func TestGetValue(t *testing.T) {
	type args struct {
		av *dynamodb.AttributeValue
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := getValue(tt.args.av); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getValue() = %v, want %v", got, tt.want)
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
