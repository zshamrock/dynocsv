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
		want *string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getValue(tt.args.av); !reflect.DeepEqual(got, tt.want) {
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
		want *string
	}{
		{
			name: "empty number set",
			args: args{values: []*string{}},
			want: aws.String("[]"),
		},
		{
			name: "single entry set",
			args: args{values: []*string{aws.String("10")}},
			want: aws.String("[10]"),
		},
		{
			name: "multiple different entries set",
			args: args{values: []*string{aws.String("42.2"), aws.String("-19"), aws.String("7.5"), aws.String("3.14")}},
			want: aws.String("[42.2,-19,7.5,3.14]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processNumberSet(tt.args.values); aws.StringValue(got) != aws.StringValue(tt.want) {
				t.Errorf("processNumberSet() = %v, want %v", aws.StringValue(got), aws.StringValue(tt.want))
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
		want *string
	}{
		{
			name: "empty number set",
			args: args{values: []*string{}},
			want: aws.String("[]"),
		},
		{
			name: "single entry set",
			args: args{values: []*string{aws.String("Hello")}},
			want: aws.String("[Hello]"),
		},
		{
			name: "multiple different entries set",
			args: args{values: []*string{aws.String("Giraffe"), aws.String("Hippo"), aws.String("Zebra"), aws.String("3.14")}},
			want: aws.String("[Giraffe,Hippo,Zebra,3.14]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := processStringSet(tt.args.values); aws.StringValue(got) != aws.StringValue(tt.want) {
				t.Errorf("processStringSet() = %v, want %v", aws.StringValue(got), aws.StringValue(tt.want))
			}
		})
	}
}
