package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"os"
	"path/filepath"
	"testing"
)

func TestDetectRuntime(t *testing.T) {
	tests := []struct {
		name     string
		envName  string
		envValue string
		want     runtime
	}{
		{
			name:     "snap env is set",
			envName:  snapEnvName,
			envValue: "/snap/dynocsv/x1",
			want:     snap,
		},
		{
			name:     "snap name env is set",
			envName:  snapNameEnvName,
			envValue: "dynocsv",
			want:     snap,
		},
		{
			name:     "snap revision env is set",
			envName:  snapRevisionEnvName,
			envValue: "x1",
			want:     snap,
		},
		{
			name:     "none runtime",
			envName:  "",
			envValue: "",
			want:     none,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envName != "" {
				_ = os.Setenv(tt.envName, tt.envValue)
			}
			if got := detectRuntime(); got != tt.want {
				_ = os.Unsetenv(tt.envName)
				t.Errorf("detectRuntime() = %v, want %v", got, tt.want)
			}
			_ = os.Unsetenv(tt.envName)
		})
	}
}

func TestSetActualUserHome(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "set actual user home from snap runtime"},
	}
	for _, tt := range tests {
		want := os.ExpandEnv("$" + homeEnvName)
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Setenv(homeEnvName, want+"/snap/dynocsv/x1")
			_ = os.Setenv(snapNameEnvName, "dynocsv")
			_ = os.Setenv(snapRevisionEnvName, "x1")
			setActualUserHome()
			_ = os.Unsetenv(snapNameEnvName)
			_ = os.Unsetenv(snapRevisionEnvName)
			got := os.ExpandEnv("$" + homeEnvName)
			// restore HOME back to its original value no matter whether the test passes or not
			_ = os.Setenv(homeEnvName, want)
			if want != got {
				t.Errorf("setActualUserHome() = %v, want %v", got, want)
			}
		})
	}
}

func TestGetSession(t *testing.T) {
	tests := []struct {
		name       string
		envs       map[string]string
		homeSuffix string
		region     string
	}{
		{
			name:       "build session from snap runtime",
			envs:       map[string]string{snapNameEnvName: "dynocsv", snapRevisionEnvName: "x1"},
			homeSuffix: "/snap/dynocsv/x1",
			region:     "us-east-1",
		},
		{
			name:       "build session from none runtime",
			envs:       map[string]string{},
			homeSuffix: "",
			region:     "us-east-1",
		},
	}
	for _, tt := range tests {
		dir, _ := os.Getwd()
		home := os.ExpandEnv("$" + homeEnvName)
		_ = os.Setenv(homeEnvName, filepath.Join(dir, tt.homeSuffix))
		for k, v := range tt.envs {
			_ = os.Setenv(k, v)
		}
		session := GetSession("dynocsv")
		// restore HOME back to its original value
		_ = os.Setenv(homeEnvName, home)
		for k, _ := range tt.envs {
			_ = os.Unsetenv(k)
		}
		if got := aws.StringValue(session.Config.Region); got != tt.region {
			t.Errorf("Region = %v, does not match %v", got, tt.region)
		}
	}
}
