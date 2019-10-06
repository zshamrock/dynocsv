package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
	"strings"
)

const (
	profileNameEnvVar = "AWS_PROFILE"
)

type runtime int

const (
	none runtime = iota
	snap
)

const (
	snapEnvName         = "SNAP"
	snapNameEnvName     = "SNAP_NAME"
	snapRevisionEnvName = "SNAP_REVISION"
	homeEnvName         = "HOME"
)

func GetSession(profile string) *session.Session {
	p := profile
	if profile == "" {
		p = getEnvProfileNameOrDefault()
	}
	return setupSession(p)
}

func getEnvProfileNameOrDefault() string {
	profile, ok := os.LookupEnv(profileNameEnvVar)
	if !ok {
		profile = session.DefaultSharedConfigProfile
	}
	return profile
}

func setupSession(profile string) *session.Session {
	r := detectRuntime()
	if r == snap {
		setActualUserHome()
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
	return sess
}

func detectRuntime() runtime {
	if isEnvSet(snapEnvName) || isEnvSet(snapNameEnvName) || isEnvSet(snapRevisionEnvName) {
		return snap
	}
	return none
}

func isEnvSet(name string) bool {
	_, found := os.LookupEnv(name)
	return found
}

func setActualUserHome() {
	home := os.ExpandEnv("$" + homeEnvName)
	_ = os.Setenv(
		homeEnvName,
		strings.TrimSuffix(home, os.ExpandEnv("/snap/$"+snapNameEnvName+"/$"+snapRevisionEnvName)))
}
