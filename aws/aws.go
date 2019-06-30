package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
)

const (
	profileNameEnvVar = "AWS_PROFILE"
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
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
	return sess
}
