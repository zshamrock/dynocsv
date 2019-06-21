package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"os"
)

const (
	profileNameEnvVar = "AWS_PROFILE"
)

var sess *session.Session

func init() {
	profile, ok := os.LookupEnv(profileNameEnvVar)
	if !ok {
		profile = session.DefaultSharedConfigProfile
	}
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
}

func GetSession() *session.Session {
	return sess
}
