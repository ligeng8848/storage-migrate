package tos

import (
	"copy-form-other/types"
	"fmt"
	"github.com/volcengine/volcengine-go-sdk/service/dms"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
)

func NewClient(option *types.ConfigOptions) (*Client, error) {
	if option.Ak == "" || option.Sk == "" || option.Region == "" {
		return nil, fmt.Errorf("ak, sk and region must be provided")
	}
	fmt.Printf("Creating DMS client with Ak: %s, Sk: %s, Region: %s\n", option.Ak, option.Sk, option.Region)
	config := volcengine.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(option.Ak, option.Sk, "")).
		WithRegion(option.Region)

	sess, _ := session.NewSession(config)
	client := dms.New(sess)

	return &Client{Client: client}, nil
}
