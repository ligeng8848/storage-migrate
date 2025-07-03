package oss

import (
	"copy-form-other/types"
	"fmt"
	openapipackage "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	mgwpackage "github.com/alibabacloud-go/hcs-mgw-20240626/client"
)

type Config struct {
	Ak       string `json:"ak"`
	Sk       string `json:"sk"`
	Region   string `json:"region"`
	UserId   string `json:"userId"`
	Endpoint string `json:"endpoint"`
}

func NewClient(option *types.ConfigOptions) (*Client, error) {
	config := openapipackage.Config{
		AccessKeyId:     &option.Ak,
		AccessKeySecret: &option.Sk,
		Endpoint:        &option.Endpoint,
	}
	client, err := mgwpackage.NewClient(&config)
	if err != nil {
		return nil, fmt.Errorf("create client failed: %w", err)
	}
	return &Client{Client: client}, nil
}
