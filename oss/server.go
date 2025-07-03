package oss

import (
	"copy-form-other/util"
	"copy-form-other/util/uuid"
	"fmt"
	mgwpackage "github.com/alibabacloud-go/hcs-mgw-20240626/client"
	"log"
	"time"
)

type Client struct {
	Client *mgwpackage.Client
}

type AddressOption struct {
	AddressType  string `json:"addressType"`
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	AccessId     string `json:"accessId"`
	AccessSecret string `json:"accessSecret"`
	Domain       string `json:"domain"`   // 如果目标是OSS可以不填。源必须填写。
	RegionId     string `json:"regionId"` // 如果是bos类型的地址，需要填写regionId。
}

func (c *Client) CreateMigrateAddress(userId string, option AddressOption) (*string, error) {
	addressName := fmt.Sprintf("%s_%s_%s_%s", option.AddressType, option.Bucket, option.Prefix, time.Now().Format("20060102150405"))
	addressName = util.Hash(addressName)
	srcDetail := mgwpackage.AddressDetail{}

	if option.AddressType != "" {
		srcDetail.AddressType = &option.AddressType
	}
	if option.Bucket != "" {
		srcDetail.Bucket = &option.Bucket
	}
	if option.Prefix != "" {
		srcDetail.Prefix = &option.Prefix
	}
	if option.AccessId != "" {
		srcDetail.AccessId = &option.AccessId
	}
	if option.AccessSecret != "" {
		srcDetail.AccessSecret = &option.AccessSecret
	}
	if option.Domain != "" {
		srcDetail.Domain = &option.Domain
	}
	if option.RegionId != "" {
		srcDetail.RegionId = &option.RegionId
	}
	_, err := c.Client.CreateAddress(&userId, &mgwpackage.CreateAddressRequest{
		ImportAddress: &mgwpackage.CreateAddressInfo{
			Name:          &addressName,
			AddressDetail: &srcDetail,
		}})
	if err != nil {
		log.Printf("create src address failed: %v", err)
		return nil, err
	}
	// 验证源端数据地址可用性。
	verifySrcAddrResp, err := c.Client.VerifyAddress(&userId, &addressName)
	if err != nil {
		log.Printf("verify src address failed: %v", err)
		return nil, err
	}
	if *verifySrcAddrResp.Body.VerifyAddressResponse.Status != "available" {
		log.Printf("verify address status failed, status: " + *verifySrcAddrResp.Body.VerifyAddressResponse.Status)
		return nil, fmt.Errorf("verify address status failed, status: %s", *verifySrcAddrResp.Body.VerifyAddressResponse.Status)
	}
	return &addressName, nil
}

func (c *Client) CreateMigrateJob(userId, srcAddress, destAddress, overwriteMode, transferMode string,
	importQos *mgwpackage.ImportQos, filterRule *mgwpackage.FilterRule,
	scheduleRule *mgwpackage.ScheduleRule) (*mgwpackage.CreateJobResponse, string, error) {
	/*
	  overwriteMode和transferMode需要组合使用，具体组合含义如下
	  always,all 全覆盖
	  always,lastmodified 根据文件最后修改时间覆盖
	  never,changed 不覆盖
	*/
	jobName := fmt.Sprintf("migrateJobId_%s", uuid.GenUUID())
	jobName = util.Hash(jobName)
	// 设置默认值
	if overwriteMode == "" {
		overwriteMode = "always"
	}
	if transferMode == "" {
		transferMode = "all"
	}
	if importQos == nil {
		defaultQps := int64(1000)
		defaultBandwidth := int64(2147483648)
		importQos = &mgwpackage.ImportQos{
			MaxImportTaskQps: &defaultQps,
			MaxBandWidth:     &defaultBandwidth,
		}
	}
	resp, err := c.Client.CreateJob(&userId, &mgwpackage.CreateJobRequest{ImportJob: &mgwpackage.CreateJobInfo{
		Name:          &jobName,
		TransferMode:  &transferMode,
		OverwriteMode: &overwriteMode,
		SrcAddress:    &srcAddress,
		DestAddress:   &destAddress,
		ImportQos:     importQos,
		FilterRule:    filterRule,
		ScheduleRule:  scheduleRule,
	}})
	if err != nil {
		return nil, "", fmt.Errorf("create job failed: %w", err)
	}

	status := "IMPORT_JOB_LAUNCHING"
	_, err = c.Client.UpdateJob(&userId, &jobName, &mgwpackage.UpdateJobRequest{
		&mgwpackage.UpdateJobInfo{Status: &status},
	})
	if err != nil {
		return nil, "", fmt.Errorf("update job failed: %w", err)
	}
	return resp, jobName, nil
}
func (c *Client) UpdateMigrateJobStatus(userId, jobName, jobStatus string) (*mgwpackage.UpdateJobResponse, error) {
	// 更新任务状态
	resp, err := c.Client.UpdateJob(&userId, &jobName, &mgwpackage.UpdateJobRequest{
		&mgwpackage.UpdateJobInfo{Status: &jobStatus},
	})
	if err != nil {
		return nil, fmt.Errorf("update job failed: %w", err)
	}
	return resp, nil
}
func (c *Client) UpdateMigrateJob(userId, jobName string, dml *mgwpackage.UpdateJobRequest) (*mgwpackage.UpdateJobResponse, error) {
	// 更新任务
	resp, err := c.Client.UpdateJob(&userId, &jobName, dml)
	if err != nil {
		return nil, fmt.Errorf("update job failed: %w", err)
	}
	return resp, nil
}

func (c *Client) GetMigrateJob(userId, jobName string) (*mgwpackage.GetJobResponse, error) {
	// 获取任务状态
	resp, err := c.Client.GetJob(&userId, &jobName, &mgwpackage.GetJobRequest{})
	if err != nil {
		return nil, fmt.Errorf("get job status failed: %w", err)
	}
	return resp, nil
}

func (c *Client) ListMigrateJob(userId string, listOption *mgwpackage.ListJobRequest) (*mgwpackage.ListJobResponse, error) {
	resp, err := c.Client.ListJob(&userId, listOption)
	if err != nil {
		return nil, fmt.Errorf("list job failed: %w", err)
	}
	return resp, nil
}
func (c *Client) ListMigrateJobHistory(userId, jobName string, listOption *mgwpackage.ListJobHistoryRequest) (*mgwpackage.ListJobHistoryResponse, error) {
	// 获取任务历史以便显示复制进度
	resp, err := c.Client.ListJobHistory(&userId, &jobName, listOption)
	if err != nil {
		return nil, fmt.Errorf("list job history failed: %w", err)
	}
	return resp, nil
}
func (c *Client) ListMigrateJobLastHistory(userId, jobName string) (*mgwpackage.ListJobHistoryResponse, error) {
	// 获取最新任务历史以便显示复制进度
	count := int32(1)
	marker := ""
	resp, err := c.Client.ListJobHistory(&userId, &jobName, &mgwpackage.ListJobHistoryRequest{
		Count:  &count,
		Marker: &marker,
	})
	if err != nil {
		return nil, fmt.Errorf("get job history failed: %w", err)
	}
	return resp, nil
}
func (c *Client) DeleteMigrateJob(userId, jobName string) (*mgwpackage.DeleteJobResponse, error) {
	// 删除迁移任务
	resp, err := c.Client.DeleteJob(&userId, &jobName, &mgwpackage.DeleteJobRequest{})
	if err != nil {
		return nil, fmt.Errorf("delete job failed: %w", err)
	}
	return resp, nil
}
