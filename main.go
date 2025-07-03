package main

func main() {
	////////////////////////// 测试OSS在线迁移服务 //////////////////////////
	// step1 创建迁移客户端
	/* cfg := &types.ConfigOptions{
		Ak:       "xxxx",
		Sk:       "xxxxx",
		Region:   "cn-beijing",
		UserId:   "1726xxxx79",
		Endpoint: "cn-beijing.mgw.aliyuncs.com",
	}
	mergeClient, err := oss.NewClient(cfg)
	if err != nil {
		panic(err)
	}
	// step2 创建源地址
	//////////////  bos 示例 //////////////
	//srcAddr, err := mergeClient.CreateMigrateAddress(cfg.UserId, oss.AddressOption{
	//	AddressType:  "bos",
	//	Bucket:       "prod-lpai-asset",
	//	Prefix:       "dataset/test-title/25-07-01-5-20250701170443/",
	//	AccessId:     "xxx",
	//	AccessSecret: "xxx",
	//	Domain:       "bj.bcebos.com",
	//	RegionId:     "bj",
	//})
	//if err != nil {
	//	log.Fatalf("create src address failed: %v", err)
	//}
	//////////////  tos 示例 //////////////
	srcAddr, err := mergeClient.CreateMigrateAddress(cfg.UserId, oss.AddressOption{
		AddressType:  "tos",
		Bucket:       "prod-lpai-asset",
		Prefix:       "dataset/rootsautomation__RICO-ScreenQA/main-20250626174632/",
		AccessId:     "xxx",
		AccessSecret: "xxxx==",
		Domain:       "tos-s3-cn-beijing.volces.com",
		RegionId:     "cn-beijing",
	})
	if err != nil {
		log.Fatalf("create src address failed: %v", err)
	}
	// step3 创建目的地址
	dstAddr, err := mergeClient.CreateMigrateAddress(cfg.UserId, oss.AddressOption{
		AddressType:  "oss",
		Bucket:       "prod-lpai-asset",
		Prefix:       "dataset/test-title/25-07-01-4-20250701164534/",
		AccessId:     "xxx",
		AccessSecret: "xxx",
		RegionId:     "oss-cn-shanghai",
	})
	if err != nil {
		log.Fatalf("create dest address failed: %v", err)
	}
	// step4 创建迁移任务
	jobResp, jobName, err := mergeClient.CreateMigrateJob(cfg.UserId, *srcAddr, *dstAddr, "", "", nil, nil, nil)
	if err != nil {
		log.Fatalf("create migrate job failed: %v", err)
	}
	log.Printf("create migrate job success, jobId: %s", *jobResp)
	// step5 查询迁移任务状态
	for {
		jobInfoResp, err := mergeClient.GetMigrateJob(cfg.UserId, jobName)
		if err != nil {
			log.Printf("get job status failed: %v", err)
			return
		}
		status := *jobInfoResp.Body.ImportJob.Status
		log.Printf("当前任务状态: %s， jobName: %s", status, jobName)

		// 获取任务历史以便显示复制进度
		historyResp, err := mergeClient.ListMigrateJobLastHistory(cfg.UserId, jobName)
		if err != nil {
			log.Printf("list job history failed: %v", err)
			return
		}
		if len(historyResp.Body.JobHistoryList.JobHistory) == 0 {
			log.Printf("no job history found")
			time.Sleep(10 * time.Second)
			continue
		}
		h := historyResp.Body.JobHistoryList.JobHistory[0]
		if status != "IMPORT_JOB_LAUNCHING" && (h.CopiedCount != nil && *h.CopiedCount >= 0) {
			log.Printf("进度: %d/%d 文件, %d/%d bytes 已复制, %d 失败", *h.CopiedCount, *h.TotalCount, *h.CopiedSize, *h.TotalSize, *h.FailedCount)
		} else {
			log.Printf("任务正在启动中，等待状态切换...")
		}

		if status == "IMPORT_JOB_INTERRUPTED" {
			log.Printf("job is interrupted")
			return
		}
		if status == "IMPORT_JOB_FINISHED" {
			log.Println("job is finished")
			break
		}
		// 如果复制完成但状态还在 doing，提示等待状态切换
		if *h.CopiedCount == *h.TotalCount && *h.FailedCount == 0 && status == "IMPORT_JOB_DOING" {
			log.Println("已复制完成，等待任务状态切换为 FINISHED... ")
			log.Printf("进度: %d/%d 文件, %d/%d bytes 已复制, %d 失败", *h.CopiedCount, *h.TotalCount, *h.CopiedSize, *h.TotalSize, *h.FailedCount)
		}

		time.Sleep(10 * time.Second)
	}
	// step6 查询已完成迁移任务
	historyResp, err := mergeClient.ListMigrateJobHistory(cfg.UserId, jobName, &mgwpackage.ListJobHistoryRequest{})
	if err != nil {
		log.Printf("listJobHistory failed: %v", err)
		return
	}
	fmt.Println(historyResp.Body.JobHistoryList.JobHistory) */

	////////////////////////// 测试TOS在线迁移服务 //////////////////////////
	// step1 创建迁移任务
	/* cfg := &types.ConfigOptions{
		Ak:     "xxxx",
		Sk:     "xxx==",
		Region: "cn-beijing",
	}
	mergeClient, err := tos.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	r, err := mergeClient.ListMigrateJob(&dms.ListDataMigrateTaskInput{})
	if err != nil {
		panic(fmt.Errorf("获取迁移任务列表失败: %w", err))
	}
	fmt.Printf("迁移任务列表: %v\n", r)
	srcOption := &tos.AddressOption{
		Ak:         "xx",
		Sk:         "xxxx",
		Endpoint:   "http://oss-cn-shanghai.aliyuncs.com",
		Bucket:     "prod-lpai-asset",
		Region:     "cn-shanghai",
		Vendor:     "StorageVendorOSS",
		PrefixList: []string{"dataset/test-title/25-07-01-4-20250701164534/"},
	}
	destOption := &tos.AddressOption{
		Ak:         "xx",
		Sk:         "xx==",
		Bucket:     "prod-lpai-asset",
		PrefixList: []string{"dataset/test-title/25-07-01-2-20250701102559/"},
	}
	basicConfig := tos.BasicConfig{
		OverwritePolicy: "Force",             // 强制覆盖
		SourceType:      "StorageTypeObject", // 源类型
		StorageClass:    "Standard",          // 存储类型
	}
	resp, jobName, err := mergeClient.CreateMigrateAddress(*srcOption, *destOption, basicConfig, "", "")
	if err != nil {
		panic(err)
	}
	fmt.Printf("迁移任务创建成功，任务ID: %v, 任务名称： %s\n", *resp.TaskID, jobName)
	// step2 查询迁移任务状态
	taskInfo, err := mergeClient.GetMigrateJobStatus(*resp.TaskID)
	if err != nil {
		panic(fmt.Errorf("获取迁移任务状态失败: %w", err))
	}
	fmt.Printf("迁移任务状态: %s, 任务ID: %v\n", *taskInfo, *resp.TaskID) */
}
