package tos

import (
	"copy-form-other/util/uuid"
	"fmt"
	"github.com/volcengine/volcengine-go-sdk/service/dms"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
	"regexp"
	"strings"
)

type Client struct {
	Client *dms.DMS
}

func NewTosClient() (*Client, error) {
	// 这里假设你已经设置了环境变量 VOLCENGINE_ACCESS_KEY_ID 和 VOLCENGINE_ACCESS_KEY_SECRET
	config := volcengine.NewConfig().
		WithCredentials(credentials.NewEnvCredentials()).
		WithRegion("cn-north-1") // 替换为你的实际区域

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("创建 TOS 客户端失败: %w", err)
	}

	client := dms.New(sess)
	return &Client{Client: client}, nil
}

type AddressOption struct {
	Ak         string
	Sk         string
	Endpoint   string // 可选 源端必填，目标端可不填
	Bucket     string
	Region     string
	Vendor     string   // 示例: bos 只能选 StorageVendorS3
	PrefixList []string // 可选
	KeyFile    string   // 可选
}
type BasicConfig struct {
	OverwritePolicy string // 强制覆盖 Force  None 不覆盖 LastModify 最后修改时间
	SourceType      string // 源类型 StorageTypeObject StorageTypeUrl
	StorageClass    string // 存储类型 Standard InheritSource ... 详情见  https://www.volcengine.com/docs/6500/1277532
}

func (c *Client) CreateMigrateAddress(srcOption, destOption AddressOption,
	basicConfig BasicConfig,
	patternOverride, replaceOverride string, // 用户自定义 pattern & replace
) (*dms.CreateDataMigrateTaskOutput, string, error) {
	uuidPart := uuid.GenUUID()
	// 清洗 uuidPart：只保留字母、数字
	cleaned := strings.Trim(uuidPart, "-")
	cleaned = strings.ReplaceAll(cleaned, "_", "") // 防止 UUID 中带非法符号
	cleaned = regexp.MustCompile(`[^a-zA-Z0-9-]`).ReplaceAllString(cleaned, "")
	taskName := fmt.Sprintf("migrate-job-id-%s", cleaned[:8])
	var prefixList []*string
	var pattern, replace string
	if patternOverride != "" && replaceOverride != "" {
		pattern = patternOverride
		replace = replaceOverride
	} else {
		switch {
		case srcOption.KeyFile != "":
			// 单文件复制
			prefixList = []*string{volcengine.String(srcOption.KeyFile)}

			if destOption.KeyFile != "" {
				// 文件 → 文件
				pattern = fmt.Sprintf("^%s$", srcOption.KeyFile)
				replace = destOption.KeyFile
			} else if len(destOption.PrefixList) > 0 {
				// 文件 → 目录（保持文件名）
				pattern = fmt.Sprintf("^%s$", srcOption.KeyFile)
				replace = fmt.Sprintf("%s$1", strings.TrimSuffix(destOption.PrefixList[0], "/")+"/")
			} else {
				return nil, "", fmt.Errorf("目标地址必须指定 Key 或 PrefixList")
			}

		case len(srcOption.PrefixList) > 0:
			// 路径复制
			for _, p := range srcOption.PrefixList {
				prefixList = append(prefixList, volcengine.String(p))
			}

			if len(destOption.PrefixList) > 0 {
				// 路径 → 路径
				srcPrefix := strings.TrimSuffix(srcOption.PrefixList[0], "/") + "/"
				dstPrefix := strings.TrimSuffix(destOption.PrefixList[0], "/") + "/"
				pattern = fmt.Sprintf("^%s(.*)$", srcPrefix)
				replace = dstPrefix + "$1"
			} else if destOption.KeyFile != "" {
				// 路径 → 单文件不支持
				return nil, "", fmt.Errorf("路径复制不支持目标为单个文件")
			} else {
				return nil, "", fmt.Errorf("目标地址必须指定 Key 或 PrefixList")
			}

		default:
			return nil, "", fmt.Errorf("源地址必须指定 Key 或 PrefixList")
		}
	}
	// 创建迁移任务
	createResp, err := c.Client.CreateDataMigrateTask(&dms.CreateDataMigrateTaskInput{
		BasicConfig: &dms.BasicConfigForCreateDataMigrateTaskInput{
			TaskName:        volcengine.String(taskName),
			OverwritePolicy: volcengine.String(basicConfig.OverwritePolicy),
			SourceType:      volcengine.String(basicConfig.SourceType),
			StorageClass:    volcengine.String(basicConfig.StorageClass),
		},
		Source: &dms.SourceForCreateDataMigrateTaskInput{
			ObjectSourceConfig: &dms.ObjectSourceConfigForCreateDataMigrateTaskInput{
				BucketAccessConfig: &dms.BucketAccessConfigForCreateDataMigrateTaskInput{
					AK:         volcengine.String(srcOption.Ak),
					SK:         volcengine.String(srcOption.Sk),
					Endpoint:   volcengine.String(srcOption.Endpoint),
					BucketName: volcengine.String(srcOption.Bucket),
					Region:     volcengine.String(srcOption.Region),
					Vendor:     volcengine.String(srcOption.Vendor),
				},
				PrefixList: prefixList,
			},
		},
		AdvanceConfig: &dms.AdvanceConfigForCreateDataMigrateTaskInput{
			RenameSetting: &dms.RenameSettingForCreateDataMigrateTaskInput{
				Pattern:    volcengine.String(pattern),
				ReplaceStr: volcengine.String(replace),
			},
		},
		Target: &dms.TargetForCreateDataMigrateTaskInput{
			AK:         volcengine.String(destOption.Ak),
			SK:         volcengine.String(destOption.Sk),
			BucketName: volcengine.String(destOption.Bucket),
		},
	})
	if err != nil {
		return nil, "", fmt.Errorf("创建迁移任务失败: %w", err)
	}
	return createResp, taskName, nil
}
func (c *Client) GetMigrateJobStatus(TaskID int64) (*dms.QueryDataMigrateTaskOutput, error) {
	resp, err := c.Client.QueryDataMigrateTask(&dms.QueryDataMigrateTaskInput{
		TaskID: volcengine.Int64(TaskID),
	})
	if err != nil {
		return nil, fmt.Errorf("获取迁移任务状态失败: %w", err)
	}
	return resp, nil
}
func (c *Client) ListMigrateJob(listOption *dms.ListDataMigrateTaskInput) (*dms.ListDataMigrateTaskOutput, error) {
	resp, err := c.Client.ListDataMigrateTask(&dms.ListDataMigrateTaskInput{
		Limit:      listOption.Limit,
		Offset:     listOption.Offset,
		TaskStatus: listOption.TaskStatus,
	})
	if err != nil {
		return nil, fmt.Errorf("列出迁移任务失败: %w", err)
	}
	return resp, nil
}
func (c *Client) StopMigrateJob(TaskID int64) (*dms.StopDataMigrateTaskOutput, error) {
	resp, err := c.Client.StopDataMigrateTask(&dms.StopDataMigrateTaskInput{
		TaskID: volcengine.Int64(TaskID),
	})
	if err != nil {
		return nil, fmt.Errorf("停止迁移任务失败: %w", err)
	}
	return resp, nil
}
