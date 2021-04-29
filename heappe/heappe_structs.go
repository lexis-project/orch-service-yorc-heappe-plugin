// Copyright 2020 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package heappe

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
)

// FileTransferProtocol used to transfer files to the HPC cluster
type FileTransferProtocol int

const (
	// NetworkShare file transfer protocol
	NetworkShare FileTransferProtocol = 1
	// SftpScp file transfer protocol
	SftpScp = 2
)

// PasswordCredentials holds user/password to perform a basic authentication
type PasswordCredentials struct {
	Username string
	Password string
}

// Authentication parameters
type Authentication struct {
	Credentials PasswordCredentials
}

// OpenIDCredentials holds OpenID Connect token and user name to perform a token-based authentication
type OpenIDCredentials struct {
	OpenIdtoken string
	Username    string
}

// Authentication parameters
type OpenIDAuthentication struct {
	Credentials OpenIDCredentials
}

// CommandTemplateParameterValue holds a command template parameter
type CommandTemplateParameterValue struct {
	CommandParameterIdentifier string
	ParameterValue             string
}

// EnvironmentVariable holds an environment variable definition
type EnvironmentVariable struct {
	Name  string
	Value string
}

// TaskParalizationParameter holds paramters of tasks parallelization
type TaskParalizationParameter struct {
	MPIProcesses  int
	OpenMPThreads int
	MaxCores      int
}

// TaskSpecification holds task properties
type TaskSpecification struct {
	Name                       string
	MinCores                   int
	MaxCores                   int
	WalltimeLimit              int
	RequiredNodes              []string `json:"RequiredNodes,omitempty"`
	Priority                   int
	JobArrays                  string
	IsExclusive                bool
	IsRerunnable               bool
	CpuHyperThreading          bool
	StandardInputFile          string
	StandardOutputFile         string
	StandardErrorFile          string
	ProgressFile               string
	LogFile                    string
	ClusterTaskSubdirectory    string
	ClusterNodeTypeID          int                         `json:"ClusterNodeTypeId"`
	CommandTemplateID          int                         `json:"CommandTemplateId"`
	TaskParalizationParameters []TaskParalizationParameter `json:"TaskParalizationParameters,omitempty"`
	EnvironmentVariables       []EnvironmentVariable       `json:"EnvironmentVariables,omitempty"`
	// TODO: DependsOn
	TemplateParameterValues []CommandTemplateParameterValue `json:"TemplateParameterValues,omitempty"`
}

// JobSpecification holds job properties
type JobSpecification struct {
	Name                 string
	Project              string
	WaitingLimit         int
	NotificationEmail    string
	PhoneNumber          string
	NotifyOnAbort        bool
	NotifyOnFinish       bool
	NotifyOnStart        bool
	ClusterID            int `json:"ClusterId"`
	FileTransferMethodID int `json:"FileTransferMethodId"`
	Tasks                []TaskSpecification
}

// JobCreateRESTParams holds HEAppE REST API job creation parameters
type JobCreateRESTParams struct {
	JobSpecification JobSpecification
	SessionCode      string
}

// JobSubmitRESTParams holds HEAppE REST API job submission parameters
type JobSubmitRESTParams struct {
	CreatedJobInfoID int64 `json:"CreatedJobInfoId"`
	SessionCode      string
}

// JobInfoRESTParams holds HEAppE REST API job info parameters
type JobInfoRESTParams struct {
	SubmittedJobInfoID int64 `json:"SubmittedJobInfoId"`
	SessionCode        string
}

// TemplateParameter holds template parameters description in a job
type TemplateParameter struct {
	Identifier  string
	Description string
}

// CommandTemplate holds a command template description in a job
type CommandTemplate struct {
	ID                 int64 `json:"Id"`
	Name               string
	Description        string
	Code               string
	TemplateParameters []TemplateParameter
}

// ClusterNodeType holds a node description in a job
type ClusterNodeType struct {
	ID               int64 `json:"Id"`
	Name             string
	Description      string
	NumberOfNodes    int
	CoresPerNode     int
	MaxWalltime      int
	CommandTemplates []CommandTemplate
}

// TaskInfo holds a task description in a job
type TaskInfo struct {
	ID                int64 `json:"Id"`
	Name              string
	State             int
	Priority          int
	AllocatedTime     float64
	AllocatedCoreIds  []string
	StartTime         string
	EndTime           string
	NodeType          ClusterNodeType
	ErrorMessage      string
	CpuHyperThreading bool
}

// JobInfo holds the response to a job creation/submission
type JobInfo struct {
	ID                 int64 `json:"Id"`
	Name               string
	State              int
	Project            string
	CreationTime       string
	SubmitTime         string
	StartTime          string
	EndTime            string
	TotalAllocatedTime float64
	Tasks              []TaskInfo
}

// TaskFileOffset holds the offset to a file of a given task
type TaskFileOffset struct {
	SubmittedTaskInfoID int64 `json:"SubmittedTaskInfoId"`
	FileType            int
	Offset              int64
}

// DownloadPartsOfJobFilesRESTParams holds HEAppE parameters for the REST API
// allowing to download parts of files
type DownloadPartsOfJobFilesRESTParams struct {
	SubmittedJobInfoID int64 `json:"SubmittedJobInfoId"`
	TaskFileOffsets    []TaskFileOffset
	SessionCode        string
}

// JobFileContent holds the response to a partial download of job files
type JobFileContent struct {
	Content             string
	RelativePath        string
	Offset              int64
	FileType            int
	SubmittedTaskInfoID int64 `json:"SubmittedTaskInfoId"`
}

// AsymmetricKeyCredentials hold credentials used to transfer files to the HPC cluster
type AsymmetricKeyCredentials struct {
	Username   string
	PrivateKey string
	PublicKey  string
}

// FileTransferMethod holds properties allowing to transfer files to the HPC cluster
type FileTransferMethod struct {
	ServerHostname string
	SharedBasepath string
	Protocol       FileTransferProtocol
	Credentials    AsymmetricKeyCredentials
}

// EndFileTransferRESTParams holds parameters used in the REST API call to notify
// the end of files trasnfer
type EndFileTransferRESTParams struct {
	SubmittedJobInfoID int64 `json:"SubmittedJobInfoId"`
	UsedTransferMethod FileTransferMethod
	SessionCode        string
}

// DownloadFileRESTParams holds HEAppE parameters for the REST API
// allowing to download a file
type DownloadFileRESTParams struct {
	SubmittedJobInfoID int64 `json:"SubmittedJobInfoId"`
	RelativeFilePath   string
	SessionCode        string
}

// ChangedFile holds properties of a file created/updated by a HEAppeJob
type ChangedFile struct {
	FileName         string
	LastModifiedDate string
}

// ListAdaptorUserGroupsRESTParams holds parameters used in the REST API call to
// get details on users
type ListAdaptorUserGroupsRESTParams struct {
	SessionCode string
}

// AdaptorUser hold user name and id properties
type AdaptorUser struct {
	ID       int64 `json:"Id"`
	Username string
}

// AdaptorUserGroup holds user properties
type AdaptorUserGroup struct {
	ID               int64 `json:"Id"`
	Name             string
	Description      string
	AccountingString string
	Users            []AdaptorUser
}

// SubmittedJobInfoUsageReport holds the description of resources used for a job
type SubmittedJobInfoUsageReport struct {
	ID                  int64 `json:"Id"`
	Name                string
	State               int
	Project             string
	CommandTemplateID   int64 `json:"CommandTemplateId"`
	CreationTime        string
	SubmitTime          string
	StartTime           string
	EndTime             string
	TotalAllocatedTime  float64
	TotalCorehoursUsage float64
}

// NodeTypeAggregatedUsage hold usage for a cluster node type
type NodeTypeAggregatedUsage struct {
	ClusterNodeType     ClusterNodeType
	SubmittedJobs       []SubmittedJobInfoUsageReport
	TotalCorehoursUsage float64
}

// UserResourceUsageRESTParams holds parameters used in the REST API call to
// get resources usage report for a user
type UserResourceUsageRESTParams struct {
	UserID      int64 `json:"UserId"`
	StartTime   string
	EndTime     string
	SessionCode string
}

// UserResourceUsageReport holds a report of resources by a user for a given time frame
type UserResourceUsageReport struct {
	User                AdaptorUser
	NodeTypeReports     []NodeTypeAggregatedUsage
	StartTime           string
	EndTime             string
	TotalCorehoursUsage float64
}

// ClusterInfo holds info on clusters managed by HEAppE
type ClusterInfo struct {
	ID          int64 `json:"Id"`
	Name        string
	Description string
	NodeTypes   []ClusterNodeType
}

// ClusterNodeUsage holds usage details for a given node of a cluster
type ClusterNodeUsage struct {
	NodeType         ClusterNodeType
	NodesUsed        int
	CoresUsedPerNode []int
}

// ClusterNodeUsageRESTParams holds parameters used in the REST API call to
// get the current usage of a given cluster node
type ClusterNodeUsageRESTParams struct {
	ClusterNodeID int64 `json:"ClusterNodeId"`
	SessionCode   string
}

// UnmarshalJSON is used to read a file transfer protocol from a string
func (p *FileTransferProtocol) UnmarshalJSON(b []byte) error {
	/*
		var s string
		err := json.Unmarshal(b, &s)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal file transfer protocol as string")
		}
	*/
	val, err := strconv.ParseInt(string(b), 10, 0)
	if err == nil {
		*p = FileTransferProtocol(val)
	} else {
		var val int
		_, err = fmt.Sscanf(string(b), "\"%d\"", &val)
		*p = FileTransferProtocol(val)
	}
	return errors.Wrap(err, "failed to parse file transfer protocol from JSON input")
}
