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
	NetworkShare FileTransferProtocol = iota
	// SftpScp file transfer protocol
	SftpScp
)

// PasswordCredentials holds user/password to perform a basic authentication
type PasswordCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Authentication parameters
type Authentication struct {
	Credentials PasswordCredentials `json:"credentials"`
}

// CommandTemplateParameterValue holds a command template parameter
type CommandTemplateParameterValue struct {
	CommandParameterIdentifier string `json:"commandParameterIdentifier"`
	ParameterValue             string `json:"parameterValue"`
}

// EnvironmentVariable holds an environment variable definition
type EnvironmentVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// TaskSpecification holds task properties
type TaskSpecification struct {
	Name                    string                          `json:"name"`
	CommandTemplateID       int                             `json:"commandTemplateId"`
	TemplateParameterValues []CommandTemplateParameterValue `json:"templateParameterValues,omitempty"`
	MinCores                int                             `json:"minCores"`
	MaxCores                int                             `json:"maxCores"`
	WalltimeLimit           int                             `json:"walltimeLimit"`
	StandardOutputFile      string                          `json:"standardOutputFile"`
	StandardErrorFile       string                          `json:"standardErrorFile"`
	ProgressFile            string                          `json:"progressFile"`
	LogFile                 string                          `json:"logFile"`
	EnvironmentVariables    []EnvironmentVariable           `json:"environmentVariables,omitempty"`
}

// JobSpecification holds job properties
type JobSpecification struct {
	Name              string              `json:"name"`
	Project           string              `json:"project"`
	ClusterNodeTypeID int                 `json:"clusterNodeTypeId"`
	Tasks             []TaskSpecification `json:"tasks"`
	Priority          int                 `json:"priority"`
	MinCores          int                 `json:"minCores"`
	MaxCores          int                 `json:"maxCores"`
	WaitingLimit      int                 `json:"waitingLimit"`
	WalltimeLimit     int                 `json:"walltimeLimit"`
}

// JobCreateRESTParams holds HEAppE REST API job creation parameters
type JobCreateRESTParams struct {
	JobSpecification JobSpecification `json:"jobSpecification"`
	SessionCode      string           `json:"sessionCode"`
}

// JobSubmitRESTParams holds HEAppE REST API job submission parameters
type JobSubmitRESTParams struct {
	CreatedJobInfoID int64  `json:"createdJobInfoId"`
	SessionCode      string `json:"sessionCode"`
}

// JobInfoRESTParams holds HEAppE REST API job info parameters
type JobInfoRESTParams struct {
	SubmittedJobInfoID int64  `json:"submittedJobInfoId"`
	SessionCode        string `json:"sessionCode"`
}

// TemplateParameter holds template parameters description in a job
type TemplateParameter struct {
	Identifier  string `json:"identifier"`
	Description string `json:"description"`
}

// CommandTemplate holds a command template description in a job
type CommandTemplate struct {
	ID                 int64               `json:"id"`
	Name               string              `json:"name"`
	Description        string              `json:"description"`
	Code               string              `json:"code"`
	TemplateParameters []TemplateParameter `json:"templateParameters"`
}

// ClusterNodeType holds a node description in a job
type ClusterNodeType struct {
	ID               int64             `json:"id"`
	Name             string            `json:"name"`
	Description      string            `json:"description"`
	NumberOfNodes    int               `json:"numberOfNodes"`
	CoresPerNode     int               `json:"coresPerNode"`
	MaxWalltime      int               `json:"maxWalltime"`
	CommandTemplates []CommandTemplate `json:"commandTemplates"`
}

// SubmittedTaskInfo holds a task description in a job
type SubmittedTaskInfo struct {
	ID               int64   `json:"id"`
	Name             string  `json:"name"`
	State            int     `json:"state"`
	AllocatedTime    float64 `json:"allocatedTime,omitempty"`
	AllocatedCoreIds string  `json:"allocatedCoreIds,omitempty"`
	StartTime        string  `json:"startTime,omitempty"`
	EndTime          string  `json:"endTime,omitempty"`
	ErrorMessage     string  `json:"errorMessage,omitempty"`
	AllParameters    string  `json:"allParameters,omitempty"`
}

// SubmittedJobInfo holds the response to a job creation/submission
type SubmittedJobInfo struct {
	ID                 int64               `json:"id"`
	Name               string              `json:"name"`
	State              int                 `json:"state"`
	Priority           int                 `json:"priority"`
	Project            string              `json:"project"`
	CreationTime       string              `json:"creationTime"`
	SubmitTime         string              `json:"submitTime,omitempty"`
	StartTime          string              `json:"startTime,omitempty"`
	EndTime            string              `json:"endTime,omitempty"`
	TotalAllocatedTime float64             `json:"totalAllocatedTime,omitempty"`
	AllParameters      string              `json:"allParameters,omitempty"`
	NodeType           ClusterNodeType     `json:"nodeType"`
	Tasks              []SubmittedTaskInfo `json:"tasks"`
}

// TaskFileOffset holds the offset to a file of a given task
type TaskFileOffset struct {
	SubmittedTaskInfoID int64 `json:"submittedTaskInfoId"`
	FileType            int   `json:"fileType"`
	Offset              int64 `json:"offset"`
}

// DownloadPartsOfJobFilesRESTParams holds HEAppE parameters for the REST API
// allowing to download parts of files
type DownloadPartsOfJobFilesRESTParams struct {
	SubmittedJobInfoID int64            `json:"submittedJobInfoId"`
	TaskFileOffsets    []TaskFileOffset `json:"taskFileOffsets"`
	SessionCode        string           `json:"sessionCode"`
}

// JobFileContent holds the response to a partial download of job files
type JobFileContent struct {
	Content             string `json:"content"`
	RelativePath        string `json:"relativePath"`
	Offset              int64  `json:"offset"`
	FileType            int    `json:"fileType"`
	SubmittedTaskInfoID int64  `json:"submittedTaskInfoId"`
}

// AsymmetricKeyCredentials hold credentials used to transfer files to the HPC cluster
type AsymmetricKeyCredentials struct {
	Username   string `json:"username"`
	PrivateKey string `json:"privateKey"`
	PublicKey  string `json:"publicKey"`
}

// FileTransferMethod holds properties allowing to transfer files to the HPC cluster
type FileTransferMethod struct {
	ServerHostname string                   `json:"serverHostname"`
	SharedBasepath string                   `json:"sharedBasepath"`
	Protocol       FileTransferProtocol     `json:"protocol"`
	Credentials    AsymmetricKeyCredentials `json:"credentials"`
}

// EndFileTransferRESTParams holds parameters used in the REST API call to notify
// the end of files trasnfer
type EndFileTransferRESTParams struct {
	SubmittedJobInfoID int64              `json:"submittedJobInfoId"`
	UsedTransferMethod FileTransferMethod `json:"usedTransferMethod"`
	SessionCode        string             `json:"sessionCode"`
}

// DownloadFileRESTParams holds HEAppE parameters for the REST API
// allowing to download a file
type DownloadFileRESTParams struct {
	SubmittedJobInfoID int64  `json:"submittedJobInfoId"`
	RelativeFilePath   string `json:"relativeFilePath"`
	SessionCode        string `json:"sessionCode"`
}

// ListAdaptorUserGroupsRESTParams holds parameters used in the REST API call to
// get details on users
type ListAdaptorUserGroupsRESTParams struct {
	SessionCode string `json:"sessionCode"`
}

// AdaptorUser hold user name and id properties
type AdaptorUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

// AdaptorUserGroup holds user properties
type AdaptorUserGroup struct {
	ID               int64         `json:"id"`
	Name             string        `json:"name"`
	Description      string        `json:"description"`
	AccountingString string        `json:"accountingString"`
	Users            []AdaptorUser `json:"users"`
}

// SubmittedJobInfoUsageReport holds the description of resources used for a job
type SubmittedJobInfoUsageReport struct {
	ID                  int64   `json:"id"`
	Name                string  `json:"name"`
	State               int     `json:"state"`
	Priority            int     `json:"priority"`
	Project             string  `json:"project"`
	CommandTemplateID   int64   `json:"commandTemplateId"`
	CreationTime        string  `json:"creationTime"`
	SubmitTime          string  `json:"submitTime"`
	StartTime           string  `json:"startTime"`
	EndTime             string  `json:"endTime"`
	TotalAllocatedTime  float64 `json:"totalAllocatedTime,omitempty"`
	AllParameters       string  `json:"allParameters"`
	TotalCorehoursUsage float64 `json:"totalCorehoursUsage,omitempty"`
}

// NodeTypeAggregatedUsage hold usage for a cluster node type
type NodeTypeAggregatedUsage struct {
	ClusterNodeType     ClusterNodeType               `json:"clusterNodeType"`
	SubmittedJobs       []SubmittedJobInfoUsageReport `json:"submittedJobs"`
	TotalCorehoursUsage float64                       `json:"totalCorehoursUsage,omitempty"`
}

// UserAggregatedUsage holds resources usage for a user
type UserAggregatedUsage struct {
	User                AdaptorUser               `json:"user"`
	NodeTypeReports     []NodeTypeAggregatedUsage `json:"nodeTypeReports"`
	TotalCorehoursUsage float64                   `json:"totalCorehoursUsage,omitempty"`
}

// UserResourceUsageRESTParams holds parameters used in the REST API call to
// get resources usage report for a user
type UserResourceUsageRESTParams struct {
	UserID      int64  `json:"userID"`
	StartTime   string `json:"startTime"`
	EndTime     string `json:"endTime"`
	SessionCode string `json:"sessionCode"`
}

// UserResourceUsageReport holds a report of resources by a user for a given time frame
type UserResourceUsageReport struct {
	User                AdaptorUser               `json:"user"`
	NodeTypeReports     []NodeTypeAggregatedUsage `json:"nodeTypeReports"`
	StartTime           string                    `json:"startTime"`
	EndTime             string                    `json:"endTime"`
	TotalCorehoursUsage float64                   `json:"totalCorehoursUsage,omitempty"`
}

// ClusterInfo holds info on clusters managed by HEAppE
type ClusterInfo struct {
	ID          int64             `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	NodeTypes   []ClusterNodeType `json:"nodeTypes"`
}

// ClusterNodeUsage holds usage details for a given node of a cluster
type ClusterNodeUsage struct {
	NodeType         ClusterNodeType `json:"nodeType"`
	NodesUsed        int             `json:"nodesUsed"`
	CoresUsedPerNode []int           `json:"coresUsedPerNode"`
}

// ClusterNodeUsageRESTParams holds parameters used in the REST API call to
// get the current usage of a given cluster node
type ClusterNodeUsageRESTParams struct {
	ClusterNodeID int64  `json:"clusterNodeId"`
	SessionCode   string `json:"sessionCode"`
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
