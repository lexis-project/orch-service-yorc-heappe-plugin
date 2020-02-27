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
	"net/http"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/log"
)

const (
	heappeAuthREST                  = "/heappe/UserAndLimitationManagement/AuthenticateUserPassword"
	heappeCreateJobREST             = "/heappe/JobManagement/CreateJob"
	heappeSubmitJobREST             = "/heappe/JobManagement/SubmitJob"
	heappeCancelJobREST             = "/heappe/JobManagement/CancelJob"
	heappeDeleteJobREST             = "/heappe/JobManagement/DeleteJob"
	heappeJobInfoREST               = "/heappe/JobManagement/GetCurrentInfoForJob"
	heappeDownloadPartsREST         = "/heappe/FileTransfer/DownloadPartsOfJobFilesFromCluster"
	heappeGetFileTransferMethodREST = "/heappe/FileTransfer/GetFileTransferMethod"
	heappeEndFileTransferREST       = "/heappe/FileTransfer/EndFileTransfer"
	heappeListChangedFilesREST      = "/heappe/FileTransfer/ListChangedFilesForJob"
	heappeUserUsageReportREST       = "/heappe/JobReporting/GetUserResourceUsageReport"
	heappeListAdaptorUserGroupsREST = "/heappe/JobReporting/ListAdaptorUserGroups"
	heappeListAvailableClustersREST = "/heappe/ClusterInformation/ListAvailableClusters"
	heappeNodeUsageREST             = "/heappe/ClusterInformation/CurrentClusterNodeUsage"
	locationURLPropertyName         = "url"
	// LocationUserPropertyName hold the name of user used to connect to HEAppE
	LocationUserPropertyName     = "user"
	locationPasswordPropertyName = "password"
)

// Client is the client interface to HEAppE service
type Client interface {
	CreateJob(job JobSpecification) (int64, error)
	SubmitJob(jobID int64) error
	CancelJob(jobID int64) error
	DeleteJob(jobID int64) error
	GetJobInfo(jobID int64) (SubmittedJobInfo, error)
	SetSessionID(sessionID string)
	GetSessionID() string
	DownloadPartsOfJobFilesFromCluster(JobID int64, offsets []TaskFileOffset) ([]JobFileContent, error)
	GetFileTransferMethod(jobID int64) (FileTransferMethod, error)
	EndFileTransfer(jobID int64, ft FileTransferMethod) error
	ListChangedFilesForJob(jobID int64) ([]string, error)
	ListAdaptorUserGroups() ([]AdaptorUserGroup, error)
	GetUserResourceUsageReport(userID int64, startTime, endTime string) (*UserResourceUsageReport, error)
	ListAvailableClusters() ([]ClusterInfo, error)
	GetCurrentClusterNodeUsage(nodeID int64) (ClusterNodeUsage, error)
}

// GetClient returns a HEAppE client for a given location
func GetClient(locationProps config.DynamicMap) (Client, error) {

	url := locationProps.GetString(locationURLPropertyName)
	if url == "" {
		return nil, errors.Errorf("No URL defined in HEAppE location configuration")
	}
	username := locationProps.GetString(LocationUserPropertyName)
	if username == "" {
		return nil, errors.Errorf("No user defined in location")
	}
	password := locationProps.GetString(locationPasswordPropertyName)

	return getBasicAuthClient(url, username, password), nil
}

// getBasicAuthClient returns a client performing a basic user/pasword authentication
func getBasicAuthClient(url, username, password string) Client {
	return &heappeClient{
		auth: Authentication{
			Credentials: PasswordCredentials{
				Username: username,
				Password: password,
			},
		},
		httpClient: getHTTPClient(url),
	}
}

type heappeClient struct {
	auth       Authentication
	sessionID  string
	httpClient *httpclient
}

// SetSessionID sets a HEAppE session ID
func (h *heappeClient) SetSessionID(sessionID string) {
	h.sessionID = sessionID
}

// GetSessionID sets a HEAppE session ID
func (h *heappeClient) GetSessionID() string {
	return h.sessionID
}

// CreateJob creates a HEAppE job
func (h *heappeClient) CreateJob(job JobSpecification) (int64, error) {

	// First authenticate
	var jobID int64
	var err error
	h.sessionID, err = h.authenticate()
	if err != nil {
		return jobID, err
	}

	params := JobCreateRESTParams{
		JobSpecification: job,
		SessionCode:      h.sessionID,
	}
	var jobResponse SubmittedJobInfo

	err = h.httpClient.doRequest(http.MethodPost, heappeCreateJobREST, http.StatusOK, params, &jobResponse)
	jobID = jobResponse.ID
	if err != nil {
		err = errors.Wrap(err, "Failed to create job")
	}

	return jobID, err
}

// SubmitJob submits a HEAppE job
func (h *heappeClient) SubmitJob(jobID int64) error {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return err
		}
	}

	params := JobSubmitRESTParams{
		CreatedJobInfoID: jobID,
		SessionCode:      h.sessionID,
	}

	var jobResponse SubmittedJobInfo

	err := h.httpClient.doRequest(http.MethodPost, heappeSubmitJobREST, http.StatusOK, params, &jobResponse)
	if err != nil {
		err = errors.Wrap(err, "Failed to submit job")
	}

	return err
}

// CancelJob cancels a HEAppE job
func (h *heappeClient) CancelJob(jobID int64) error {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return err
		}
	}

	params := JobInfoRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
	}

	var jobResponse SubmittedJobInfo

	err := h.httpClient.doRequest(http.MethodPost, heappeCancelJobREST, http.StatusOK, params, &jobResponse)
	if err != nil {
		err = errors.Wrap(err, "Failed to cancel job")
	}

	return err
}

// DeleteJob delete a HEAppE job
func (h *heappeClient) DeleteJob(jobID int64) error {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return err
		}
	}

	params := JobInfoRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
	}

	var response string

	err := h.httpClient.doRequest(http.MethodPost, heappeDeleteJobREST, http.StatusOK, params, &response)
	if err != nil {
		err = errors.Wrap(err, "Failed to delete job")
	}

	return err
}

// GetJobInfo gets a HEAppE job info
func (h *heappeClient) GetJobInfo(jobID int64) (SubmittedJobInfo, error) {

	var jobInfo SubmittedJobInfo

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return jobInfo, err
		}
	}

	params := JobInfoRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
	}

	err := h.httpClient.doRequest(http.MethodPost, heappeJobInfoREST, http.StatusOK, params, &jobInfo)
	if err != nil {
		err = errors.Wrap(err, "Failed to get job state")
	}

	return jobInfo, err
}

func (h *heappeClient) DownloadPartsOfJobFilesFromCluster(jobID int64, offsets []TaskFileOffset) ([]JobFileContent, error) {

	contents := make([]JobFileContent, 0)

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return contents, err
		}
	}

	params := DownloadPartsOfJobFilesRESTParams{
		SubmittedJobInfoID: jobID,
		TaskFileOffsets:    offsets,
		SessionCode:        h.sessionID,
	}

	err := h.httpClient.doRequest(http.MethodPost, heappeDownloadPartsREST, http.StatusOK, params, &contents)
	if err != nil {
		err = errors.Wrap(err, "Failed to download part of job outputs")
	}

	return contents, err
}

func (h *heappeClient) GetFileTransferMethod(jobID int64) (FileTransferMethod, error) {

	var transferMethod FileTransferMethod
	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return transferMethod, err
		}
	}

	params := JobInfoRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
	}

	err := h.httpClient.doRequest(http.MethodPost, heappeGetFileTransferMethodREST, http.StatusOK, params, &transferMethod)
	if err != nil {
		err = errors.Wrap(err, "Failed to download part of job outputs")
	}

	return transferMethod, err

}

func (h *heappeClient) EndFileTransfer(jobID int64, ft FileTransferMethod) error {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return err
		}
	}

	params := EndFileTransferRESTParams{
		SubmittedJobInfoID: jobID,
		UsedTransferMethod: ft,
		SessionCode:        h.sessionID,
	}

	var result string
	err := h.httpClient.doRequest(http.MethodPost, heappeEndFileTransferREST, http.StatusOK, params, &result)
	if err != nil {
		err = errors.Wrap(err, "Failed to download part of job outputs")
	}

	return err

}

func (h *heappeClient) ListChangedFilesForJob(jobID int64) ([]string, error) {

	filenames := make([]string, 0)
	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return filenames, err
		}
	}

	params := JobInfoRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
	}

	err := h.httpClient.doRequest(http.MethodPost, heappeListChangedFilesREST, http.StatusOK, params, &filenames)
	if err != nil {
		err = errors.Wrap(err, "Failed to download part of job outputs")
	}

	return filenames, err

}

func (h *heappeClient) ListAdaptorUserGroups() ([]AdaptorUserGroup, error) {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return nil, err
		}
	}

	params := ListAdaptorUserGroupsRESTParams{
		SessionCode: h.sessionID,
	}

	result := make([]AdaptorUserGroup, 0)
	err := h.httpClient.doRequest(http.MethodPost, heappeListAdaptorUserGroupsREST, http.StatusOK, params, &result)
	if err != nil {
		log.Printf("Error calling HEAppE API %s: %s", heappeListAdaptorUserGroupsREST, err.Error())
		err = errors.Wrap(err, "Failed to get list of users")
	}

	return result, err
}

func (h *heappeClient) GetUserResourceUsageReport(userID int64, startTime, endTime string) (*UserResourceUsageReport, error) {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return nil, err
		}
	}

	params := UserResourceUsageRESTParams{
		UserID:      userID,
		StartTime:   startTime,
		EndTime:     endTime,
		SessionCode: h.sessionID,
	}

	var report UserResourceUsageReport
	err := h.httpClient.doRequest(http.MethodPost, heappeUserUsageReportREST, http.StatusOK, params, &report)
	if err != nil {
		log.Printf("Error calling HEAppE API %s: %s", heappeUserUsageReportREST, err.Error())
		err = errors.Wrap(err, "Failed to get resources usage report")
	}

	return &report, err
}

func (h *heappeClient) ListAvailableClusters() ([]ClusterInfo, error) {

	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return nil, err
		}
	}

	result := make([]ClusterInfo, 0)
	err := h.httpClient.doRequest(http.MethodGet, heappeListAvailableClustersREST, http.StatusOK, "", &result)
	if err != nil {
		log.Printf("Error calling HEAppE API %s: %s", heappeListAvailableClustersREST, err.Error())
		err = errors.Wrap(err, "Failed to get list of available clusters")
	}

	return result, err
}

func (h *heappeClient) GetCurrentClusterNodeUsage(nodeID int64) (ClusterNodeUsage, error) {

	var result ClusterNodeUsage
	var err error
	if h.sessionID == "" {
		h.sessionID, err = h.authenticate()
		if err != nil {
			return result, err
		}
	}

	params := ClusterNodeUsageRESTParams{
		ClusterNodeID: nodeID,
		SessionCode:   h.sessionID,
	}

	err = h.httpClient.doRequest(http.MethodPost, heappeNodeUsageREST, http.StatusOK, params, &result)
	if err != nil {
		log.Printf("Error calling HEAppE API %s: %s", heappeNodeUsageREST, err.Error())
		err = errors.Wrap(err, "Failed to get list of users")
	}

	return result, err
}

func (h *heappeClient) authenticate() (string, error) {
	var sessionID string

	err := h.httpClient.doRequest(http.MethodPost, heappeAuthREST, http.StatusOK, h.auth, &sessionID)
	if err != nil {
		return sessionID, errors.Wrap(err, "Failed to authenticate to HEAppE")
	}
	if len(sessionID) == 0 {
		err = errors.Errorf("Failed to open a HEAppE session")
	}

	return sessionID, err
}
