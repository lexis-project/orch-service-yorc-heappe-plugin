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
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/log"
)

const (
	heappeAuthREST                  = "/heappe/UserAndLimitationManagement/AuthenticateUserPassword"
	heappeAuthOpenIDREST            = "/heappe/UserAndLimitationManagement/AuthenticateUserOpenId"
	heappeCreateJobREST             = "/heappe/JobManagement/CreateJob"
	heappeSubmitJobREST             = "/heappe/JobManagement/SubmitJob"
	heappeCancelJobREST             = "/heappe/JobManagement/CancelJob"
	heappeDeleteJobREST             = "/heappe/JobManagement/DeleteJob"
	heappeJobInfoREST               = "/heappe/JobManagement/GetCurrentInfoForJob"
	heappeDownloadPartsREST         = "/heappe/FileTransfer/DownloadPartsOfJobFilesFromCluster"
	heappeGetFileTransferMethodREST = "/heappe/FileTransfer/GetFileTransferMethod"
	heappeEndFileTransferREST       = "/heappe/FileTransfer/EndFileTransfer"
	heappeDownloadFileREST          = "/heappe/FileTransfer/DownloadFileFromCluster"
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
	CreateJob(job JobSpecification) (JobInfo, error)
	SubmitJob(jobID int64) error
	CancelJob(jobID int64) error
	DeleteJob(jobID int64) error
	GetJobInfo(jobID int64) (JobInfo, error)
	SetSessionID(sessionID string)
	GetSessionID() string
	GetURL() string
	DownloadPartsOfJobFilesFromCluster(JobID int64, offsets []TaskFileOffset) ([]JobFileContent, error)
	GetFileTransferMethod(jobID int64) (FileTransferMethod, error)
	EndFileTransfer(jobID int64, ft FileTransferMethod) error
	DownloadFileFromCluster(jobID int64, filePath string) (string, error)
	ListChangedFilesForJob(jobID int64) ([]ChangedFile, error)
	ListAdaptorUserGroups() ([]AdaptorUserGroup, error)
	GetUserResourceUsageReport(userID int64, startTime, endTime string) (*UserResourceUsageReport, error)
	ListAvailableClusters() ([]ClusterInfo, error)
	GetCurrentClusterNodeUsage(nodeID int64) (ClusterNodeUsage, error)
}

// GetClient returns a HEAppE client for a given location
func GetClient(locationProps config.DynamicMap, accessToken, refreshToken string) (Client, error) {

	url := locationProps.GetString(locationURLPropertyName)
	if url == "" {
		return nil, errors.Errorf("No URL defined in HEAppE location configuration")
	}

	if accessToken != "" {
		return getOpenIDAuthClient(url, accessToken, refreshToken), nil
	}

	username := locationProps.GetString(LocationUserPropertyName)
	if username == "" {
		return nil, errors.Errorf("No user defined in location")
	}
	password := locationProps.GetString(locationPasswordPropertyName)

	return getBasicAuthClient(url, username, password), nil
}

// getOpenIDAuthClient returns a client performing an OpenID connect token-based authentication
func getOpenIDAuthClient(url, accessToken, refreshToken string) Client {
	return &heappeClient{
		openIDAuth: OpenIDAuthentication{
			Credentials: OpenIDCredentials{
				Username:           "YorcUser",
				OpenIdAccessToken:  accessToken,
				OpenIdRefreshToken: refreshToken,
			},
		},
		httpClient: getHTTPClient(url),
	}
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
	openIDAuth OpenIDAuthentication
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

// GetURL returns the URL of the HEAppE instance
func (h *heappeClient) GetURL() string {
	return h.httpClient.baseURL
}

// CreateJob creates a HEAppE job
func (h *heappeClient) CreateJob(job JobSpecification) (JobInfo, error) {

	var jobResponse JobInfo

	// First authenticate
	var err error
	h.sessionID, err = h.authenticate()
	if err != nil {
		return jobResponse, err
	}

	params := JobCreateRESTParams{
		JobSpecification: job,
		SessionCode:      h.sessionID,
	}

	createStr, _ := json.Marshal(params)
	log.Printf("Creating job %s", string(createStr))

	err = h.httpClient.doRequest(http.MethodPost, heappeCreateJobREST, http.StatusOK, params, &jobResponse)
	if err != nil {
		err = errors.Wrapf(err, "Failed to create job %+v", params)
	}

	return jobResponse, err
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

	var jobResponse JobInfo

	err := h.httpClient.doRequest(http.MethodPost, heappeSubmitJobREST, http.StatusOK, params, &jobResponse)
	if err != nil {
		err = errors.Wrapf(err, "Failed to submit job %+v", params)
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

	var jobResponse JobInfo

	err := h.httpClient.doRequest(http.MethodPost, heappeCancelJobREST, http.StatusOK, params, &jobResponse)
	if err != nil {
		err = errors.Wrapf(err, "Failed to cancel job %d", jobID)
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
		err = errors.Wrapf(err, "Failed to delete job %d", jobID)
	}

	return err
}

// GetJobInfo gets a HEAppE job info
func (h *heappeClient) GetJobInfo(jobID int64) (JobInfo, error) {

	var jobInfo JobInfo

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
		err = errors.Wrapf(err, "Failed to get state of job %d", jobID)

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
		err = errors.Wrapf(err, "Failed to download part of job outputs for job %d, request %+v", jobID, params)
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
		err = errors.Wrapf(err, "Failed to get file transfer method for job %d", jobID)
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
		err = errors.Wrapf(err, "Failed to end file transfer for job %d", jobID)
	}

	return err

}

func (h *heappeClient) DownloadFileFromCluster(jobID int64, filePath string) (string, error) {

	var fContent string
	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return fContent, err
		}
	}

	params := DownloadFileRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
		RelativeFilePath:   filePath,
	}

	err := h.httpClient.doRequest(http.MethodPost, heappeDownloadFileREST, http.StatusOK, params, &fContent)
	if err != nil {
		err = errors.Wrapf(err, "Failed to download file %s content for job %d", filePath, jobID)
	}

	return fContent, err

}

func (h *heappeClient) ListChangedFilesForJob(jobID int64) ([]ChangedFile, error) {

	changedFiles := make([]ChangedFile, 0)
	if h.sessionID == "" {
		var err error
		h.sessionID, err = h.authenticate()
		if err != nil {
			return changedFiles, err
		}
	}

	params := JobInfoRESTParams{
		SubmittedJobInfoID: jobID,
		SessionCode:        h.sessionID,
	}

	err := h.httpClient.doRequest(http.MethodPost, heappeListChangedFilesREST, http.StatusOK, params, &changedFiles)
	if err != nil {
		err = errors.Wrapf(err, "Failed to list changed files for job %d", jobID)
	}

	return changedFiles, err

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
		err = errors.Wrapf(err, "Failed to get resources usage report for user %d", userID)

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
		err = errors.Wrapf(err, "Failed to get list of users on node %d", nodeID)
	}

	return result, err
}

func (h *heappeClient) authenticate() (string, error) {
	var sessionID string
	var err error

	if h.openIDAuth.Credentials.OpenIdAccessToken != "" {
		err = h.httpClient.doRequest(http.MethodPost, heappeAuthOpenIDREST, http.StatusOK, h.openIDAuth, &sessionID)
	} else {
		err = h.httpClient.doRequest(http.MethodPost, heappeAuthREST, http.StatusOK, h.auth, &sessionID)
	}
	if err != nil {
		return sessionID, errors.Wrap(err, "Failed to authenticate to HEAppE")
	}
	if len(sessionID) == 0 {
		err = errors.Errorf("Failed to open a HEAppE session")
	}

	return sessionID, err
}
