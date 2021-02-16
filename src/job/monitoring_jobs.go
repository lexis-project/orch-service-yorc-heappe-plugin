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

package job

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/lexis-project/yorc-heappe-plugin/v1/heappe"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/scheduling"
)

const (
	actionDataSessionID        = "sessionID"
	jobStatePending            = "PENDING"
	jobStateRunning            = "RUNNING"
	jobStateCompleted          = "COMPLETED"
	jobStateFailed             = "FAILED"
	jobStateCanceled           = "CANCELED"
	actionDataOffsetKeyFormat  = "%d_%d_%d"
	fileContentConsulAttribute = "filecontent"
)

type fileType int

const (
	logFile fileType = iota
	progressFile
	standardErrorFile
	standardOutputFile
)

var fileTypes = []fileType{logFile, progressFile, standardErrorFile, standardOutputFile}

// ActionOperator holds function allowing to execute an action
type ActionOperator struct {
}

type actionData struct {
	jobID     int64
	taskID    string
	nodeName  string
	sessionID string
	filePath  string
}

// ExecAction allows to execute and action
func (o *ActionOperator) ExecAction(ctx context.Context, cfg config.Configuration, taskID, deploymentID string, action *prov.Action) (bool, error) {
	log.Debugf("Execute Action with ID:%q, taskID:%q, deploymentID:%q", action.ID, taskID, deploymentID)

	if action.ActionType == "heappe-job-monitoring" {
		deregister, err := o.monitorJob(ctx, cfg, deploymentID, action)
		if err != nil {
			// action scheduling needs to be unregistered
			return true, err
		}

		return deregister, nil
	}

	if action.ActionType == "heappe-filecontent-monitoring" {
		deregister, err := o.getFileContent(ctx, cfg, deploymentID, action)
		if err != nil {
			// action scheduling needs to be unregistered
			return true, err
		}

		return deregister, nil
	}

	return true, errors.Errorf("Unsupported actionType %q", action.ActionType)
}

func (o *ActionOperator) monitorJob(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var (
		err        error
		deregister bool
		ok         bool
	)

	actionData := &actionData{}
	// Check nodeName
	actionData.nodeName, ok = action.Data["nodeName"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information nodeName for actionType:%q", action.ActionType)
	}
	// Check jobID
	jobIDstr, ok := action.Data["jobID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information jobID for actionType:%q", action.ActionType)
	}
	actionData.jobID, err = strconv.ParseInt(jobIDstr, 10, 64)
	if err != nil {
		return true, errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s", jobIDstr, deploymentID, actionData.nodeName)
	}

	// Check taskID
	actionData.taskID, ok = action.Data["taskID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information taskID for actionType:%q", action.ActionType)
	}

	listChangedFilesWhileRunning := false
	boolStr, ok := action.Data[listChangedFilesAction]
	if ok {
		listChangedFilesWhileRunning, _ = strconv.ParseBool(boolStr)
	}
	heappeClient, err := getHEAppEClient(ctx, cfg, deploymentID, actionData.nodeName, action.Data["token"])
	if err != nil {
		return true, err
	}

	// Set session ID if defined, else a new session will be created
	actionData.sessionID, ok = action.Data[actionDataSessionID]
	if ok && actionData.sessionID != "" {
		heappeClient.SetSessionID(actionData.sessionID)
	}

	jobInfo, err := heappeClient.GetJobInfo(actionData.jobID)
	if err != nil {
		return true, err
	}

	if actionData.sessionID == "" {
		// Storing the session ID for next job state check
		err = scheduling.UpdateActionData(nil, action.ID, actionDataSessionID, heappeClient.GetSessionID())
		if err != nil {
			return true, errors.Wrapf(err, "failed to update action data for deployment %s node %s", deploymentID, actionData.nodeName)
		}
	}

	jobState := getJobState(jobInfo)
	previousJobState, err := deployments.GetInstanceStateString(ctx, deploymentID, actionData.nodeName, "0")
	if err != nil {
		return true, errors.Wrapf(err, "failed to get instance state for job %d", actionData.jobID)
	}

	// See if monitoring must be continued and set job state if terminated
	switch jobState {
	case jobStateCompleted:
		// job has been done successfully : unregister monitoring
		deregister = true
	case jobStatePending:
		// Not yet runninh: monitoring is keeping on (deregister stays false)
	case jobStateRunning:
		// job is still running : monitoring is keeping on (deregister stays false)
		if listChangedFilesWhileRunning {
			updateErr := updateListOfChangedFiles(ctx, heappeClient, deploymentID, actionData.nodeName, actionData.jobID)
			if err != nil {
				log.Printf("Failed to update list of files changed by Job %d : %s", actionData.jobID, updateErr.Error())
			}
		}
	default:
		// Other cases as FAILED, CANCELED : error is return with job state and job info is logged
		deregister = true
		// Log event containing all the slurm information

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(fmt.Sprintf("job state:%+v", jobState))
		// Error to be returned
		exitStatus := getJobExitStatus(jobInfo)
		if exitStatus == "" {
			err = errors.Errorf("job %q with ID: %d finished unsuccessfully with state: %q", jobInfo.Name, actionData.jobID, jobState)
		} else {
			err = errors.Errorf("job %q with ID: %d finished unsuccessfully with state: %q, exit status: %s", jobInfo.Name, actionData.jobID, jobState, exitStatus)
		}
	}

	// If the job state is a final state, print job logs before printing the state change
	if deregister {
		// Updtae list of files changed by the job
		err := updateListOfChangedFiles(ctx, heappeClient, deploymentID, actionData.nodeName, actionData.jobID)
		if err != nil {
			log.Printf("Failed to update list of files changed by Job %d : %s", actionData.jobID, err.Error())
		}
		// Log job outputs
		logErr := o.getJobOutputs(ctx, heappeClient, deploymentID, actionData.nodeName, action, jobInfo)
		if logErr != nil {
			log.Printf("Failed to get job outputs : %s", logErr.Error())
		}
		// Print state change
		if previousJobState != jobState {
			err := deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobState)
			if err != nil {
				log.Printf("Failed to set Job %d state %s: %s", actionData.jobID, jobState, err.Error())
			}
		}

	} else {
		// Print state change
		if previousJobState != jobState {
			err := deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobState)
			if err != nil {
				log.Printf("Failed to set instance %s %s state %s: %s", deploymentID, actionData.nodeName, jobState, err.Error())
			}
		}
		// Log job outputs
		if jobState == jobStateRunning {
			logErr := o.getJobOutputs(ctx, heappeClient, deploymentID, actionData.nodeName, action, jobInfo)
			if logErr != nil {
				log.Printf("Failed to get job outputs : %s", logErr.Error())
			}
		}
	}

	return deregister, err
}

func (o *ActionOperator) getJobOutputs(ctx context.Context, heappeClient heappe.Client,
	deploymentID, nodeName string, action *prov.Action, jobInfo heappe.JobInfo) error {

	var err error
	var offsets []heappe.TaskFileOffset
	for _, task := range jobInfo.Tasks {
		for _, fType := range fileTypes {
			var tOffset heappe.TaskFileOffset
			tOffset.SubmittedTaskInfoID = task.ID
			tOffset.FileType = int(fType)
			tOffset.Offset, err = getOffset(jobInfo.ID, task.ID, tOffset.FileType, action)
			if err != nil {
				return errors.Wrapf(err, "Failed to compute offset for log file on deployment %s node %s job %d ",
					deploymentID, nodeName, jobInfo.ID)
			}

			offsets = append(offsets, tOffset)
		}
	}

	contents, err := heappeClient.DownloadPartsOfJobFilesFromCluster(jobInfo.ID, offsets)
	if err != nil {
		return err
	}

	// Print contents
	for _, fileContent := range contents {
		if strings.TrimSpace(fileContent.Content) != "" {
			fileTypeStr := displayFileType(fileContent.FileType)
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(
				fmt.Sprintf("Job %d task %d %s:", jobInfo.ID, fileContent.SubmittedTaskInfoID, fileTypeStr))
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString("\n" + fileContent.Content)

			// Save the new offset
			newOffset := fileContent.Offset + int64(len(fileContent.Content))
			offsetKey := getActionDataOffsetKey(jobInfo.ID, fileContent.SubmittedTaskInfoID, fileContent.FileType)
			err = scheduling.UpdateActionData(nil, action.ID, offsetKey, strconv.FormatInt(newOffset, 10))
			if err != nil {
				return errors.Wrapf(err, "failed to update action data for deployment %s node %s job %d task %d %s",
					deploymentID, nodeName, jobInfo.ID, fileContent.SubmittedTaskInfoID, fileTypeStr)
			}
		}
	}

	return err
}

func (o *ActionOperator) getFileContent(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var (
		err error
		ok  bool
	)

	actionData := &actionData{}
	// Check nodeName
	actionData.nodeName, ok = action.Data["nodeName"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information nodeName for actionType:%q", action.ActionType)
	}
	// Check jobID
	jobIDstr, ok := action.Data["jobID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information jobID for actionType:%q", action.ActionType)
	}
	actionData.jobID, err = strconv.ParseInt(jobIDstr, 10, 64)
	if err != nil {
		return true, errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s", jobIDstr, deploymentID, actionData.nodeName)
	}

	// Check taskID
	actionData.taskID, ok = action.Data["taskID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information taskID for actionType:%q", action.ActionType)
	}

	// Check filePath
	actionData.filePath, ok = action.Data["filePath"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information filePath for actionType:%q", action.ActionType)
	}

	heappeClient, err := getHEAppEClient(ctx, cfg, deploymentID, actionData.nodeName, action.Data["token"])
	if err != nil {
		return true, err
	}

	// Set session ID if defined, else a new session will be created
	actionData.sessionID, ok = action.Data[actionDataSessionID]
	if ok && actionData.sessionID != "" {
		heappeClient.SetSessionID(actionData.sessionID)
	}

	jobInfo, err := heappeClient.GetJobInfo(actionData.jobID)
	if err != nil {
		return true, err
	}

	if actionData.sessionID == "" {
		// Storing the session ID for next job state check
		err = scheduling.UpdateActionData(nil, action.ID, actionDataSessionID, heappeClient.GetSessionID())
		if err != nil {
			return true, errors.Wrapf(err, "failed to update action data for deployment %s node %s", deploymentID, actionData.nodeName)
		}
	}

	jobState := getJobState(jobInfo)
	if jobState == jobStatePending {
		// Job not yet running, no need to check for files created yet
		return false, err
	}
	changedFiles, err := heappeClient.ListChangedFilesForJob(actionData.jobID)
	if err != nil {
		return true, err
	}

	var foundFile bool
	for _, filePath := range changedFiles {
		if filePath == actionData.filePath {
			foundFile = true
			break
		}
	}

	if !foundFile {
		// Not yet produced by the job, ending this iteration,
		// except if the job status is done, in which case no new file will be
		// produced and we didn't find the expected file, so ending with a failure
		if jobState == jobStateCompleted || jobState == jobStateFailed ||
			jobState == jobStateCanceled {

			err = deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobStateFailed)
			if err != nil {
				log.Printf("Failed to set Job %s state %s: %s", actionData.nodeName, jobState, err.Error())
			} else {
				err = errors.Errorf("Failed to find file %s expected to be produced by job ID %d", actionData.filePath, actionData.jobID)
			}

			return true, err
		} else {
			// Ending this iteration, we'll check again if the file is there next time
			return false, err
		}
	}

	fContent, err := heappeClient.DownloadFileFromCluster(actionData.jobID, actionData.filePath)
	if err != nil {
		return true, err
	}

	// Store the content
	err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
		fileContentConsulAttribute, fContent)
	if err != nil {
		err = errors.Wrapf(err, "Failed to store file content for deployment %s node %s", deploymentID, actionData.nodeName)
		return true, err
	}

	// Work done, update this job state to completed
	err = deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobStateCompleted)

	return true, err
}

func getOffset(jobID, taskID int64, fileType int, action *prov.Action) (int64, error) {

	offsetKey := getActionDataOffsetKey(jobID, taskID, fileType)
	offsetStr := action.Data[offsetKey]
	var err error
	var offset int64
	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
	}
	return offset, err
}

func getActionDataOffsetKey(jobID, taskID int64, fileType int) string {
	return fmt.Sprintf(actionDataOffsetKeyFormat, jobID, taskID, fileType)
}

func getJobState(jobInfo heappe.JobInfo) string {
	var strValue string
	strValue, err := stateToString(jobInfo.State)
	if err != nil {
		log.Printf("Error getting state for job %d, unexpected state %d, considering it failed", jobInfo.ID, jobInfo.State)
	}
	return strValue
}

func stateToString(state int) (string, error) {
	strValue := jobStateFailed
	var err error
	switch state {
	case 0, 1, 2, 4:
		strValue = jobStatePending
	case 8:
		strValue = jobStateRunning
	case 16:
		strValue = jobStateCompleted
	case 32:
		strValue = jobStateFailed
	case 64:
		strValue = jobStateFailed // HEAppE state canceled
	default:
		err = errors.Errorf("Unknown state value %d", state)
	}
	return strValue, err
}

func getJobExitStatus(jobInfo heappe.JobInfo) string {

	var buffer bytes.Buffer
	for _, taskInfo := range jobInfo.Tasks {
		stateStr, _ := stateToString(taskInfo.State)
		if stateStr == jobStateFailed {
			buffer.WriteString(fmt.Sprintf("Task %d %s %s: %s. ", taskInfo.ID, taskInfo.Name, stateStr, taskInfo.ErrorMessage))
		}
	}

	return strings.TrimSpace(buffer.String())
}

func displayFileType(fType int) string {
	var strValue string

	switch fileType(fType) {
	case logFile:
		strValue = "Log file"
	case progressFile:
		strValue = "Progress file"
	case standardErrorFile:
		strValue = "Standard Error"
	case standardOutputFile:
		strValue = "Standard Output"
	default:
		log.Printf("Unknown file type %d, unexpected state %d", fType)
		strValue = "Unknwown file"
	}
	return strValue
}
