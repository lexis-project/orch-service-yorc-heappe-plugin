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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lexis-project/yorc-heappe-plugin/heappe"
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
	jobStateCreated            = "CREATED"
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
		return o.monitorJob(ctx, cfg, deploymentID, action)
	}

	if action.ActionType == "heappe-job-urgent-computing-monitoring" {
		return o.monitorUrgentComputingJob(ctx, cfg, deploymentID, action)
	}

	if action.ActionType == "heappe-filecontent-monitoring" {
		return o.getFileContent(ctx, cfg, deploymentID, action)
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

	if actionData.jobID == 0 {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).Registerf(
			"Job %s is skipped, ending monitoring task with success", actionData.nodeName)
		err := deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobStateCompleted)
		if err != nil {
			log.Printf("Failed to set Job %s state %s: %s", actionData.nodeName, jobStateCompleted, err.Error())
			return false, err
		}
		// ending monitoring on success
		return true, err
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
	heappeClient, err := getHEAppEClient(ctx, cfg, deploymentID, actionData.nodeName, action.Data["user"])
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
		// Be resilient to temporary gateway errors here while a job is running
		nonFatalError := strings.Contains(err.Error(), "502 Bad Gateway") ||
			strings.Contains(err.Error(), "504 Gateway Time-out") ||
			strings.Contains(err.Error(), "i/o timeout") ||
			strings.Contains(err.Error(), "dial tcp")

		if nonFatalError {
			log.Printf("Ignoring non fatal error trying to get job info: %s", err.Error())
		}
		return !nonFatalError, err
	}

	if actionData.sessionID == "" {
		// Storing the session ID for next job state check
		sessionID := heappeClient.GetSessionID()
		log.Debugf("New HEAppE session ID %s\n", sessionID)
		err = scheduling.UpdateActionData(nil, action.ID, actionDataSessionID, sessionID)
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
		// job has been done successfully : update the list of changed files
		nbAttempts := 0
		newAttempt := true
		for newAttempt {
			nbAttempts++
			err = updateListOfChangedFiles(ctx, heappeClient, deploymentID, actionData.nodeName, actionData.jobID)
			newAttempt = (err != nil)
			if err != nil {
				// Be resilient to temporary errors here
				if nbAttempts < 15 {
					log.Printf("Retrying to get list of files for job %d after error %s", actionData.jobID, err.Error())
					time.Sleep(60 * time.Second)
				} else {
					log.Printf("Failed to update list of files changed by Job %d : %s", actionData.jobID, err.Error())
					newAttempt = false
				}
			}
		}
		// job has been done successfully : unregister monitoring
		deregister = true
	case jobStateCreated, jobStatePending:
		// Not yet running: monitoring is keeping on (deregister stays false)
	case jobStateRunning:
		// job is still running : monitoring is keeping on (deregister stays false)
		if listChangedFilesWhileRunning {
			updateErr := updateListOfChangedFiles(ctx, heappeClient, deploymentID, actionData.nodeName, actionData.jobID)
			if updateErr != nil {
				log.Printf("Failed to update list of files changed by Job %d : %s", actionData.jobID, updateErr.Error())
			}
		}
		// Update the start date the first time this job is seen running
		if previousJobState != jobState {
			updateErr := deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
				startDateConsulAttribute, jobInfo.StartTime)
			if updateErr != nil {
				log.Printf("Failed to set job start date %s for Job %s ID %d: %s",
					jobInfo.SubmitTime, actionData.nodeName, actionData.jobID, updateErr.Error())
			}

		}

	default:
		// Other cases as FAILED, CANCELED : error is return with job state and job info is logged
		deregister = true
		// Log event containing all the job information
		isCanceledByUrgentComputing := false
		var val *deployments.TOSCAValue
		val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", cancelFlagConsulAttribute)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to get %s attribute %s: %s", actionData.nodeName, cancelFlagConsulAttribute, err.Error()))
		} else if val != nil {
			isCanceledByUrgentComputing, _ = strconv.ParseBool(val.RawString())
		}

		if isCanceledByUrgentComputing {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(
				fmt.Sprintf("Job %d node %s was canceled for urgent computing, setting its state to %s",
					actionData.jobID, actionData.nodeName, jobStateCompleted))

			err = deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobStateCompleted)
			if err != nil {
				log.Printf("Failed to set Job %d state %s: %s", actionData.jobID, jobState, err.Error())
			}
			return true, err
		}

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(fmt.Sprintf("job state:%+v", jobState))
		// Error to be returned
		exitStatus := getJobExitStatus(jobInfo)
		if exitStatus == "" {
			err = errors.Errorf("job %q with ID: %d finished unsuccessfully with state: %q", jobInfo.Name, actionData.jobID, jobState)
		} else {
			err = errors.Errorf("job %q with ID: %d finished unsuccessfully with state: %q, exit status: %s", jobInfo.Name, actionData.jobID, jobState, exitStatus)
		}
	}

	// Store the map providing the correspondance between task name and task status
	tasksNameStatus := make(map[string]string, len(jobInfo.Tasks))
	for _, taskInfo := range jobInfo.Tasks {
		status, err := stateToString(taskInfo.State)
		if err != nil {
			log.Printf("Job %s ID %d task %s has unexpected state value %d",
				actionData.nodeName, actionData.jobID, taskInfo.Name, taskInfo.State)
			status = jobStateCreated
		}
		tasksNameStatus[taskInfo.Name] = status
	}
	setAttrErr := deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName, tasksNameStatusConsulAttribute,
		tasksNameStatus)
	if setAttrErr != nil {
		err = errors.Wrapf(setAttrErr, "Job %d, failed to store tasks status details", jobInfo.ID)
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

func (o *ActionOperator) monitorUrgentComputingJob(ctx context.Context, cfg config.Configuration, deploymentID string, action *prov.Action) (bool, error) {
	var err error
	var ok bool

	actionData := &actionData{}
	// Check nodeName
	actionData.nodeName, ok = action.Data["nodeName"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information nodeName for actionType:%q", action.ActionType)
	}

	// Check taskID
	actionData.taskID, ok = action.Data["taskID"]
	if !ok {
		return true, errors.Errorf("Missing mandatory information taskID for actionType:%q", action.ActionType)
	}

	cancelRemainingJobs := false
	boolStr, ok := action.Data[actionCancelRemainingJobs]
	if ok {
		cancelRemainingJobs, _ = strconv.ParseBool(boolStr)
	}

	user := action.Data["user"]

	isCanceled := false
	val, err := deployments.GetInstanceAttributeValue(ctx, deploymentID, actionData.nodeName, "0", cancelFlagConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", actionData.nodeName, cancelFlagConsulAttribute, err.Error()))
	} else if val != nil {
		isCanceled, _ = strconv.ParseBool(val.RawString())
	}

	if isCanceled {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(
			fmt.Sprintf("Terminating  %s as job was canceled", actionData.nodeName))
		// Set state
		err = deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobStateFailed)
		if err != nil {
			log.Printf("Failed to set instance %s %s state %s: %s", deploymentID, actionData.nodeName, jobStateFailed, err.Error())
		}
		return true, err
	}
	heappeJobNodeNames := strings.Split(action.Data[actionAssociatedHEAppeJobNodeNames], ",")
	var jobDoneID int64
	var jobDoneNodeName string
	allFinished := true
	cancelJobNodeNameID := make(map[string]int64)
	for _, heappeJobNodeName := range heappeJobNodeNames {
		jobExec := Execution{
			DeploymentID: deploymentID,
			NodeName:     heappeJobNodeName,
		}
		jobID, err := jobExec.getJobID(ctx)
		if err != nil {
			return true, err
		}
		heappeClient, err := getHEAppEClient(ctx, cfg, deploymentID, heappeJobNodeName, user)
		if err != nil {
			return true, err
		}
		jobInfo, err := heappeClient.GetJobInfo(jobID)
		if err != nil {
			// Be resilient to temporary gateway errors here while a job is running
			nonFatalError := strings.Contains(err.Error(), "502 Bad Gateway") ||
				strings.Contains(err.Error(), "504 Gateway Time-out") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "dial tcp")

			if nonFatalError {
				log.Printf("Ignoring non fatal error trying to get job info: %s", err.Error())
			}
			return !nonFatalError, err
		}
		jobState := getJobState(jobInfo)
		log.Debugf("Job %s state %s", heappeJobNodeName, jobState)

		switch jobState {
		case jobStateCompleted:
			// job has been done successfully
			jobDoneID = jobID
			jobDoneNodeName = heappeJobNodeName
			if !cancelRemainingJobs {
				// No cancel of running job needed, no need to check other jobs statuses
				break
			}
		case jobStateCreated, jobStatePending, jobStateRunning:
			// Not yet running: monitoring is keeping on (deregister stays false)
			allFinished = false
			if cancelRemainingJobs {
				cancelJobNodeNameID[heappeJobNodeName] = jobID
			}
		default:
			// Other cases as FAILED, CANCELED : error is return with job state and job info is logged
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(fmt.Sprintf("job state:%+v", jobState))
		}

	} // end loop on jobs

	if jobDoneNodeName == "" {
		if !allFinished {
			log.Debugf("%s: no job done yet and jobs still running, still monitoring...", actionData.nodeName)
			return false, nil
		} else {
			err = errors.Errorf("%s: all monitored job finished and no job succeeded", actionData.nodeName)
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(err.Error())
			return true, err
		}
	}

	// A job is done, exposing its attribute values here
	err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
		jobIDConsulAttribute, strconv.FormatInt(jobDoneID, 10))
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to store job ID %d in urgent computing job %s: %s", jobDoneID, actionData.nodeName, err.Error()))
		return true, err
	}

	heappeURL := ""
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, jobDoneNodeName, "0", heappeURLConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", jobDoneNodeName, heappeURLConsulAttribute, err.Error()))
		return true, err
	} else if val != nil {
		heappeURL = val.RawString()
	}
	err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
		heappeURLConsulAttribute, heappeURL)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to store HEAppE URL in urgent computing job %s: %s", actionData.nodeName, err.Error()))
	}

	startDate := ""
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, jobDoneNodeName, "0", startDateConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", jobDoneNodeName, startDateConsulAttribute, err.Error()))
	} else if val != nil {
		startDate = val.RawString()
	}
	err = deployments.SetAttributeForAllInstances(ctx, deploymentID, actionData.nodeName,
		startDateConsulAttribute, startDate)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to store start date in urgent computing job %s: %s", actionData.nodeName, err.Error()))
	}

	var transferVal map[string]string
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, jobDoneNodeName, "0", transferConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", jobDoneNodeName, transferConsulAttribute, err.Error()))
	} else if val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &transferVal)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to convert file transfer details for %s from %s: %s", jobDoneNodeName, val.RawString(), err.Error()))
			return true, err
		}
	}
	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName, transferConsulAttribute,
		transferVal)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to set file transfer details in urgent computing job %s: %s", actionData.nodeName, err.Error()))
		err = errors.Wrapf(err, "Failed to store file transfer details for %s", actionData.nodeName)
		return true, err
	}

	var changedFiles []heappe.ChangedFile
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, jobDoneNodeName, "0", changedFilesConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", jobDoneNodeName, changedFilesConsulAttribute, err.Error()))
	} else if val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &changedFiles)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to convert changed files details for %s from %s: %s", jobDoneNodeName, val.RawString(), err.Error()))
			return true, err
		}
	}
	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName, changedFilesConsulAttribute,
		changedFiles)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to set changed files details in urgent computing job %s: %s", actionData.nodeName, err.Error()))
		err = errors.Wrapf(err, "Failed to store changed files details for %s", actionData.nodeName)
		return true, err
	}

	var tasksNameId map[string]string
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, jobDoneNodeName, "0", tasksNameIDConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", jobDoneNodeName, tasksNameIDConsulAttribute, err.Error()))
	} else if val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &tasksNameId)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to convert tasks name - id details for %s from %s: %s", jobDoneNodeName, val.RawString(), err.Error()))
			return true, err
		}
	}
	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName, tasksNameIDConsulAttribute,
		tasksNameId)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to set tasks name - id in urgent computing job %s: %s", actionData.nodeName, err.Error()))
		err = errors.Wrapf(err, "Failed to store tasks name - id details for %s", actionData.nodeName)
		return true, err
	}

	var tasksNameStatus map[string]string
	val, err = deployments.GetInstanceAttributeValue(ctx, deploymentID, jobDoneNodeName, "0", tasksNameStatusConsulAttribute)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to get %s attribute %s: %s", jobDoneNodeName, tasksNameStatusConsulAttribute, err.Error()))
	} else if val != nil && val.RawString() != "" {
		err = json.Unmarshal([]byte(val.RawString()), &tasksNameStatus)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to convert tasks name - id details for %s from %s: %s", jobDoneNodeName, val.RawString(), err.Error()))
			return true, err
		}
	}
	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, actionData.nodeName, tasksNameStatusConsulAttribute,
		tasksNameStatus)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
			fmt.Sprintf("Failed to set tasks name - status in urgent computing job %s: %s", actionData.nodeName, err.Error()))
		err = errors.Wrapf(err, "Failed to store tasks name - status details for %s", actionData.nodeName)
		return true, err
	}

	// Set state
	err = deployments.SetInstanceStateStringWithContextualLogs(ctx, deploymentID, actionData.nodeName, "0", jobStateCompleted)
	if err != nil {
		log.Printf("Failed to set instance %s %s state %s: %s", deploymentID, actionData.nodeName, jobStateCompleted, err.Error())
	}

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(
		fmt.Sprintf("Jobs to cancel by urgent computing job %s: %v", actionData.nodeName, cancelJobNodeNameID))

	// Cancel remaining jobs if requires
	for nodeName, jobID := range cancelJobNodeNameID {
		jobExec := Execution{
			Cfg:          cfg,
			DeploymentID: deploymentID,
			NodeName:     nodeName,
			User:         user,
		}
		cancelErr := jobExec.cancelJobForUrgentComputing(ctx)
		if cancelErr != nil {
			// non-fatal error
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).RegisterAsString(
				fmt.Sprintf("Failed to cancel job %d urgent computing job %s: %s", jobID, actionData.nodeName, err.Error()))
		}
	}
	return true, err
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
				return errors.Wrapf(err, "Failed to compute offset for log file on deployment %s node %s job %d offset %+v",
					deploymentID, nodeName, jobInfo.ID, tOffset)
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

	heappeClient, err := getHEAppEClient(ctx, cfg, deploymentID, actionData.nodeName, action.Data["user"])
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
	if jobState == jobStateCreated || jobState == jobStatePending {
		// Job not yet running, no need to check for files created yet
		return false, err
	}
	changedFiles, err := heappeClient.ListChangedFilesForJob(actionData.jobID)
	if err != nil {
		return true, err
	}

	var foundFile bool
	for _, changedFile := range changedFiles {
		if changedFile.FileName == actionData.filePath {
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
	case 0, 1:
		strValue = jobStateCreated
	case 2, 4:
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
