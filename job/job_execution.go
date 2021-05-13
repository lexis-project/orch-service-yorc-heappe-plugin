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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/laurentganne/yorcoidc"
	"github.com/lexis-project/yorc-heappe-plugin/heappe"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	locationAAIURL                = "aai_url"
	locationAAIClientID           = "aai_client_id"
	locationAAIClientSecret       = "aai_client_secret"
	locationAAIRealm              = "aai_realm"
	installOperation              = "install"
	uninstallOperation            = "uninstall"
	enableFileTransferOperation   = "custom.enable_file_transfer"
	disableFileTransferOperation  = "custom.disable_file_transfer"
	listChangedFilesOperation     = "custom.list_changed_files"
	listChangedFilesAction        = "listChangedFilesAction"
	jobSpecificationProperty      = "JobSpecification"
	infrastructureType            = "heappe"
	jobIDConsulAttribute          = "job_id"
	heappeURLConsulAttribute      = "heappe_url"
	transferUser                  = "user"
	transferKey                   = "key"
	transferServer                = "server"
	transferPath                  = "path"
	transferConsulAttribute       = "file_transfer"
	transferObjectConsulAttribute = "transfer_object"
	tasksNameIDConsulAttribute    = "tasks_name_id"
	startDateConsulAttribute      = "start_date"
	changedFilesConsulAttribute   = "changed_files"
	tasksParamsEnvVar             = "TASKS_PARAMETERS"
)

// Execution holds job Execution properties
type Execution struct {
	KV                     *api.KV
	Cfg                    config.Configuration
	DeploymentID           string
	TaskID                 string
	NodeName               string
	User                   string
	ListFilesWhileRunning  bool
	Operation              prov.Operation
	MonitoringTimeInterval time.Duration
	EnvInputs              []*operations.EnvInput
	VarInputsNames         []string
}

// ExecuteAsync executes an asynchronous operation
func (e *Execution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	if strings.ToLower(e.Operation.Name) != tosca.RunnableRunOperationName {
		return nil, 0, errors.Errorf("Unsupported asynchronous operation %q", e.Operation.Name)
	}

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return nil, 0, err
	}

	data := make(map[string]string)
	data["taskID"] = e.TaskID
	data["nodeName"] = e.NodeName
	data["jobID"] = strconv.FormatInt(jobID, 10)
	data["user"] = e.User
	data[listChangedFilesAction] = strconv.FormatBool(e.ListFilesWhileRunning)

	return &prov.Action{ActionType: "heappe-job-monitoring", Data: data}, e.MonitoringTimeInterval, err
}

// Execute executes a synchronous operation
func (e *Execution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Creating Job %q", e.NodeName)
		err = e.createJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to create Job %q, error %s", e.NodeName, err.Error())

		}
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Deleting Job %q", e.NodeName)
		err = e.deleteJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to delete Job %q, error %s", e.NodeName, err.Error())

		}
	case enableFileTransferOperation:
		err = e.enableFileTransfer(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to enable file transfer for Job %q, error %s", e.NodeName, err.Error())

		}
	case disableFileTransferOperation:
		err = e.disableFileTransfer(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to disable file transfer for Job %q, error %s", e.NodeName, err.Error())

		}
	case listChangedFilesOperation:
		err = e.listChangedFiles(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to list changed files for Job %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting Job %q", e.NodeName)
		err = e.submitJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to submit Job %q, error %s", e.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
		err = e.cancelJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Failed to cancel Job %q, error %s", e.NodeName, err.Error())

		}
	default:
		err = errors.Errorf("Unsupported operation %q", e.Operation.Name)
	}

	return err
}

// ResolveExecution resolves inputs and artifacts before the execution of an operation
func (e *Execution) ResolveExecution(ctx context.Context) error {
	log.Debugf("Preparing execution of operation %q on node %q for deployment %q", e.Operation.Name, e.NodeName, e.DeploymentID)

	return e.resolveInputs(ctx)
}

func (e *Execution) createJob(ctx context.Context) error {

	jobSpec, err := e.getJobSpecification(ctx)
	if err != nil {
		return err
	}

	tasksParamsStr := e.getValueFromEnvInputs(tasksParamsEnvVar)

	var tasksParams map[string][]heappe.CommandTemplateParameterValue

	if tasksParamsStr != "" {
		data, err := base64.StdEncoding.DecodeString(tasksParamsStr)
		if err != nil {
			fmt.Println("error:", err)
			return errors.Wrapf(err, "Error base64 decoding tasks parameters values %s", tasksParamsStr)
		}
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Runtime tasks data %s", data)
		err = json.Unmarshal([]byte(data), &tasksParams)
		if err != nil {
			log.Printf("Got error unmarshaling Runtime tasks parameters: %+s\n", err.Error())
			return errors.Wrapf(err, "Error unmarshaling runtime tasks parameters %s", tasksParamsStr)
		}
	}

	// Update tasks parameters in job specification
	for i, task := range jobSpec.Tasks {
		val, ok := tasksParams[task.Name]
		if ok {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Setting task %s parameters computed at runtime: %+v ", task.Name, val)
			jobSpec.Tasks[i].TemplateParameterValues = val
		}
	}

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
		"Creating job %+v ", jobSpec)

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	jobInfo, err := heappeClient.CreateJob(jobSpec)
	if err != nil {
		return err
	}

	// Store the job id
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		jobIDConsulAttribute, strconv.FormatInt(jobInfo.ID, 10))
	if err != nil {
		err = errors.Wrapf(err, "Job %d created on HEAppE, but failed to store this job id", jobInfo.ID)
		return err
	}

	// Store the URL of the HEAppE instance where this job is created
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		heappeURLConsulAttribute, heappeClient.GetURL())
	if err != nil {
		err = errors.Wrapf(err, "Job %d created on HEAppE, but failed to store the HEAppE instancel URL %s", jobInfo.ID, heappeClient.GetURL())
		return err
	}

	// Store the map providing the correspondance between task names and task IDs
	tasksNameId := make(map[string]string, len(jobInfo.Tasks))
	for _, taskInfo := range jobInfo.Tasks {
		tasksNameId[taskInfo.Name] = strconv.FormatInt(taskInfo.ID, 10)
	}
	err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName, tasksNameIDConsulAttribute,
		tasksNameId)
	if err != nil {
		err = errors.Wrapf(err, "Job %d, failed to store taskIDs details", jobInfo.ID)
		return err
	}

	// Start date is not yet defined, but initializing this value for
	// components related to this job which would check this attribute value
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		startDateConsulAttribute, "")
	if err != nil {
		err = errors.Wrapf(err, "Job %d created on HEAppE, but failed to store empty submission date", jobInfo.ID)
	}

	// Changed files is not yet defined, but initializing this value for
	// components related to this job which would check this attribute value
	changedFiles := make([]heappe.ChangedFile, 0)
	if err != nil {
		return err
	}

	err = deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName,
		changedFilesConsulAttribute, changedFiles)
	if err != nil {
		err = errors.Wrapf(err, "Job %d, failed to store list of changed files", jobInfo.ID)
		return err
	}

	return err
}

func (e *Execution) getValueFromEnvInputs(envVar string) string {

	var result string
	for _, envInput := range e.EnvInputs {
		if envInput.Name == envVar {
			result = envInput.Value
			break
		}
	}
	return result

}

func (e *Execution) resolveInputs(ctx context.Context) error {
	var err error
	log.Debugf("Get environment inputs for node:%q", e.NodeName)
	e.EnvInputs, e.VarInputsNames, err = operations.ResolveInputsWithInstances(
		ctx, e.DeploymentID, e.NodeName, e.TaskID, e.Operation, nil, nil)
	log.Debugf("Environment inputs: %v", e.EnvInputs)
	return err
}

func (e *Execution) deleteJob(ctx context.Context) error {

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return err
	}

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	return heappeClient.DeleteJob(jobID)
}

func (e *Execution) submitJob(ctx context.Context) error {

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return err
	}

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	return heappeClient.SubmitJob(jobID)
}

func (e *Execution) enableFileTransfer(ctx context.Context) error {

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return err
	}

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	ftDetails, err := heappeClient.GetFileTransferMethod(jobID)
	if err != nil {
		return err
	}

	return e.storeFileTransferAttributes(ctx, ftDetails, jobID)
}

func (e *Execution) disableFileTransfer(ctx context.Context) error {

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return err
	}

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	ids, err := deployments.GetNodeInstancesIds(ctx, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	if len(ids) == 0 {
		return errors.Errorf("Found no instance for node %s in deployment %s", e.NodeName, e.DeploymentID)
	}

	attr, err := deployments.GetInstanceAttributeValue(ctx, e.DeploymentID, e.NodeName, ids[0], transferObjectConsulAttribute)
	if err != nil {
		return err
	}

	var fileTransfer heappe.FileTransferMethod
	err = json.Unmarshal([]byte(attr.RawString()), &fileTransfer)
	if err != nil {
		return err
	}

	err = heappeClient.EndFileTransfer(jobID, fileTransfer)
	if err != nil {
		return err
	}

	// Reset file transfer attributes
	fileTransfer.Credentials.Username = ""
	fileTransfer.Credentials.PrivateKey = ""
	fileTransfer.ServerHostname = ""
	fileTransfer.SharedBasepath = ""

	return e.storeFileTransferAttributes(ctx, fileTransfer, jobID)
}

func (e *Execution) listChangedFiles(ctx context.Context) error {

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return err
	}

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	return updateListOfChangedFiles(ctx, heappeClient, e.DeploymentID, e.NodeName, jobID)
}

func updateListOfChangedFiles(ctx context.Context, heappeClient heappe.Client, deploymentID, nodeName string, jobID int64) error {

	changedFiles, err := heappeClient.ListChangedFilesForJob(jobID)
	if err != nil {
		return err
	}

	err = deployments.SetAttributeComplexForAllInstances(ctx, deploymentID, nodeName,
		changedFilesConsulAttribute, changedFiles)
	if err != nil {
		err = errors.Wrapf(err, "Job %d, failed to store list of changed files", jobID)
		return err
	}

	return err

}

func (e *Execution) storeFileTransferAttributes(ctx context.Context, fileTransfer heappe.FileTransferMethod, jobID int64) error {

	err := deployments.SetAttributeComplexForAllInstances(ctx, e.DeploymentID, e.NodeName, transferConsulAttribute,
		map[string]string{
			transferUser:   fileTransfer.Credentials.Username,
			transferKey:    fileTransfer.Credentials.PrivateKey,
			transferServer: fileTransfer.ServerHostname,
			transferPath:   fileTransfer.SharedBasepath,
		})
	if err != nil {
		err = errors.Wrapf(err, "Job %d, failed to store file transfer details", jobID)
		return err
	}

	// Storing the full file transfer object needed when it will be disabled
	v, err := json.Marshal(fileTransfer)
	if err != nil {
		return err
	}
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		transferObjectConsulAttribute, string(v))
	if err != nil {
		err = errors.Wrapf(err, "Job %d, failed to store file transfer path", jobID)
	}

	return err
}

func (e *Execution) cancelJob(ctx context.Context) error {

	jobID, err := e.getJobID(ctx)
	if err != nil {
		return err
	}

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName, e.User)
	if err != nil {
		return err
	}

	return heappeClient.CancelJob(jobID)
}

func (e *Execution) getJobID(ctx context.Context) (int64, error) {
	var jobID int64

	val, err := deployments.GetInstanceAttributeValue(ctx, e.DeploymentID, e.NodeName, "0", jobIDConsulAttribute)
	if err != nil {
		return jobID, errors.Wrapf(err, "Failed to get job id for deployment %s node %s", e.DeploymentID, e.NodeName)
	} else if val == nil {
		return jobID, errors.Errorf("Found no job id for deployment %s node %s", e.DeploymentID, e.NodeName)
	}

	strVal := val.RawString()
	jobID, err = strconv.ParseInt(strVal, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s", strVal, e.DeploymentID, e.NodeName)
	}

	return jobID, err

}

func (e *Execution) getJobSpecification(ctx context.Context) (heappe.JobSpecification, error) {

	var jobSpec heappe.JobSpecification
	var err error

	var stringPropNames = []struct {
		field    *string
		propName string
	}{
		{field: &(jobSpec.Name), propName: "Name"},
		{field: &(jobSpec.Project), propName: "Project"},
		{field: &(jobSpec.NotificationEmail), propName: "NotificationEmail"},
		{field: &(jobSpec.NotificationEmail), propName: "PhoneNumber"},
	}

	for _, stringPropName := range stringPropNames {
		*(stringPropName.field), err = deployments.GetStringNodePropertyValue(ctx, e.DeploymentID,
			e.NodeName, jobSpecificationProperty, stringPropName.propName)
		if err != nil {
			return jobSpec, err
		}
	}

	var intPropNames = []struct {
		field    *int
		propName string
	}{
		{field: &(jobSpec.ClusterID), propName: "ClusterId"},
		{field: &(jobSpec.WaitingLimit), propName: "WaitingLimit"},
		{field: &(jobSpec.FileTransferMethodID), propName: "FileTransferMethodId"},
	}

	for _, intPropName := range intPropNames {
		*(intPropName.field), err = getIntNodePropertyValue(ctx, e.DeploymentID,
			e.NodeName, jobSpecificationProperty, intPropName.propName)
		if err != nil {
			return jobSpec, err
		}
	}

	// Currently, the job file transfer method equals the cluster ID
	// so overriding its definition here
	jobSpec.FileTransferMethodID = jobSpec.ClusterID

	var boolPropNames = []struct {
		field    *bool
		propName string
	}{
		{field: &(jobSpec.NotifyOnAbort), propName: "NotifyOnAbort"},
		{field: &(jobSpec.NotifyOnFinish), propName: "NotifyOnFinish"},
		{field: &(jobSpec.NotifyOnStart), propName: "NotifyOnStart"},
	}

	for _, boolPropName := range boolPropNames {
		*(boolPropName.field), err = getBoolNodePropertyValue(ctx, e.DeploymentID,
			e.NodeName, jobSpecificationProperty, boolPropName.propName)
		if err != nil {
			return jobSpec, err
		}
	}

	// TODO: get environement variables

	// Getting associated tasks
	tasks, err := deployments.GetNodePropertyValue(ctx, e.DeploymentID, e.NodeName, jobSpecificationProperty, "Tasks")
	if err != nil {
		return jobSpec, err
	}

	if tasks != nil && tasks.RawString() != "" {
		var ok bool
		jobSpec.Tasks = make([]heappe.TaskSpecification, 0)
		taskArray, ok := tasks.Value.([]interface{})
		if !ok {
			return jobSpec, errors.Errorf(
				"failed to retrieve job tasks specification for deployment %s node %s, wrong type for %+s",
				e.DeploymentID, e.NodeName, tasks.RawString())
		}

		for _, taskVal := range taskArray {
			attrMap, ok := taskVal.(map[string]interface{})
			if !ok {
				return jobSpec, errors.Errorf(
					"failed to retrieve task specification for deployment %s node %s, wrong type for %+v",
					e.DeploymentID, e.NodeName, taskVal)
			}

			var task heappe.TaskSpecification

			// Get string properties

			var taskPropConsulPropString = []struct {
				taskProp   *string
				consulProp string
			}{
				{taskProp: &(task.Name), consulProp: "Name"},
				{taskProp: &(task.JobArrays), consulProp: "JobArrays"},
				{taskProp: &(task.StandardInputFile), consulProp: "StandardInputFile"},
				{taskProp: &(task.StandardOutputFile), consulProp: "StandardOutputFile"},
				{taskProp: &(task.StandardErrorFile), consulProp: "StandardErrorFile"},
				{taskProp: &(task.ProgressFile), consulProp: "ProgressFile"},
				{taskProp: &(task.LogFile), consulProp: "LogFile"},
				{taskProp: &(task.ClusterTaskSubdirectory), consulProp: "ClusterTaskSubdirectory"},
			}

			for _, props := range taskPropConsulPropString {
				rawValue, ok := attrMap[props.consulProp]
				if ok {
					val, ok := rawValue.(string)
					if !ok {
						return jobSpec, errors.Errorf(
							"Expected a string for deployment %s node %s task %s property %s, got %+v",
							e.DeploymentID, e.NodeName, task.Name, props.consulProp, rawValue)
					}
					*props.taskProp = val
				}
			}

			// Get int properties

			var taskPropConsulPropInt = []struct {
				taskProp   *int
				consulProp string
			}{
				{taskProp: &(task.MinCores), consulProp: "MinCores"},
				{taskProp: &(task.MaxCores), consulProp: "MaxCores"},
				{taskProp: &(task.WalltimeLimit), consulProp: "WalltimeLimit"},
				{taskProp: &(task.Priority), consulProp: "Priority"},
				{taskProp: &(task.ClusterNodeTypeID), consulProp: "ClusterNodeTypeId"},
				{taskProp: &(task.CommandTemplateID), consulProp: "CommandTemplateId"},
			}

			for _, props := range taskPropConsulPropInt {
				rawValue, ok := attrMap[props.consulProp]
				if ok {
					val, ok := rawValue.(string)
					if !ok {
						return jobSpec, errors.Errorf(
							"Expected a string stored for deployment %s node %s task %s property %s, got %+v",
							e.DeploymentID, e.NodeName, task.Name, props.consulProp, rawValue)
					}
					*props.taskProp, err = strconv.Atoi(val)
					if err != nil {
						return jobSpec, errors.Wrapf(err,
							"Cannot convert as an int value %q for deployment %s node %s task %s property %s",
							val, e.DeploymentID, e.NodeName, task.Name, props.consulProp)
					}
				}
			}

			var taskPropConsulPropBool = []struct {
				taskProp   *bool
				consulProp string
			}{
				{taskProp: &(task.IsExclusive), consulProp: "IsExclusive"},
				{taskProp: &(task.IsRerunnable), consulProp: "IsRerunnable"},
				{taskProp: &(task.CpuHyperThreading), consulProp: "CpuHyperThreading"},
			}

			for _, props := range taskPropConsulPropBool {
				rawValue, ok := attrMap[props.consulProp]
				if ok {
					val, ok := rawValue.(string)
					if !ok {
						return jobSpec, errors.Errorf(
							"Expected a string stored for deployment %s node %s task %s property %s, got %+v",
							e.DeploymentID, e.NodeName, task.Name, props.consulProp, rawValue)
					}
					*props.taskProp = (strings.ToLower(val) == "true")
				}
			}

			// get required nodes
			paramsRequiredNodes, ok := attrMap["RequiredNodes"]
			if ok {
				paramsArray, ok := paramsRequiredNodes.([]string)
				if !ok {
					return jobSpec, errors.Errorf(
						"failed to retrieve RequiredNodes parameter for deployment %s node %s task %s, wrong type for %+v",
						e.DeploymentID, e.NodeName, task.Name, paramsRequiredNodes)
				}

				task.RequiredNodes = paramsArray

			}

			// Get template parameters
			parameters, ok := attrMap["TemplateParameterValues"]
			if ok {
				paramsArray, ok := parameters.([]interface{})
				if !ok {
					return jobSpec, errors.Errorf(
						"failed to retrieve command template parameters for deployment %s node %s task %s, wrong type for %+v",
						e.DeploymentID, e.NodeName, task.Name, parameters)
				}

				for _, paramsVal := range paramsArray {
					attrMap, ok := paramsVal.(map[string]interface{})
					if !ok {
						return jobSpec, errors.Errorf(
							"failed to retrieve parameters for deployment %s node %s task %s, wrong type for %+v",
							e.DeploymentID, e.NodeName, task.Name, paramsVal)
					}

					var param heappe.CommandTemplateParameterValue
					v, ok := attrMap["CommandParameterIdentifier"]
					if !ok {
						return jobSpec, errors.Errorf(
							"Failed to get command parameter identifier for deployment %s node %s task %s, parameter %+v",
							e.DeploymentID, e.NodeName, task.Name, attrMap)
					}
					param.CommandParameterIdentifier, ok = v.(string)
					if !ok {
						return jobSpec, errors.Errorf(
							"Failed to get command parameter identifier string value for deployment %s node %s task %s, identifier %+v",
							e.DeploymentID, e.NodeName, task.Name, v)
					}

					v, ok = attrMap["ParameterValue"]
					if ok {
						param.ParameterValue, ok = v.(string)
						if !ok {
							return jobSpec, errors.Errorf(
								"Failed to get command parameter string value for deployment %s node %s task %s identifier %s, value %+v",
								e.DeploymentID, e.NodeName, task.Name, param.CommandParameterIdentifier, v)
						}

					}

					task.TemplateParameterValues = append(task.TemplateParameterValues, param)
				}
			}

			// Get environment variables
			parameters, ok = attrMap["EnvironmentVariables"]
			if ok {
				paramsArray, ok := parameters.([]interface{})
				if !ok {
					return jobSpec, errors.Errorf(
						"failed to retrieve environment variables for deployment %s node %s task %s, wrong type for %+v",
						e.DeploymentID, e.NodeName, task.Name, parameters)
				}

				for _, paramsVal := range paramsArray {
					attrMap, ok := paramsVal.(map[string]interface{})
					if !ok {
						return jobSpec, errors.Errorf(
							"failed to retrieve environment variable for deployment %s node %s task %s, wrong type for %+v",
							e.DeploymentID, e.NodeName, task.Name, paramsVal)
					}

					var param heappe.EnvironmentVariable
					v, ok := attrMap["Name"]
					if !ok {
						return jobSpec, errors.Errorf(
							"Failed to get environment variable name for deployment %s node %s task %s, parameter %+v",
							e.DeploymentID, e.NodeName, task.Name, attrMap)
					}
					param.Name, ok = v.(string)
					if !ok {
						return jobSpec, errors.Errorf(
							"Failed to get environment variable name string value for deployment %s node %s task %s, identifier %+v",
							e.DeploymentID, e.NodeName, task.Name, v)
					}

					v, ok = attrMap["Value"]
					if ok {
						param.Value, ok = v.(string)
						if !ok {
							return jobSpec, errors.Errorf(
								"Failed to get environment variable string value for deployment %s node %s task %s identifier %s, value %+v",
								e.DeploymentID, e.NodeName, task.Name, param.Name, v)
						}

					}

					task.EnvironmentVariables = append(task.EnvironmentVariables, param)
				}
			}

			// Get task parallelization variables
			parameters, ok = attrMap["TaskParalizationParameters"]
			if ok {
				paramsArray, ok := parameters.([]interface{})
				if !ok {
					return jobSpec, errors.Errorf(
						"failed to retrieve parallelization parameters for deployment %s node %s task %s, wrong type for %+v",
						e.DeploymentID, e.NodeName, task.Name, parameters)
				}

				for _, paramsVal := range paramsArray {
					attrMap, ok := paramsVal.(map[string]interface{})
					if !ok {
						return jobSpec, errors.Errorf(
							"failed to retrieve environment variable for deployment %s node %s task %s, wrong type for %+v",
							e.DeploymentID, e.NodeName, task.Name, paramsVal)
					}

					var param heappe.TaskParalizationParameter
					// Get int properties
					var paramPropConsulPropInt = []struct {
						paramProp  *int
						consulProp string
					}{
						{paramProp: &(param.MPIProcesses), consulProp: "MPIProcesses"},
						{paramProp: &(param.OpenMPThreads), consulProp: "OpenMPThreads"},
						{paramProp: &(param.MaxCores), consulProp: "MaxCores"},
					}

					for _, props := range paramPropConsulPropInt {
						rawValue, ok := attrMap[props.consulProp]
						if ok {
							val, ok := rawValue.(string)
							if !ok {
								return jobSpec, errors.Errorf(
									"Expected a string stored for deployment %s node %s task %s property %s, got %+v",
									e.DeploymentID, e.NodeName, task.Name, props.consulProp, rawValue)
							}
							*props.paramProp, err = strconv.Atoi(val)
							if err != nil {
								return jobSpec, errors.Wrapf(err,
									"Cannot convert as an int value %q for deployment %s node %s task %s property %s",
									val, e.DeploymentID, e.NodeName, task.Name, props.consulProp)
							}
						}
					}

					task.TaskParalizationParameters = append(task.TaskParalizationParameters, param)
				}
			}

			jobSpec.Tasks = append(jobSpec.Tasks, task)

		}
	}
	return jobSpec, err
}

func getIntNodePropertyValue(ctx context.Context, deploymentID, nodeName, propertyName string,
	nestedKeys ...string) (int, error) {

	var result int
	strVal, err := deployments.GetStringNodePropertyValue(ctx, deploymentID, nodeName, propertyName, nestedKeys...)
	if err != nil {
		return result, err
	}

	if len(strVal) > 0 {
		result, err = strconv.Atoi(strVal)
	}
	return result, err
}

func getBoolNodePropertyValue(ctx context.Context, deploymentID, nodeName, propertyName string,
	nestedKeys ...string) (bool, error) {

	result := false
	strVal, err := deployments.GetStringNodePropertyValue(ctx, deploymentID, nodeName, propertyName, nestedKeys...)
	if err != nil {
		return result, err
	}

	if len(strVal) > 0 {
		result = (strings.ToLower(strVal) == "true")
	}
	return result, err
}

func getHEAppEClient(ctx context.Context, cfg config.Configuration, deploymentID, nodeName, user string) (heappe.Client, error) {
	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return nil, err
	}

	locationProps, err := locationMgr.GetLocationPropertiesForNode(ctx, deploymentID,
		nodeName, infrastructureType)
	if err != nil {
		return nil, err
	}

	aaiClient := GetAAIClient(deploymentID, locationProps)
	accessToken, err := aaiClient.GetAccessToken()
	if err != nil {
		return nil, err
	}
	var refreshTokenFunc heappe.RefreshTokenFunc = func() (string, error) {
		accessToken, _, err := aaiClient.RefreshToken(ctx)
		return accessToken, err
	}

	return heappe.GetClient(locationProps, user, accessToken, refreshTokenFunc)
}

// GetAAIClient returns the AAI client for a given location
func GetAAIClient(deploymentID string, locationProps config.DynamicMap) yorcoidc.Client {
	url := locationProps.GetString(locationAAIURL)
	clientID := locationProps.GetString(locationAAIClientID)
	clientSecret := locationProps.GetString(locationAAIClientSecret)
	realm := locationProps.GetString(locationAAIRealm)
	return yorcoidc.GetClient(deploymentID, url, clientID, clientSecret, realm)
}

// RefreshToken refreshes an access token
func RefreshToken(ctx context.Context, locationProps config.DynamicMap, deploymentID, nodeName string) (string, string, error) {

	aaiClient := GetAAIClient(deploymentID, locationProps)
	// Getting an AAI client to check token validity
	accessToken, newRefreshToken, err := aaiClient.RefreshToken(ctx)
	return accessToken, newRefreshToken, err

}
