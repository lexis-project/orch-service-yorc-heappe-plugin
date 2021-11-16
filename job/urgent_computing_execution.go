// Copyright 2021 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
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
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/helper/consulutil"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/storage"
	storageTypes "github.com/ystia/yorc/v4/storage/types"
	"github.com/ystia/yorc/v4/tosca"
)

const (
	actionCancelRemainingJobs          = "cancelRemainingJobs"
	actionAssociatedHEAppeJobNodeNames = "HEAppEJobNodeNames"
	cancelFlagConsulAttribute          = "is_canceled"
)

// UrgentComputingExecution holds urgent computing job xecution properties
type UrgentComputingExecution struct {
	KV                     *api.KV
	Cfg                    config.Configuration
	DeploymentID           string
	TaskID                 string
	NodeName               string
	User                   string
	cancelRemainingJobs    bool
	Operation              prov.Operation
	MonitoringTimeInterval time.Duration
}

// ExecuteAsync executes an asynchronous operation
func (u *UrgentComputingExecution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {
	if strings.ToLower(u.Operation.Name) != tosca.RunnableRunOperationName {
		return nil, 0, errors.Errorf("Unsupported asynchronous operation %q", u.Operation.Name)
	}

	data := make(map[string]string)
	data["taskID"] = u.TaskID
	data["nodeName"] = u.NodeName
	data["user"] = u.User
	data[actionCancelRemainingJobs] = strconv.FormatBool(u.cancelRemainingJobs)

	// Get associated HEAppE job names
	jobNodeNames, err := u.getAssociatedHEAppEJobNodeNames(ctx)
	if err != nil {
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, u.DeploymentID).Registerf(
			"Failed to get HEAppE job associated to %s, error %s", u.NodeName, err.Error())
		return &prov.Action{}, u.MonitoringTimeInterval, err
	}

	data[actionAssociatedHEAppeJobNodeNames] = strings.Join(jobNodeNames, ",")

	return &prov.Action{ActionType: "heappe-job-urgent-computing-monitoring", Data: data}, u.MonitoringTimeInterval, nil
}

// Execute executes a synchronous operation
func (u *UrgentComputingExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(u.Operation.Name) {
	case installOperation, "standard.create":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
			"Creating Job %q", u.NodeName)
		err = u.createJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
				"Failed to create Job %q, error %s", u.NodeName, err.Error())

		}
	case uninstallOperation, "standard.delete":
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
			"Deleting Job %q", u.NodeName)
		err = u.deleteJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
				"Failed to delete Job %q, error %s", u.NodeName, err.Error())

		}
	case enableFileTransferOperation, disableFileTransferOperation, listChangedFilesOperation:
		// Nothing to do
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
			"Operation %s for job %s, nothing to do", u.Operation.Name, u.NodeName)
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
			"Submitting Job %q", u.NodeName)
		err = u.submitJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
				"Failed to submit Job %q, error %s", u.NodeName, err.Error())

		}
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
			"Canceling Job %q", u.NodeName)
		err = u.cancelJob(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
				"Failed to cancel Job %q, error %s", u.NodeName, err.Error())

		}
	default:
		err = errors.Errorf("Unsupported operation %q", u.Operation.Name)
	}

	return err
}

// ResolveExecution resolves inputs and artifacts before the execution of an operation
func (u *UrgentComputingExecution) ResolveExecution(ctx context.Context) error {
	log.Debugf("Preparing execution of operation %q on node %q for deployment %q", u.Operation.Name, u.NodeName, u.DeploymentID)
	// nothing to do here
	return nil
}

func (u *UrgentComputingExecution) createJob(ctx context.Context) error {

	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
		"Creating Urgent computing job %s ", u.NodeName)

	// Store the cancel that will be set to true if ever this job is canceled
	_ = deployments.SetAttributeForAllInstances(ctx, u.DeploymentID, u.NodeName,
		cancelFlagConsulAttribute, strconv.FormatBool(false))
	return nil
}

func (u *UrgentComputingExecution) submitJob(ctx context.Context) error {
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
		"Submitting Urgent computing job %s ", u.NodeName)
	return nil
}

func (u *UrgentComputingExecution) cancelJob(ctx context.Context) error {
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
		"Canceling Urgent computing job %s ", u.NodeName)
	return deployments.SetAttributeForAllInstances(ctx, u.DeploymentID, u.NodeName,
		cancelFlagConsulAttribute, strconv.FormatBool(true))
}

func (u *UrgentComputingExecution) deleteJob(ctx context.Context) error {
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, u.DeploymentID).Registerf(
		"Deleting Urgent computing job %s ", u.NodeName)
	return nil
}

func (u *UrgentComputingExecution) getAssociatedHEAppEJobNodeNames(ctx context.Context) ([]string, error) {
	var heappeJobNodeNames []string
	nodeTemplate, err := getStoredNodeTemplate(ctx, u.DeploymentID, u.NodeName)
	if err != nil {
		return heappeJobNodeNames, err
	}

	// Get the associated targets
	for _, nodeReq := range nodeTemplate.Requirements {
		for _, reqAssignment := range nodeReq {
			// Check the corresponding node is not skipped
			heappeNodeTemplate, err := getStoredNodeTemplate(ctx, u.DeploymentID, reqAssignment.Node)
			if err != nil {
				return heappeJobNodeNames, err
			}
			if len(heappeNodeTemplate.Metadata[tosca.MetadataLocationNameKey]) > 0 &&
				heappeNodeTemplate.Metadata[tosca.MetadataLocationNameKey] == "SKIPPED" {
				log.Debugf("Ignoring HEAppE job %s to skip", reqAssignment.Node)
				continue
			}
			heappeJobNodeNames = append(heappeJobNodeNames, reqAssignment.Node)
		}
	}
	return heappeJobNodeNames, err
}

// getStoredNodeTemplate returns the description of a node stored by Yorc
func getStoredNodeTemplate(ctx context.Context, deploymentID, nodeName string) (*tosca.NodeTemplate, error) {
	node := new(tosca.NodeTemplate)
	nodePath := path.Join(consulutil.DeploymentKVPrefix, deploymentID, "topology", "nodes", nodeName)
	found, err := storage.GetStore(storageTypes.StoreTypeDeployment).Get(nodePath, node)
	if !found {
		err = errors.Errorf("No such node %s in deployment %s", nodeName, deploymentID)
	}
	return node, err
}
