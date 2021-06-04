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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/lexis-project/yorc-heappe-plugin/job"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
)

const (
	heappeInfrastructureType              = "heappe"
	locationJobMonitoringTimeInterval     = "job_monitoring_time_interval"
	locationDefaultMonitoringTimeInterval = 5 * time.Second
	heappeJobType                         = "org.lexis.common.heappe.nodes.pub.Job"
	heappeSendDatasetType                 = "org.lexis.common.heappe.nodes.Dataset"
	heappeReceiveDatasetType              = "org.lexis.common.heappe.nodes.Results"
	heappeWaitFileGetContent              = "org.lexis.common.heappe.nodes.WaitFileAndGetContentJob"
)

// Execution is the interface holding functions to execute an operation
type Execution interface {
	ResolveExecution(ctx context.Context) error
	ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error)
	Execute(ctx context.Context) error
}

func newExecution(ctx context.Context, cfg config.Configuration, taskID, deploymentID, nodeName string,
	operation prov.Operation) (Execution, error) {

	consulClient, err := cfg.GetConsulClient()
	if err != nil {
		return nil, err
	}
	kv := consulClient.KV()

	var exec Execution
	isJob, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, heappeJobType)
	if err != nil {
		return exec, err
	}

	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return exec, err
	}
	locationProps, err := locationMgr.GetLocationPropertiesForNode(ctx,
		deploymentID, nodeName, heappeInfrastructureType)
	if err != nil {
		return exec, err
	}

	monitoringTimeInterval := locationProps.GetDuration(locationJobMonitoringTimeInterval)
	if monitoringTimeInterval <= 0 {
		// Default value
		monitoringTimeInterval = locationDefaultMonitoringTimeInterval
	}

	ids, err := deployments.GetNodeInstancesIds(ctx, deploymentID, nodeName)
	if err != nil {
		return exec, err
	}

	if len(ids) == 0 {
		return exec, errors.Errorf("Found no instance for node %s in deployment %s", nodeName, deploymentID)
	}

	// Getting an AAI client to check token validity
	aaiClient := job.GetAAIClient(deploymentID, locationProps)

	accessToken, err := aaiClient.GetAccessToken()
	if err != nil {
		return nil, err
	}

	if accessToken == "" {
		token, err := deployments.GetStringNodePropertyValue(ctx, deploymentID,
			nodeName, "token")
		if err != nil {
			return exec, err
		}

		if token == "" {
			return exec, errors.Errorf("Found no token node %s in deployment %s", nodeName, deploymentID)
		}

		valid, err := aaiClient.IsAccessTokenValid(ctx, token)
		if err != nil {
			return exec, errors.Wrapf(err, "Failed to check validity of token")
		}

		if !valid {
			errorMsg := fmt.Sprintf("Token provided in input for Job %s is not anymore valid", nodeName)
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelERROR, deploymentID).Registerf(errorMsg)
			return exec, errors.Errorf(errorMsg)
		}
		// Exchange this token for an access and a refresh token for the orchestrator
		accessToken, _, err = aaiClient.ExchangeToken(ctx, token)
		if err != nil {
			return exec, errors.Wrapf(err, "Failed to exchange token for orchestrator")
		}

		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).Registerf(
			fmt.Sprintf("Token exchanged for an orchestrator client access/refresh token for node %s", nodeName))

	}

	// Checking the access token validity
	valid, err := aaiClient.IsAccessTokenValid(ctx, accessToken)
	if err != nil {
		return exec, errors.Wrapf(err, "Failed to check validity of access token")
	}

	if !valid {
		log.Printf("HEAppE plugin requests to refresh token for deployment %s\n", deploymentID)
		accessToken, _, err = aaiClient.RefreshToken(ctx)
		if err != nil {
			return exec, errors.Wrapf(err, "Failed to refresh token for orchestrator")
		}
	}

	// Getting user info
	userInfo, err := aaiClient.GetUserInfo(ctx, accessToken)
	if err != nil {
		return exec, errors.Wrapf(err, "Job %s, failed to get user info from access token", nodeName)
	}

	if isJob {

		listFiles, err := deployments.GetBooleanNodeProperty(ctx, deploymentID,
			nodeName, "listChangedFilesWhileRunning")
		if err != nil {
			return exec, err
		}

		exec = &job.Execution{
			KV:                     kv,
			Cfg:                    cfg,
			DeploymentID:           deploymentID,
			TaskID:                 taskID,
			NodeName:               nodeName,
			User:                   userInfo.GetName(),
			ListFilesWhileRunning:  listFiles,
			Operation:              operation,
			MonitoringTimeInterval: monitoringTimeInterval,
		}

		return exec, exec.ResolveExecution(ctx)
	}

	isReceiveDataset := false
	isWaitFileGetContent := false
	isSendDataset, err := deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, heappeSendDatasetType)
	if err != nil {
		return exec, errors.Wrapf(err, "Could not get type for deployment %s node %s", deploymentID, nodeName)
	}
	if !isSendDataset {
		isReceiveDataset, err = deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, heappeReceiveDatasetType)
		if err != nil {
			return exec, errors.Wrapf(err, "Could not get type for deployment %s node %s", deploymentID, nodeName)
		}

		if !isReceiveDataset {
			isWaitFileGetContent, err = deployments.IsNodeDerivedFrom(ctx, deploymentID, nodeName, heappeWaitFileGetContent)
			if err != nil {
				return exec, errors.Wrapf(err, "Could not get type for deployment %s node %s", deploymentID, nodeName)
			}

		}

	}

	if !isSendDataset && !isReceiveDataset && !isWaitFileGetContent {
		return exec, errors.Errorf("operation %q supported only for nodes derived from %q, %q, %q or %q",
			operation, heappeJobType, heappeSendDatasetType, heappeReceiveDatasetType, heappeWaitFileGetContent)
	}

	exec = &job.DatasetTransferExecution{
		KV:                     kv,
		Cfg:                    cfg,
		DeploymentID:           deploymentID,
		TaskID:                 taskID,
		NodeName:               nodeName,
		User:                   userInfo.GetName(),
		Operation:              operation,
		MonitoringTimeInterval: monitoringTimeInterval,
	}

	return exec, exec.ResolveExecution(ctx)
}
