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
	"time"

	"github.com/pkg/errors"

	"github.com/lexis-project/yorc-heappe-plugin/v1/job"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/prov"
)

const (
	heappeInfrastructureType              = "heappe"
	locationJobMonitoringTimeInterval     = "job_monitoring_time_interval"
	locationDefaultMonitoringTimeInterval = 5 * time.Second
	heappeJobType                         = "org.heappe.nodes.Job"
	heappeSendDatasetType                 = "org.heappe.nodes.Dataset"
	heappeReceiveDatasetType              = "org.heappe.nodes.Results"
	heappeWaitFileGetContent              = "org.heappe.nodes.WaitFileAndGetContentJob"
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
		return nil, err
	}
	locationProps, err := locationMgr.GetLocationPropertiesForNode(ctx,
		deploymentID, nodeName, heappeInfrastructureType)
	if err != nil {
		return nil, err
	}

	monitoringTimeInterval := locationProps.GetDuration(locationJobMonitoringTimeInterval)
	if monitoringTimeInterval <= 0 {
		// Default value
		monitoringTimeInterval = locationDefaultMonitoringTimeInterval
	}

	if isJob {
		exec = &job.Execution{
			KV:                     kv,
			Cfg:                    cfg,
			DeploymentID:           deploymentID,
			TaskID:                 taskID,
			NodeName:               nodeName,
			Operation:              operation,
			MonitoringTimeInterval: monitoringTimeInterval,
		}

		return exec, err
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
		Operation:              operation,
		MonitoringTimeInterval: monitoringTimeInterval,
	}

	return exec, exec.ResolveExecution(ctx)
}
