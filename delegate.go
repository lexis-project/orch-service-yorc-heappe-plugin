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
	"strings"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/tasks"
	"github.com/ystia/yorc/v4/tosca"
)

type delegateExecutor struct{}

func (de *delegateExecutor) ExecDelegate(ctx context.Context, cfg config.Configuration, taskID, deploymentID, nodeName, delegateOperation string) error {
	log.Debugf("Entering plugin ExecDelegate")

	// Get node instances related to this task (may be a subset of all instances for a scaling operation for instance)
	instances, err := tasks.GetInstances(ctx, taskID, deploymentID, nodeName)
	if err != nil {
		return err
	}

	// Emit events and logs on instance status change
	for _, instanceName := range instances {
		err = deployments.SetInstanceStateWithContextualLogs(ctx, deploymentID, nodeName, instanceName, tosca.NodeStateCreating)
		if err != nil {
			return err
		}
	}

	operation := prov.Operation{
		Name: strings.ToLower(delegateOperation),
	}
	exec, err := newExecution(ctx, cfg, taskID, deploymentID, nodeName, operation)
	if err != nil {
		return err
	}

	err = exec.Execute(ctx)
	if err != nil {
		return err
	}

	for _, instanceName := range instances {
		err = deployments.SetInstanceStateWithContextualLogs(ctx, deploymentID, nodeName, instanceName, tosca.NodeStateStarted)
		if err != nil {
			return err
		}
	}
	return nil
}
