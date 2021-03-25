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

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
)

type operationExecutor struct{}

func (e *operationExecutor) ExecAsyncOperation(ctx context.Context, cfg config.Configuration, taskID, deploymentID, nodeName string, operation prov.Operation, stepName string) (*prov.Action, time.Duration, error) {

	exec, err := newExecution(ctx, cfg, taskID, deploymentID, nodeName, operation)
	if err != nil {
		return nil, 0, err
	}

	return exec.ExecuteAsync(ctx)
}

func (e *operationExecutor) ExecOperation(ctx context.Context, cfg config.Configuration, taskID, deploymentID, nodeName string, operation prov.Operation) error {

	log.Debugf("Executing operation %+v", operation)

	//var err error
	// operationName := strings.ToLower(operation.Name)

	// create/delete operations are managed by the Delegate Executor

	exec, err := newExecution(ctx, cfg, taskID, deploymentID, nodeName, operation)
	if err != nil {
		return err
	}

	err = exec.Execute(ctx)
	return err
}
