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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	scp "github.com/bramvdbogaerde/go-scp"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/deployments"
	"github.com/ystia/yorc/v4/events"
	"github.com/ystia/yorc/v4/helper/ziputil"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/prov"
	"github.com/ystia/yorc/v4/prov/operations"
	"github.com/ystia/yorc/v4/tosca"
	"golang.org/x/crypto/ssh"
)

const (
	sendInputsOperation    = "custom.sendjobinputs"
	getResultsOperation    = "custom.getresults"
	zipDatasetArtifactName = "zip_dataset"
	jobIDEnvVar            = "JOB_ID"
	filePathEnvVar         = "FILE_PATH"
	zipResultAttribute     = "zip_result"
)

// DatasetTransferExecution holds properties of a data transfer operation
type DatasetTransferExecution struct {
	KV                     *api.KV
	Cfg                    config.Configuration
	DeploymentID           string
	TaskID                 string
	NodeName               string
	Operation              prov.Operation
	OverlayPath            string
	Artifacts              map[string]string
	MonitoringTimeInterval time.Duration
	EnvInputs              []*operations.EnvInput
	VarInputsNames         []string
}

// ExecuteAsync executes an asynchronous operation - supported to wait for a file and get its content
func (e *DatasetTransferExecution) ExecuteAsync(ctx context.Context) (*prov.Action, time.Duration, error) {

	if strings.ToLower(e.Operation.Name) != tosca.RunnableRunOperationName {
		return nil, 0, errors.Errorf("Unsupported asynchronous operation %q", e.Operation.Name)
	}

	jobIDStr := e.getValueFromEnvInputs(jobIDEnvVar)
	if jobIDStr == "" {
		return nil, 0, errors.Errorf("Failed to get associated job ID")
	}
	_, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s",
			jobIDStr, e.DeploymentID, e.NodeName)
		return nil, 0, err
	}

	data := make(map[string]string)
	data["filePath"] = e.getValueFromEnvInputs(filePathEnvVar)
	if data["filePath"] == "" {
		return nil, 0, errors.Errorf("Failed to get file path property")
	}
	data["taskID"] = e.TaskID
	data["nodeName"] = e.NodeName
	data["jobID"] = jobIDStr

	return &prov.Action{ActionType: "heappe-filecontent-monitoring", Data: data}, e.MonitoringTimeInterval, err

}

// Execute executes a synchronous operation
func (e *DatasetTransferExecution) Execute(ctx context.Context) error {

	var err error
	switch strings.ToLower(e.Operation.Name) {
	case installOperation, "standard.create":
		// Nothing to do
	case uninstallOperation, "standard.delete":
		// Nothing to do
	case sendInputsOperation:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Sending input to job %q", e.NodeName)
		err = e.transferDataset(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Dataset transfer %q failed, error %s", e.NodeName, err.Error())
		}
	case getResultsOperation:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Getting results from job %q", e.NodeName)
		err = e.getResultFiles(ctx)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
				"Getting results from job %q failed, error %s", e.NodeName, err.Error())
		}
	case tosca.RunnableSubmitOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Submitting Job %q", e.NodeName)
	case tosca.RunnableCancelOperationName:
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, e.DeploymentID).Registerf(
			"Canceling Job %q", e.NodeName)
	default:
		err = errors.Errorf("Unsupported operation %q on dataset transfer", e.Operation.Name)
	}

	return err
}

// ResolveExecution resolves inputs and artifacts before the execution of an operation
func (e *DatasetTransferExecution) ResolveExecution(ctx context.Context) error {
	log.Debugf("Preparing execution of operation %q on node %q for deployment %q", e.Operation.Name, e.NodeName, e.DeploymentID)
	ovPath, err := operations.GetOverlayPath(e.Cfg, e.TaskID, e.DeploymentID)
	if err != nil {
		return err
	}
	e.OverlayPath = ovPath

	if err = e.resolveInputs(ctx); err != nil {
		return err
	}
	if err = e.resolveArtifacts(ctx); err != nil {
		return err
	}

	return err
}

func (e *DatasetTransferExecution) transferDataset(ctx context.Context) error {

	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	jobIDStr := e.getValueFromEnvInputs(jobIDEnvVar)
	if jobIDStr == "" {
		return errors.Errorf("Failed to get associated job ID")
	}
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s",
			jobIDStr, e.DeploymentID, e.NodeName)
		return err
	}

	// Get files to transfer
	fileNames, err := e.getDatasetFileNames(jobID)
	if err != nil {
		return err
	}

	// Get details on destination where to transfer files
	transferMethod, err := heappeClient.GetFileTransferMethod(jobID)
	if err != nil {
		return err
	}

	defer func() {
		_ = heappeClient.EndFileTransfer(jobID, transferMethod)
	}()

	clientConfig, err := getSSHClientConfig(transferMethod.Credentials.Username,
		transferMethod.Credentials.PrivateKey)
	if err != nil {
		return errors.Wrapf(err, "Failed to create a SSH client using HEAppE file transfer credentials")
	}

	client := scp.NewClient(transferMethod.ServerHostname+":22", &clientConfig)

	// Connect to the remote server
	err = client.Connect()
	if err != nil {
		return errors.Wrapf(err, "Failed to connect to remote server using HEAppE file transfer credentials")
	}
	defer client.Close()

	// Transfer each file in the dataset
	for _, fileName := range fileNames {

		f, err := os.Open(fileName)
		if err != nil {
			return errors.Wrapf(err, "Failed to open dataset file %s", fileName)
		}
		defer f.Close()

		baseName := filepath.Base(fileName)
		err = client.CopyFile(f, filepath.Join(transferMethod.SharedBasepath, baseName), "0744")
		if err != nil {
			return errors.Wrapf(err, "Failed to copy dataset file %s to remote server", baseName)
		}
	}

	return err
}

func (e *DatasetTransferExecution) getDatasetFileNames(jobID int64) ([]string, error) {

	var fileNames []string

	datasetFileName := e.Artifacts[zipDatasetArtifactName]
	if datasetFileName == "" {
		return fileNames, errors.Errorf("No dataset provided")
	}

	datasetAbsPath := filepath.Join(e.OverlayPath, datasetFileName)

	destDir := filepath.Join(e.OverlayPath, fmt.Sprintf("heappe_dataset_%d", jobID))
	os.RemoveAll(destDir)
	err := os.MkdirAll(destDir, 0700)
	if err != nil {
		return fileNames, err
	}

	fileNames, err = ziputil.Unzip(datasetAbsPath, destDir)
	if err != nil {
		err = errors.Wrapf(err, "Failed to unzip dataset")
	}

	return fileNames, err
}

func (e *DatasetTransferExecution) getResultFiles(ctx context.Context) error {

	// Get details on remote host where to get result files
	heappeClient, err := getHEAppEClient(ctx, e.Cfg, e.DeploymentID, e.NodeName)
	if err != nil {
		return err
	}

	jobIDStr := e.getValueFromEnvInputs(jobIDEnvVar)
	if jobIDStr == "" {
		return errors.Errorf("Failed to get associated job ID")
	}
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "Unexpected Job ID value %q for deployment %s node %s",
			jobIDStr, e.DeploymentID, e.NodeName)
		return err
	}

	// Get list of result files
	filenames, err := heappeClient.ListChangedFilesForJob(jobID)
	if err != nil {
		return err
	}

	log.Debugf("Result files for deployment %s node %s : %+v", e.DeploymentID, e.NodeName, filenames)

	if len(filenames) == 0 {
		// Nothing to do
		return err
	}

	transferMethod, err := heappeClient.GetFileTransferMethod(jobID)
	if err != nil {
		return err
	}

	defer func() {
		_ = heappeClient.EndFileTransfer(jobID, transferMethod)
	}()

	// go-scp is not yet providing the copy from remote
	// see pull request https://github.com/bramvdbogaerde/go-scp/pull/12
	// In the meantime using scp command
	// Creating the private key file
	pkeyFile := filepath.Join(e.OverlayPath, fmt.Sprintf("heappepkey_%d", jobID))
	err = ioutil.WriteFile(pkeyFile, []byte(transferMethod.Credentials.PrivateKey), 0400)
	if err != nil {
		return err
	}
	defer os.Remove(pkeyFile)

	copyDir := filepath.Join(e.OverlayPath, fmt.Sprintf("heappe_results_%d", jobID))
	archivePath := copyDir + ".zip"
	os.RemoveAll(copyDir)
	err = os.MkdirAll(copyDir, 0700)
	if err != nil {
		return err
	}

	defer os.RemoveAll(copyDir)

	// Transfer each file in the dataset
	for _, filename := range filenames {

		remotePath := filepath.Join(transferMethod.SharedBasepath, filename)
		localPath := filepath.Join(copyDir, filename)
		localDir := filepath.Dir(localPath)
		if localDir != "." {
			err = os.MkdirAll(localDir, 0700)
			if err != nil {
				return errors.Wrapf(err, "Failed to create local dir %s", localDir)
			}
		}
		copyCmd := exec.Command("/bin/scp", "-i", pkeyFile,
			"-o", "StrictHostKeyChecking=no",
			fmt.Sprintf("%s@%s:%s",
				transferMethod.Credentials.Username,
				transferMethod.ServerHostname,
				remotePath),
			localPath)
		stdoutStderr, err := copyCmd.CombinedOutput()
		if err != nil {
			log.Printf("Failed to copy remote file: %s - %s", err.Error(), string(stdoutStderr))
			return errors.Wrapf(err, "Failed to copy remote file %s", filename)
		}
		if len(stdoutStderr) > 0 {
			log.Debugf("Copy of %s: %s", filename, string(stdoutStderr))
		}
	}

	zippedContent, err := ziputil.ZipPath(copyDir)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(archivePath, zippedContent, 0700)
	if err != nil {
		return err
	}

	// Set the corresponding attribute
	err = deployments.SetAttributeForAllInstances(ctx, e.DeploymentID, e.NodeName,
		zipResultAttribute, archivePath)
	if err != nil {
		err = errors.Wrapf(err, "Failed to store attribute %s for deployment %s node %s",
			zipResultAttribute, e.DeploymentID, e.NodeName)
	}

	return err
}

func (e *DatasetTransferExecution) resolveInputs(ctx context.Context) error {
	var err error
	log.Debugf("Get environment inputs for node:%q", e.NodeName)
	e.EnvInputs, e.VarInputsNames, err = operations.ResolveInputsWithInstances(
		ctx, e.DeploymentID, e.NodeName, e.TaskID, e.Operation, nil, nil)
	log.Debugf("Environment inputs: %v", e.EnvInputs)
	return err
}

func (e *DatasetTransferExecution) resolveArtifacts(ctx context.Context) error {
	var err error
	log.Debugf("Get artifacts for node:%q", e.NodeName)
	e.Artifacts, err = deployments.GetFileArtifactsForNode(ctx, e.DeploymentID, e.NodeName)
	log.Debugf("Resolved artifacts: %v", e.Artifacts)
	return err
}

func getSSHClientConfig(username, privateKey string) (ssh.ClientConfig, error) {

	var clientConfig ssh.ClientConfig

	signer, err := ssh.ParsePrivateKey([]byte(privateKey))
	if err == nil {
		clientConfig = ssh.ClientConfig{
			User: username,
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(signer),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
	}

	return clientConfig, err
}

func (e *DatasetTransferExecution) getValueFromEnvInputs(envVar string) string {

	var result string
	for _, envInput := range e.EnvInputs {
		if envInput.Name == envVar {
			result = envInput.Value
			break
		}
	}
	return result

}
