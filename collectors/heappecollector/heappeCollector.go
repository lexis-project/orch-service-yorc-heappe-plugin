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

package heappecollector

import (
	"context"
	"encoding/json"
	"time"

	"github.com/lexis-project/yorc-heappe-plugin/collectors"
	"github.com/lexis-project/yorc-heappe-plugin/heappe"
	"github.com/pkg/errors"

	"github.com/ystia/yorc/v4/config"
	"github.com/ystia/yorc/v4/locations"
	"github.com/ystia/yorc/v4/log"
)

const (
	// UserQueryParameter is the parameter specifying the user for which to provide
	// Infrastrcuture usage (by default, the user provided in the location will be used)
	UserQueryParameter = "user"
	// StartTimeQueryParameter is the parameter sepcifying the start time of
	// Infrastructure usage report
	StartTimeQueryParameter = "start"
	// EndTimeQueryParameter is the parameter sepcifying the end time of
	// Infrastructure usage report
	EndTimeQueryParameter = "end"
	infrastructureType    = "heappe"
)

// ClustersUsage defines the structure of the collected info
type ClustersUsage map[string][]heappe.ClusterNodeUsage

type heappeUsageCollectorDelegate struct {
}

// NewInfraUsageCollectorDelegate creates a new slurm infra usage collector delegate for specific slurm infrastructure
func NewInfraUsageCollectorDelegate() collectors.InfraUsageCollectorDelegate {
	return &heappeUsageCollectorDelegate{}
}

// CollectInfo allows to collect usage info about defined infrastructure
func (h *heappeUsageCollectorDelegate) CollectInfo(ctx context.Context, cfg config.Configuration,
	taskID, locationName string, params map[string]string) (map[string]interface{}, error) {

	locationMgr, err := locations.GetManager(cfg)
	if err != nil {
		return nil, err
	}

	locationProps, err := locationMgr.GetLocationProperties(locationName, infrastructureType)
	if err != nil {
		return nil, err
	}

	heappeClient, err := heappe.GetClient(locationProps, "YorcUser", "", nil)
	if err != nil {
		return nil, err
	}

	var bytesVal []byte
	if len(params) == 0 {
		bytesVal, err = getClustersNodeUsage(heappeClient)
		if err != nil {
			return nil, err
		}
	} else {
		bytesVal, err = getUserInfraUsage(heappeClient, locationProps, params)
		if err != nil {
			return nil, err
		}
	}

	/*

	 */
	var result map[string]interface{}
	err = json.Unmarshal(bytesVal, &result)

	if err != nil {
		log.Printf("Got error unmarshaling result: %+s\n", err.Error())

	}
	return result, err
}

func getClustersNodeUsage(client heappe.Client) ([]byte, error) {

	// First get IDs of nodes in the cluster
	clusters, err := client.ListAvailableClusters()
	if err != nil {
		return nil, err
	}

	clustersUsage := make(ClustersUsage, len(clusters))
	for _, cluster := range clusters {
		var nodesUsage []heappe.ClusterNodeUsage
		for _, nodeType := range cluster.NodeTypes {
			nodeUsage, err := client.GetCurrentClusterNodeUsage(nodeType.ID)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to get usage for node type %s on cluster %s",
					cluster.Name, nodeType.Name)
			}
			nodesUsage = append(nodesUsage, nodeUsage)
		}
		clustersUsage[cluster.Name] = nodesUsage
	}

	return json.Marshal(clustersUsage)
}

func getUserID(client heappe.Client, userName string) (int64, error) {
	var userID int64

	adaptorUserGroups, err := client.ListAdaptorUserGroups()
	if err != nil {
		return userID, err
	}
	for _, adaptorUserGroup := range adaptorUserGroups {
		for _, adaptorUser := range adaptorUserGroup.Users {
			if adaptorUser.Username == userName {
				return adaptorUser.ID, err
			}
		}
	}

	return userID, errors.Errorf("Found no user with name %s", userName)
}

func getUserInfraUsage(client heappe.Client, locationProps config.DynamicMap, params map[string]string) ([]byte, error) {
	// Getting the user for which to provide a usage report
	userName := params[UserQueryParameter]
	if userName == "" {
		userName = locationProps.GetString(heappe.LocationUserPropertyName)
		if userName == "" {
			return nil, errors.Errorf("No user defined in location")
		}
	}

	// Gettting the corresponding user ID from HEAppE
	userID, err := getUserID(client, userName)
	if err != nil {
		return nil, err
	}

	endTime := params[EndTimeQueryParameter]
	if endTime == "" {
		endTime = time.Now().Format(time.RFC3339)
	}
	startTime := params[StartTimeQueryParameter]
	if startTime == "" {
		return nil, errors.Errorf("Missing parameter %q in query to get infrastructure usage from a given start time", StartTimeQueryParameter)
	}
	report, err := client.GetUserResourceUsageReport(userID, startTime, endTime)
	if err != nil {
		return nil, err
	}

	return json.Marshal(report)
}
