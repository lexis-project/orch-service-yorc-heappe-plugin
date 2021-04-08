#!/usr/bin/env bash
# Copyright 2020 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export GO111MODULE=on
export CGO_ENABLED=0

build: format
	@echo "--> Running go build"
	@mkdir -p ./bin
	@go build -o bin/heappe-plugin
	@echo "--> Embedding HEAppE types in binary"
	@rm -Rf ./build
	@mkdir ./build
	@zip -q -r ./build/embeddedResources.zip ./tosca/heappe-types.yaml
	@cat ./build/embeddedResources.zip >> ./bin/heappe-plugin
	@zip -A ./bin/heappe-plugin > /dev/null

format:
	@echo "--> Running go fmt"
	@go fmt ./...

.PHONY: build format
