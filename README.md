# Yorc HEAppE plugin

The Yorc HEAppE plugin implements a Yorc ([Ystia orchestrator](https://github.com/ystia/yorc/)) plugin as described in [Yorc documentation](https://yorc.readthedocs.io/en/latest/plugins.html), allowing the orchestrator to use the HEappE (([High-End Application Execution](http://heappe.eu)) API to manage jobs executions on HPC infrastructures.

## Acknowledgement

This code repository is a result / contains results of the LEXIS project. The project has received funding from the European Union’s Horizon 2020 Research and Innovation programme (2014-2020) under grant agreement No. 825532.

## To build this plugin

You need first to have a working [Go environment](https://golang.org/doc/install).
Then to build, execute the following instructions:

```
mkdir -p $GOPATH/src/lexis-project
cd $GOPATH/src/lexis-project
git clone https://github.com/lexis-project/yorc-heappe-plugin
cd yorc-heappe-plugin
make
```

The plugin is then available at `bin/heappe-plugin`.

## TOSCA components

This plugin provides the following TOSCA components defined in the TOSCA file [a4c/ddi-types-a4c.yaml](a4c/heappe-types-a4c.yaml)
that can be uploaded in [Alien4Cloud](https://alien4cloud.github.io/) catalog of TOSCA components:

### org.lexis.common.heappe.nodes.Job
HEAppE job implementing the standard operations create, submit, run, cancel, delete.
Custom operations are also implemented to provide the correspondind HEAppE API features:
* enable_file_transfer: Enables files transfer to/from the job
* disable_file_transfer: Disables files transfer to/from the job
* list_changed_files: lists the files created/updated by the job, and their modification date.
The list of files created/updated by the job is provided by this component attribute `changed_files`.

### org.lexis.common.heappe.nodes.JobWithRuntimeTaskParameters
HEAppE job for which the task parameters are not properties configured before the deployment,
but are attributes defined at runtime by another TOSCA component.

### org.lexis.common.heappe.nodes.WaitFileAndGetContentJob
Component associated to a HEAppE job, allowing to wait for a given file to be generated by the job,
and to get the content of this file. The content of the file is provide by this component attribute `filecontent`.

## Licensing

This plugin is licensed under the [Apache 2.0 License](LICENSE).

