# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#  author: bregeon@in2p3.fr, June 2021
#

import os
import sys
import copy
import time
import logging
import subprocess
import re

from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from lsst.daf.butler import Butler, DimensionUniverse, ButlerURI
from lsst.pipe.base.graph import QuantumGraph

_LOG = logging.getLogger(__name__)


def get_job_id(name):
    try:
        job_id = int(name.split('_')[0])
    except:
        job_id = 0
    return job_id

def get_cmdline(gwf_job):
    """Command line for a GenericWorkflowJob."""
    cmd = ' '.join((gwf_job.executable.src_uri, gwf_job.arguments))
    newcmd = cmd
    for key in re.findall(r"<ENV:([^>]+)>", cmd):
        newcmd = newcmd.replace(rf"<ENV:{key}>", "${%s}" % key)
    return newcmd

class LocalService(BaseWmsService):
    """Local version of WMS service
    """

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an Local iDDS ready for submission

        Parameters
        ----------
        config : `~lsst.ctrl.bps.BPSConfig`
            BPS configuration that includes necessary submit/runtime information
        generic_workflow :  `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            The generic workflow (e.g., has executable name and arguments)
        out_prefix : `str`
            The root directory into which all WMS-specific files are written

        Returns
        ----------
        workflow : `~lsst.ctrl.bps.wms.Local.Local_service.LocalBpsWmsWorkflow`
            Local workflow ready to be run.
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        local_workflow = LocalBpsWmsWorkflow.from_generic_workflow(config, generic_workflow, out_prefix,
                                                             f"{self.__class__.__module__}."
                                                             f"{self.__class__.__name__}")
        # workflow.write(out_prefix)
        _LOG.info(60*"-")
        _LOG.info("Prepared a local workflow")
        # for job_id in local_workflow.local_job_ids[:10]:
        for job_id in local_workflow.local_ordered_job_ids[:20]:
            _LOG.info("Job %d is %s \n %s", job_id, local_workflow.local_jobs[job_id][0],
                                                    local_workflow.local_jobs[job_id][1])

        _LOG.info(60*"-")
        for job_id in local_workflow.local_job_ids[:20]:
            job_name = local_workflow.local_jobs[job_id][0]
            inputs = local_workflow.local_jobs_input_files[job_name]
            outputs = local_workflow.local_jobs_output_files[job_name]
            preds = local_workflow.local_predecessors[job_id]
            succs = local_workflow.local_successors[job_id]
            _LOG.info("Job: %s", job_name)
            _LOG.info("\t inputs: %s", inputs)
            _LOG.info("\t outputs: %s", outputs)
            _LOG.info("\t predecessors: %s", preds)
            _LOG.info("\t successors: %s", succs)
        return local_workflow

    def check_job_input(self, workflow, job_name):
        """ Check input files for a given job in Butler

        Parameters
        ----------
        job_name

        """
        butler = Butler(self.config['butlerConfig'], run=self.config['inCollection'])
        registry = butler.registry
        qgraph_file = workflow.local_jobs_input_files[job_name][0]
        quantum_graph = QuantumGraph.loadUri(qgraph_file, DimensionUniverse())
        _LOG.info(60 * "-")
        _LOG.info('%s %s %s', job_name, qgraph_file, quantum_graph)
        _LOG.info(' --- INPUTS ---')
        for node in quantum_graph.inputQuanta:
            _LOG.info('--- Node = %s', node)
            for dataset_refs in node.quantum.inputs.values():
                _LOG.info('Dataset %s', dataset_refs)
                for dataset_ref in dataset_refs:
                    _LOG.info('Type = %s', dataset_ref.datasetType)
                    _LOG.info('ID = %s', dataset_ref.dataId)
                    #_LOG.info('Butler Run = %s', butler.run)
                    ref = registry.findDataset(dataset_ref.datasetType,
                                               dataset_ref.dataId,
                                               collections=butler.run) # ['HSC/defaults', 'HSC/calib/unbounded']
                    if ref is not None:
                        _LOG.info('Butler ref = %s', ref)
                        uri = butler.getURI(ref, collections=butler.run) # ['HSC/defaults', 'HSC/calib/unbounded']
                        _LOG.info("URI = {}\n".format(uri))
                    else:
                        _LOG.info("No URI found")

    def check_if_job_has_outputs(self, workflow, job_name):
        """
        Use the repo butler to determine if a job's outputs are present.
        If any outputs are missing, return False.
        Code stolen from Jim Chiang
        """
        butler = Butler(self.config['butlerConfig'], run=self.config['outCollection'])
        registry = butler.registry
        qgraph_file = workflow.local_jobs_input_files[job_name][0]
        quantum_graph = QuantumGraph.loadUri(qgraph_file, DimensionUniverse())
        _LOG.info(60 * "-")
        _LOG.info('%s %s %s', job_name, qgraph_file, quantum_graph)
        for node in quantum_graph:
            for dataset_refs in node.quantum.outputs.values():
                for dataset_ref in dataset_refs:
                    ref = registry.findDataset(dataset_ref.datasetType,
                                               dataset_ref.dataId,
                                               collections=butler.run)
                    if ref is None:
                        return False
        return True

    def check_wkf_inputs(self, workflow):
        """ Check input files in Butler
        Parameters
        ----------
        workflow : `~lsst.ctrl.bps.wms_service.BaseWorkflow`
            A single basic workflow to submit
        """
        # input data
        _LOG.info(60 * "-")
        butler = Butler(self.config['butlerConfig'], run=self.config['inCollection'])
        registry = butler.registry
        # load quantum graph from pipetask init
        ptask_init_id = workflow.local_job_ids[0]
        ptask_init_name = workflow.local_jobs[ptask_init_id][0]
        qgraph_file = workflow.local_jobs_input_files[ptask_init_name][0]
        quantum_graph = QuantumGraph.loadUri(qgraph_file, DimensionUniverse())
        _LOG.info('%s %s %s', ptask_init_name, qgraph_file, quantum_graph)
        _LOG.info(' --- INPUTS ---')
        for node in quantum_graph.inputQuanta:
            _LOG.info('--- Node = %s', node)
            for dataset_refs in node.quantum.inputs.values():
                _LOG.info('Dataset %s', dataset_refs)
                for dataset_ref in dataset_refs:
                    _LOG.info('Type = %s', dataset_ref.datasetType)
                    _LOG.info('ID = %s', dataset_ref.dataId)
                    #_LOG.info('Butler Run = %s', butler.run)
                    ref = registry.findDataset(dataset_ref.datasetType,
                                               dataset_ref.dataId,
                                               collections=butler.run) #
                    if ref is not None:
                        _LOG.info('Butler = %s', ref)
                        uri = butler.getURI(ref, collections=butler.run)
                        _LOG.info("URI = {}\n".format(uri))
                    else:
                        _LOG.info("No URI found")

    def run_one_job(self, workflow, job_id, job_name, job_cmd):
        """ Run one job locally

        Parameters
        ----------
        workflow
        job_id
        job_name
        job_cmd
        """
        _LOG.info("Running Job %d is %s \n %s", job_id, job_name, job_cmd)
        _LOG.info("Job predecessors: %s", workflow.local_predecessors[job_id])
        # self.check_job_input(workflow, job_name)
        inputs_list_str = ""
        for one_input in workflow.local_jobs_input_files[job_name]:
            inputs_list_str = inputs_list_str + "%s " % one_input.strip("file:")[2:]
            copy_inputs_cmd = "cp -f %s ." % one_input.strip("file:")[2:]
            _LOG.info("CMD %s", copy_inputs_cmd)
            with subprocess.Popen([copy_inputs_cmd], stdout=subprocess.PIPE,
                                  shell=True) as proc:
                _LOG.info(proc.stdout.read())
        # prepare pipe wrapper
        pipe_wrapper = self.prepare_cmdline_wrapper(job_id, job_cmd)
        _LOG.info("\tRunning wrapper %s", pipe_wrapper)
        with subprocess.Popen(['./' + pipe_wrapper], stdout=subprocess.PIPE, shell=True) as proc:
            _LOG.info(proc.stdout.read())
        # ask butler if job outputs are present to determine if job is successful
        # Jim Chiang code
        return self.check_if_job_has_outputs(workflow, job_name)

    def submit(self, workflow, check_inputs=False, dry_run=False):
        """Submit a simple workflow locally

        Parameters
        ----------
        workflow : `~lsst.ctrl.bps.wms_service.BaseWorkflow`
            A single basic workflow to submit
        """
        # Always run pipetask init first
        ptask_init_id = workflow.local_job_ids[0]
        ptask_init_name = workflow.local_jobs[ptask_init_id][0]
        ptask_init_cmd = workflow.local_jobs[ptask_init_id][1]
        self.run_one_job(workflow, ptask_init_id, ptask_init_name, ptask_init_cmd)

        if check_inputs:
            _LOG.info("Check inputs in butler, but first run pipetask init")
            # now check if input data are available
            # self.check_wkf_inputs(workflow)
            # for job_id in workflow.local_job_ids[1:10]:
            for job_id in workflow.local_ordered_job_ids[0:5]:
                job_name = workflow.local_jobs[job_id][0]
                self.check_job_input(workflow, job_name)

        if not dry_run:
            _LOG.info("Submitting %s", workflow)
            _LOG.info("Smart job sequence")
            _LOG.info(workflow.local_smart_job_sequence)
            #for job_id in workflow.local_job_ids[1:10]:
            # for job_id in workflow.local_ordered_job_ids:
            for job_id in workflow.local_smart_job_sequence:
            # do not rerun task init
                if job_id > 0:
                    job_name = workflow.local_jobs[job_id][0]
                    job_cmd = workflow.local_jobs[job_id][1]
                    _LOG.info("Running Job %d is %s \n %s", job_id, job_name, job_cmd)
                    success_code = self.run_one_job(workflow, job_id, job_name, job_cmd)
                    _LOG.info("Return code for job %s is %d.", job_name, success_code)
                    if not success_code:
                        _LOG.error("%s failed, aborting.", job_name)
                        sys.exit(1)
        else:
            _LOG.info("Smart job sequence")
            _LOG.info(workflow.local_smart_job_sequence)

        # jobs_queue = copy.copy(workflow.local_ordered_job_ids)
        # running_jobs = list()
        # while len(jobs_queue)>0:
        #     for job_id in jobs_queue:
        #         job_name = workflow.local_jobs[job_id][0]
        #         # check predecessors as well
        #         if inputs_ready[job_name] and n_threads<4:
        #             local_id = submit_job[job_name]
        #             running_jobs.append((job_id, local_id))
        #             n_threads = n_threads + 1
        #     # update status
        #     for jobs in running_jobs:
        #         status = get_job_status(jobs[1])
        #         if status is "Done":
        #             running_jobs.remove(jobs)
        #             jobs_queue.remove(jobs[0])
        #     time.sleep(10)

    def prepare_cmdline_wrapper(self, job_id, cmdline):
        """Create a local job wrapper

        Parameters
        ----------
        cmdline : string
            Command line from generic_worklow job
        """
        _LOG.info("Preparing command wrapper")
        template = open(os.environ['CTRL_BPS_DIR'] + '/python/lsst/ctrl/bps/templates/pipetask_wrapper.sh').read()
        content = template.replace('PIPE_TASK_CMDLINE', '"%s"' % cmdline)
        wrapper_name = 'pipetask_wrapper_%d.sh' % job_id
        open(wrapper_name, 'w').write(content)
        # make it executable
        os.chmod(wrapper_name, 0o755)
        return wrapper_name


class LocalBpsWmsWorkflow(BaseWmsWorkflow):
    """
    A single Local based workflow
    Parameters
    ----------
    name : `str`
        Unique name for Workflow
    config : `~lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.generated_tasks = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        # Docstring inherited from parent class
        local_workflow = cls(generic_workflow.name, config)
        local_workflow.run_attrs = copy.deepcopy(generic_workflow.run_attrs)
        local_workflow.run_attrs['bps_wms_service'] = service_class
        local_workflow.run_attrs['bps_wms_workflow'] = f"{cls.__module__}.{cls.__name__}"

        # Create initial list of file objects for *all* files that WMS must handle
        local_workflow.local_files = dict()
        for gwf_file in generic_workflow.get_files(data=True, transfer_only=True):
            if gwf_file.wms_transfer:
                pfn = f"file://{gwf_file.src_uri}"
                local_workflow.local_files[gwf_file.name] = pfn
        _LOG.info("Generated tasks")
        _LOG.info(local_workflow.generated_tasks)

        # Add jobs to the list of local jobs, and associate input and output files
        local_workflow.local_jobs = dict()
        local_workflow.local_jobs_input_files = dict()
        local_workflow.local_jobs_output_files = dict()
        local_workflow.local_ordered_job_ids = list()
        for job_name in generic_workflow:
            gwf_job = generic_workflow.get_job(job_name)
            # input files as pfn
            gwf_job_inputs = list()
            for gwf_file in generic_workflow.get_job_inputs(gwf_job.name, data=True, transfer_only=True):
                gwf_job_inputs.append(local_workflow.local_files[gwf_file.name])
            # output files as pfn
            gwf_job_outputs = list()
            for gwf_file in generic_workflow.get_job_outputs(gwf_job.name, data=True, transfer_only=False):
                gwf_job_outputs.append(local_workflow.local_files[gwf_file.name])
            # store in members
            try:
                job_id = int(gwf_job.name.split('_')[0])
            except:
                job_id = 0
            local_workflow.local_ordered_job_ids.append(job_id)
            local_workflow.local_jobs[job_id] = (gwf_job.name, get_cmdline(gwf_job))
            local_workflow.local_jobs_input_files[job_name] = gwf_job_inputs
            local_workflow.local_jobs_output_files[job_name] = gwf_job_outputs

        # list of job ids ordered by number
        job_ids = list(local_workflow.local_jobs.keys())
        job_ids.sort()
        local_workflow.local_job_ids = job_ids

        # Save job dependencies
        local_workflow.local_predecessors = dict()
        local_workflow.local_successors = dict()
        for job_name in generic_workflow:
            job_id = get_job_id(job_name)
            childs = list()
            parents = list()
            for parent_name in generic_workflow.predecessors(job_name):
                id = get_job_id(parent_name)
                parents.append(id)
            for child_name in generic_workflow.successors(job_name):
                id = get_job_id(child_name)
                childs.append(id)
            local_workflow.local_predecessors[job_id] = parents
            local_workflow.local_successors[job_id] = childs

        # Build smart sequence
        all_jobs_queue = copy.copy(local_workflow.local_ordered_job_ids)
        all_jobs_queue.sort()
        smart_job_sequence = list()
        while len(all_jobs_queue)>0:
            for id in all_jobs_queue:
                print("working with ", id)
                ready = True
                print(local_workflow.local_predecessors[id])
                for parent in local_workflow.local_predecessors[id]:
                    print("\tparent ", parent)
                    is_done = parent in smart_job_sequence
                    ready = ready*is_done
                if ready:
                    print("adding ", id)
                    smart_job_sequence.append(id)
                    all_jobs_queue.remove(id)
        local_workflow.local_smart_job_sequence = smart_job_sequence
        # Done
        _LOG.debug("Local dag attribs %s", local_workflow.run_attrs)
        return local_workflow
