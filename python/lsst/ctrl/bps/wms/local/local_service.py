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
import copy
import logging
import binascii

from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
# from lsst.ctrl.bps.wms.Local.idds_tasks import IDDSWorkflowGenerator
from lsst.daf.butler import ButlerURI
# from idds.workflow.workflow import Workflow as IDDS_client_workflow
# from idds.doma.workflow.domalsstwork import DomaLSSTWork
# import idds.common.constants as idds_constants
# import idds.common.utils as idds_utils
# import PanDAtools.idds_api

_LOG = logging.getLogger(__name__)


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
        workflow = LocalBpsWmsWorkflow.from_generic_workflow(config, generic_workflow, out_prefix,
                                                             f"{self.__class__.__module__}."
                                                             f"{self.__class__.__name__}")
        # workflow.write(out_prefix)
        _LOG.info("Prepared a local workflow")
        for job in self.local_jobs:
            _LOG.info("Job: %s"%job.name)
        return workflow

    def submit(self, workflow):
        """Submit a simple workflow locally

        Parameters
        ----------
        workflow : `~lsst.ctrl.bps.wms_service.BaseWorkflow`
            A single basic workflow to submit
        """
        _LOG.debug("Submitting %s" % workflow)


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

        # Create initial Pegasus File objects for *all* files that WMS must handle
        self.local_files = dict()
        for gwf_file in generic_workflow.get_files(data=True, transfer_only=True):
            if gwf_file.wms_transfer:
                pfn = f"file://{gwf_file.src_uri}"
                self.local_files[gwf_file.name] = pfn

        # Add jobs to the list of local jobs, and associate input and output files
        self.local_jobs = list()
        self.local_jobs_input_files = dict()
        self.local_jobs_output_files = dict()
        for job_name in generic_workflow:
            gwf_job = generic_workflow.get_job(job_name)
            # input files as pfn
            gwf_job_inputs = list()
            for gwf_file in generic_workflow.get_job_inputs(gwf_job.name, data=True, transfer_only=True):
                 gwf_job_inputs.append(self.local_files[gwf_file.name])
            # output files as pfn
            gwf_job_outputs = list()
            for gwf_file in generic_workflow.get_job_outputs(gwf_job.name, data=True, transfer_only=True):
                 gwf_job_outputs.append(self.local_files[gwf_file.name])
            # store in members
            self.local_jobs.append(gwf_job)
            self.local_jobs_input_files[job_name] = gwf_job_inputs
            self.local_jobs_output_files[job_name] = gwf_job_outputs

        # Add job dependencies to the DAX.
        self.local_deps = dict()
        for job_name in generic_workflow:
            childs = list()
            for child_name in generic_workflow.successors(job_name):
                childs.append(child_name)
            self.local_deps[job_name] = childs
        _LOG.debug("Local dag attribs %s", local_workflow.run_attrs)
        return local_workflow
