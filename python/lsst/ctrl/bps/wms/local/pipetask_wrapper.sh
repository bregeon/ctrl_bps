#!/usr/bin/env bash
echo "Cleaning environment $CLEANED_ENV"
[ -z "$CLEANED_ENV" ] && exec /bin/env -i CLEANED_ENV="Done" HOME=${HOME} SHELL=/bin/bash /bin/bash -l "$0" "$@"

# setup lsst distrib
RUBIN_VER="20"
source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2021_${RUBIN_VER}/loadLSST.bash
setup lsst_distrib

# setup HSC pipeline
# PIPE_WORK_DIR="/sps/lsstcest/users/bregeon/pipe_test"
PIPE_WORK_DIR="/home/bregeon/Rubin/pipe/work/"

cd $PIPE_WORK_DIR/testdata_ci_hsc
setup -j -r .
cd -

cd $PIPE_WORK_DIR/ci_hsc_gen3
setup -j -r .
cd -

# run command line
cmdline=PIPE_TASK_CMDLINE
echo $cmdline
eval $cmdline

# check return code, print result, and return code
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Error"
else
    echo "Success"
fi
exit $retVal