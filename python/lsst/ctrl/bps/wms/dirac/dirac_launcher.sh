#!/usr/bin/env bash
echo "Cleaning environment $CLEANED_ENV"
[ -z "$CLEANED_ENV" ] && exec /bin/env -i CLEANED_ENV="Done" HOME=${HOME} SHELL=/bin/bash /bin/bash -l "$0" "$@"

# source Dirac
source /home/bregeon/Rubin/pipe/myDirac/bashrc

# launch Dirac job
python pipe_job_launcher.py "$@"
