#!/bin/bash
set -ex
[ -e /home/ec2-user/glue_ready ] && exit 0
sudo -u ec2-user -i <<'EOF'

ANACONDA_DIR=/home/ec2-user/anaconda3

# Create and Activate Conda Env
echo "INFO: Creating glue_pyspark conda enviornment"
conda create --name glue_pyspark ipykernel jupyterlab pandas=1.5.3 -y

echo "INFO: Activating glue_pyspark"
source activate glue_pyspark

# Initialize Latest Python Path
echo "INFO: Getting site packages directory of latest python version"
SITE_PACKAGES_DIR=$(python -c 'import site; print(site.getsitepackages()[0])')
echo "INFO: Site package directory is --- $SITE_PACKAGES_DIR"

# Install Glue Sessions to Env
echo "INFO: Installing AWS Glue Sessions with pip"
pip install aws-glue-sessions==1.0.0

# Check if the 'glue_pyspark' environment is active and packages are properly installed
if conda list -n glue_pyspark aws-glue-sessions | grep -q aws-glue-sessions; then
  echo "INFO: Package 'aws-glue-sessions' is installed in the 'glue_pyspark' environment successfully"
else
  echo "ERROR: Package 'aws-glue-sessions' is not installed in the 'glue_pyspark' environment or 'glue_pyspark' was not created properly. Notebook will go into 'Failed' state. Contact Admin."
  sleep 500
fi

# Clone glue_pyspark to glue_scala. This is required to match kernel naming conventions to their environments and can't have two kernels in one conda env.
echo "INFO: Cloning glue_pyspark to glue_scala"
conda create --name glue_scala --clone glue_pyspark -y

# Check if the 'glue_scala' environment is active and packages are properly installed
if conda list -n glue_scala aws-glue-sessions | grep -q aws-glue-sessions; then
  echo "INFO: Package 'aws-glue-sessions' is installed in the 'glue_scala' environment successfully"
else
  echo "ERROR: Package 'aws-glue-sessions' is not installed in the 'glue_scala' environment or 'glue_scala' was not created properly. Notebook will go into 'Failed' state. Contact Admin."
  sleep 500
fi

# Remove python3 kernel from glue_pyspark and glue_scala
echo "INFO: Removing python3 kernel from glue_pyspark and glue_scala"
rm -r ${ANACONDA_DIR}/envs/glue_pyspark/share/jupyter/kernels/python3
rm -r ${ANACONDA_DIR}/envs/glue_scala/share/jupyter/kernels/python3

# Copy kernels to Jupyter kernel env (Discoverable by conda_nb_kernel)
echo "INFO: Copying Glue PySpark Kernel"
cp -r ${SITE_PACKAGES_DIR}/aws_glue_interactive_sessions_kernel/glue_pyspark/ ${ANACONDA_DIR}/envs/glue_pyspark/share/jupyter/kernels/glue_pyspark/

echo "INFO: Copying Glue Spark Kernel"
mkdir ${ANACONDA_DIR}/envs/glue_scala/share/jupyter/kernels
cp -r ${SITE_PACKAGES_DIR}/aws_glue_interactive_sessions_kernel/glue_spark/ ${ANACONDA_DIR}/envs/glue_scala/share/jupyter/kernels/glue_spark/

echo "INFO: Changing Jupyter kernel manager from EnvironmentKernelSpecManager to CondaKernelSpecManager"
JUPYTER_CONFIG=/home/ec2-user/.jupyter/jupyter_notebook_config.py

sed -i '/EnvironmentKernelSpecManager/ s/^/#/' ${JUPYTER_CONFIG}
echo "c.CondaKernelSpecManager.name_format='conda_{environment}'" >> ${JUPYTER_CONFIG}
echo "c.CondaKernelSpecManager.env_filter='anaconda3$|JupyterSystemEnv$|/R$'" >> ${JUPYTER_CONFIG}

EOF

set -ex

# OVERVIEW
# This script stops a SageMaker notebook once it's idle for more than 1 hour (default time)
# If you want the notebook the stop only if no browsers are open, remove the --ignore-connections flag
#
# Note that this script will fail if either condition is not met
#   1. Ensure the Notebook Instance has internet connectivity to fetch the example config
#   2. Ensure the Notebook Instance execution role permissions to SageMaker:StopNotebookInstance to stop the notebook
#       and SageMaker:DescribeNotebookInstance to describe the notebook.
#

# PARAMETERS
IDLE_TIME=3600

echo "Fetching the autostop script"
wget https://raw.githubusercontent.com/aws-samples/amazon-sagemaker-notebook-instance-lifecycle-config-samples/master/scripts/auto-stop-idle/autostop.py


echo "Detecting Python install with boto3 install"

# Find which install has boto3 and use that to run the cron command. So will use default when available
# Redirect stderr as it is unneeded
CONDA_PYTHON_DIR=$(source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv && which python)
if $CONDA_PYTHON_DIR -c "import boto3" 2>/dev/null; then
    PYTHON_DIR=$CONDA_PYTHON_DIR
elif /usr/bin/python -c "import boto3" 2>/dev/null; then
    PYTHON_DIR='/usr/bin/python'
else
    # If no boto3 just quit because the script won't work
    echo "No boto3 found in Python or Python3. Exiting..."
    exit 1
fi

echo "Found boto3 at $PYTHON_DIR"


echo "Starting the SageMaker autostop script in cron"

(crontab -l 2>/dev/null; echo "*/5 * * * * $PYTHON_DIR $PWD/autostop.py --time $IDLE_TIME --ignore-connections >> /var/log/jupyter.log") | crontab -

systemctl restart jupyter-server
sudo touch /home/ec2-user/glue_ready