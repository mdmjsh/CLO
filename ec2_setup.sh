sudo yum install python36 python36-pip
flintrock run-command clo-spark-cluster 'sudo yum install -y python36 python36-pip'

# Set pyspark to use python3 by default
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' spark/conf/spark-env.sh

# jupyter setup - https://raw.githubusercontent.com/pzfreo/ox-clo/master/code/flintrock-jupyter.sh
sudo yum install gcc gcc-c++ -y
sudo pip install jupyter
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser'
pyspark --master spark://0.0.0.0:7077 --packages  org.apache.hadoop:hadoop-aws:2.7.4

# Tunnel cluster to local machine for jupyter
lsof -ti:8888 | xargs kill -9
ssh -i spark_cluster.pem -4 -fN -L 8888:localhost:8888 ec2-user@ec2-34-250-89-10.eu-west-1.compute.amazonaws.com