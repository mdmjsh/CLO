notebook_start:
	docker run -p 8888:8888 jupyter/pyspark-notebook

exec_pyspark_container:
	 docker exec -it `docker container ls  --format='{{json .}}' | jq -r .ID` bash

MASTER_IP=[ec2-master-ip]

NODES=10

CLUSTER=clo-spark-cluster-lt-${NODES}-node
# CLUSTER=clo-spark-cluster-lt-q2-4-node

flintrock_launch:
	flintrock launch ${CLUSTER} --num-slaves ${NODES}
	flintrock run-command ${CLUSTER} 'sudo yum update -y'
	flintrock run-command ${CLUSTER} 'sudo yum -y install python36 python36-pip'
	flintrock run-command ${CLUSTER} 'sudo pip install virtualenvwrapper'
	flintrock copy-file ${CLUSTER} clo_bashrc /home/ec2-user/.bashrc
	flintrock copy-file ${CLUSTER} part_1.ipynb /home/ec2-user/part_1.ipynb
	flintrock copy-file ${CLUSTER} part_2.py /home/ec2-user/part_2.py
	flintrock copy-file ${CLUSTER} requirements.txt /home/ec2-user/requirements.txt
	flintrock copy-file ${CLUSTER} chicago.shp /home/ec2-user/chicago.shp
	flintrock copy-file ${CLUSTER} chicago.dbf /home/ec2-user/chicago.dbf
	flintrock copy-file ${CLUSTER} chicago.prj /home/ec2-user/chicago.prj
	flintrock copy-file ${CLUSTER} chicago.shx /home/ec2-user/chicago.shx
	flintrock run-command ${CLUSTER} 'source .bashrc'
	flintrock run-command ${CLUSTER} 'mkvirtualenv clo --python=`which python3`'
	flintrock run-command ${CLUSTER} 'workon clo'
	flintrock run-command ${CLUSTER} 'sudo yum install gcc gcc-c++ -y'
	# https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html
	flintrock run-command ${CLUSTER} 'curl -O https://bootstrap.pypa.io/get-pip.py && python3 get-pip.py --user'
	flintrock run-command ${CLUSTER} 'workon clo && pip3.6 install pip install ipykernel'
	flintrock run-command ${CLUSTER} 'workon clo && pip3.6 install jupyter'
	flintrock run-command ${CLUSTER} 'workon clo && pip3.6 install -r requirements.txt'
	# create jupyter python 3 kernel - https://ipython.readthedocs.io/en/6.5.0/install/kernel_install.html#kernels-for-different-environments
	flintrock run-command ${CLUSTER}  'workon clo && sudo `which python` -m ipykernel install --name "python3"'
	flintrock run-command ${CLUSTER} 'workon clo &&  export PYSPARK_PYTHON=`which python`'
	flintrock run-command ${CLUSTER} 'echo "pyspark --master spark://0.0.0.0:7077 --packages  org.apache.hadoop:hadoop-aws:2.7.4" > run.sh && chmod +x run.sh'



CORES = 1 2 4 8
run_q2:
	# $(foreach var,$(CORES), echo $(var);)
	# Set pyspark executor to the system python, not jupyter
	$(foreach core,$(CORES), flintrock run-command ${CLUSTER} 'workon clo && export PYSPARK_DRIVER_PYTHON=`which python` \
	&& export PYSPARK_DRIVER_PYTHON_OPTS=`which python` \
	&& spark-submit --master spark://ec2-34-244-39-108.eu-west-1.compute.amazonaws.com:7077 part_2.py \
	--packages org.apache.hadoop:hadoop-aws:2.7.4 part_2.py --executor-cores $(core)';)


flintrock_login:
	flintrock login ${CLUSTER}

flintrock_destroy:
	flintrock destroy ${CLUSTER}

make tunnel:
	lsof -ti:8888 | xargs kill -9
	ssh -i spark_cluster.pem -4 -fN -L 8888:localhost:8888 [ec2-master-url]


