notebook_start:
	docker run -p 8888:8888 jupyter/pyspark-notebook

exec_pyspark_container:
	 docker exec -it `docker container ls  --format='{{json .}}' | jq -r .ID` bash

flintrock_launch:
	flintrock login clo-spark-cluster

flintrock_login:
	flintrock login clo-spark-cluster
