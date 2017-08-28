#/bin/bash

CURRENT_DIR=$(cd $(dirname $0) && pwd)
RESOURCE_DIR="src/main/resources"
DATA=(data hive)

for data in ${DATA[@]}; do
    # echo $data
    hdfs dfs -mkdir $data
    for find_file in `\find $CURRENT_DIR/$RESOURCE_DIR/$data -maxdepth 1 -type f`; do
        hdfs dfs -put $find_file $data
    done
done

