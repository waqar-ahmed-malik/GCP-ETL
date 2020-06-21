#!/bin/bash 
################################################
#Script Name	:copy_dataset.sh               #                                                                               
#Description	:This bashscript is to copy all#                                                                                 
#the tables in a dataset passed in the aaray.  #                                                                                       
#Author       	:Ramneek kaur                  #                          
#Date         	:6/20/2019                     #                   
################################################

declare -a datasetArray=(
    "CUSTOMER_PRODUCT" 
	"REFERENCE" 
	"OPERATIONAL"
)
for dataset in "${datasetArray[@]}";do
    echo "Dataset Name :""$dataset"
	export SOURCE_DATASET="aaa-mwg-dwprod:"$dataset""
	echo $SOURCE_DATASET
	export DEST_PREFIX="aaa-mwg-dwuat:"$dataset""
	echo $DEST_PREFIX
	for f in `bq ls -n 200 $SOURCE_DATASET |grep TABLE | awk '{print $1}'`
	do
		export CLONE_CMD="bq --nosync cp -f $SOURCE_DATASET.$f $DEST_PREFIX.$f"
		echo $CLONE_CMD
        echo '**starting copy of tables** '
		echo `$CLONE_CMD`
	done
	echo $?
done
echo '**All tables are successfully copied for 3 datasets from project PROD to project UAT.** '