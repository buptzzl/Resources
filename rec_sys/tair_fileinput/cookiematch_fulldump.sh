#!/bin/bash

#----------------------------------------------------------------------------
# 将cookiematch全量数据从hadoop中导出，并导入到tair中
# @version 1.0.1
# @author lishitong <lishitong@emar.com.cn>
# @since 1.0.1
# @change
# 2012.11.20
#----------------------------------------------------------------------------

#set -vx
HADOOP_HOME=/dmp/hadoop/hadoop-1.0.3/
HADOOP_ExE=$HADOOP_HOME/bin/hadoop
HIVE_HOME=/dmp/hive/hive-0.9.0
hive=${HIVE_HOME}/bin/hive
COOKIEMATCH_OUTPUT=/inf/rtb/tmp/cookiematch
IMPORT_EXEC=/dmp/bin/runTair.sh

##
## get last nday
##
##
function get_lastnday()
{
        d=`date -d "$2 $1 days ago" +%Y%m%d`;
        echo $d;
}


if [ $# -eq 1 ];
then
        DATE=$1
else
        DATE=`get_lastnday 1`
fi

## cookie match dump
## 
##
function cookiematch_dump()
{
    $hive <<EOF 
insert overwrite local directory '$COOKIEMATCH_OUTPUT' 
select  user_id,orig_plat_type,plat_user_id
from i_base_user
where day_id = $DATE;
EOF

}

##
## cookiematch导出
##
cookiematch_dump;

if [ $? -ne 0 ]
then
        echo "dump error";
	exit;
else
	echo "dump ok";
	touch $COOKIEMATCH_OUTPUT/dump.success
fi


##
## dump OK,生成success文件
##
if [ ! -f $COOKIEMATCH_OUTPUT/dump.success ];
then
        echo "no success file[$COOKIEMATCH_OUTPUT/dump.success], dump cookiematch file not success!";
        exit;
fi

##
## 执行tair导入程序
##
if [ ! -s $IMPORT_EXEC ];
then
        echo "$IMPORT_EXEC not found !";
        exit;
else
	/bin/bash $IMPORT_EXEC
fi


if [ $? -ne 0 ]
then
        echo "cookie match full dump false!";
else
	echo "cookie match full dump OK!"
fi
