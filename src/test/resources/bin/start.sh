#!/bin/sh
cd `dirname $0`/..
BASE_DIR="`pwd`"
CONFPATH=$BASE_DIR/conf
LOGPATH=$BASE_DIR/log

cd $BASE_DIR/lib/

for jar in `ls *.jar`
do
	JARPATH="$JARPATH:""$jar"
done
JAVA_OPTS="-server -Xms512m -Xmx512m -XX:NewSize=107m -XX:MaxNewSize=107m -XX:+UseConcMarkSweepGC -XX:+HeapDumpOnOutOfMemoryError"
export JAVA_OPTS

START_LOG=$LOGPATH/start.log
java $JAVA_OPTS -classpath $CONFPATH$JARPATH:. com.taobao.alexander.sequence.SequenceServer>> $START_LOG 2>&1 &

