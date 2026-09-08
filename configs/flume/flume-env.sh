export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}"

export FLUME_HOME="${FLUME_HOME:-/opt/flume}"

export FLUME_CONF_DIR="${FLUME_CONF_DIR:-${FLUME_HOME}/conf}"

export FLUME_LOG_DIR="${FLUME_LOG_DIR:-${FLUME_HOME}/logs}"

export FLUME_PID_DIR="${FLUME_PID_DIR:-${FLUME_HOME}/run}"

export FLUME_TMP_DIR="${FLUME_TMP_DIR:-${FLUME_HOME}/tmp}"

export FLUME_OPTS="${FLUME_OPTS:--Xms256m -Xmx512m}"

export JAVA_OPTS="${JAVA_OPTS:--Xms256m -Xmx512m}"

export FLUME_AGENT_NAME="${FLUME_AGENT_NAME:-agent}"

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"

export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*}"

export HADOOP_OPTS="${HADOOP_OPTS:--Djava.library.path=${HADOOP_HOME}/lib/native}"

export LANG="${LANG:-C.UTF-8}"

export LC_ALL="${LC_ALL:-C.UTF-8}"