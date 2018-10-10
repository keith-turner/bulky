ACCUMULO_VERSION="1.9.2"
HADOOP_VERSION=$(hadoop version | head -1 | cut -f 2 -d ' ')

#TODO ZK version
#TODO fail if command above fails

CLASSPATH=$(mvn -Daccumulo.version=$ACCUMULO_VERSION -Dhadoop.version=$HADOOP_VERSION -q exec:exec -Dexec.executable=echo -Dexec.args="%classpath")
CONF_DIR=$(readlink -f ./conf)
export CLASSPATH="$CONF_DIR:$HADOOP_CONF_DIR:$CLASSPATH"

