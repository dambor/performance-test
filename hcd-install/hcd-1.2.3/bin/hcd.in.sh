#!/bin/sh

contains() {
    string="$1"
    substring="$2"
    if test "${string#*$substring}" != "$string"
    then
        return 0    # $substring is in $string
    else
        return 1    # $substring is not in $string
    fi
}

sanitize_classpath() {
    result=$(echo "$1" | sed 's/^\(:\)*\(.*\)\(:\)*$/\2/')
    result=$(echo "$result" | sed 's/:*$//')
    result=$(echo "$result" | sed -E 's/::+/:/g')
    echo $result
}

remove_duplicates() {
    result=$(echo "$1" | awk -v RS=':' -v ORS=":" '!a[$1]++{if (NR > 1) printf ORS; printf $a[$1]}')
    result=$(sanitize_classpath "$result")
    echo $result
}

# Parses cassandra-env.sh to find JMX_PORT set there as we do not need anything else from that file when running tools
get_jmx_port() {
    local env_file
    if [ -f "$CASSANDRA_ENV_FILE" ]; then
        env_file="$CASSANDRA_ENV_FILE"
    elif [ -f "$CASSANDRA_CONF/cassandra-env.sh" ]; then
        env_file="$CASSANDRA_CONF/cassandra-env.sh"
    else
        echo "Can't find $CASSANDRA_ENV_FILE, assuming default JMX port for HCD: 7199" 1>&2
        echo "7199"
        return 1
    fi

    local jmx_port_line="$(grep '\s*JMX_PORT=' "$env_file" | xargs echo)"
    if [ -n "$jmx_port_line" ]; then
        echo "$jmx_port_line" | cut -f 2 -d '='
    fi
}

if [ -n "$2" ]; then
for PARAM in "$@"; do
   if [ "$PARAM" = "$1" ]; then
      continue
   fi

   if contains "$PARAM" "cassandra.username" ; then
      DS_CREDENTIALS_SUPPLIED="1"
   fi
done
fi

HCD_CMD="$1"

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ "x$JAVA_HOME" != "x" -a -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
elif [ "`uname`" = "Darwin" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home)
    if [ -x "$JAVA_HOME/bin/java" ]; then
      JAVA="$JAVA_HOME/bin/java"
    fi
else
    JAVA="`which java`"
    if [ "x$JAVA" = "x" -a -x "/usr/lib/jvm/default-java/bin/java" ]; then
        # Use the default java installation
        JAVA="/usr/lib/jvm/default-java/bin/java"
    fi
    if [ "x$JAVA" != "x" ]; then
        JAVA=$(readlink -f "$JAVA")
        export JAVA_HOME=$(echo "$JAVA" | sed "s:bin/java::")
    fi
fi
export JAVA

if [ "x$JAVA" = "x" ]; then
    echo "Java executable not found (hint: set JAVA_HOME)" >&2
    exit 1
fi

# Helper functions
filematch () { case "$2" in $1) return 0 ;; *) return 1 ;; esac ; }

#########################################
# Setup HCD env
#########################################

if [ -z "$TMPDIR" ]; then
    # TMPDIR env variable is not set. HCD will use /tmp as a temporary files location
    export TMPDIR="/tmp"
fi

if [ ! -d "$TMPDIR" ]; then
    mkdir -m 1777 -p "$TMPDIR"
    if [ ! -d "$TMPDIR" ]; then
        echo "Error: Temporary location $TMPDIR does not exist and could not be created" 1>&2
        exit 1
    fi
fi

if [ ! -w "$TMPDIR" ]; then
    echo "Error: Temporary location $TMPDIR is not writable" 1>&2
    exit 1
fi

if [ -z "$HCD_HOME" ]; then
    abspath=$(cd "$(dirname $0)" && pwd -P)
    HCD_HOME="`dirname "$abspath"`"
    if [ -x "$HCD_HOME/bin/hcd" ]; then
        export HCD_HOME
    elif [ -x "/usr/bin/hcd" ]; then
        export HCD_HOME="/usr"
    elif [ -x "/usr/share/hcd/cassandra" ]; then
        export HCD_HOME="/usr/share/hcd"
    elif [ -x "/usr/share/hcd/bin/hcd" ]; then
        export HCD_HOME="/usr/share/hcd"
    elif [ -x "/opt/hcd/bin/hcd" ]; then
        export HCD_HOME="/opt/hcd"
    else
        DIR="`dirname $0`"
        for i in 1 2 3 4 5 6; do
            if [ -x "$DIR/bin/hcd" ]; then
                export HCD_HOME="$DIR"
                break
            fi
            DIR="$DIR/.."
        done
        if [ ! -x "$HCD_HOME/bin/hcd" ]; then
            echo "Cannot determine HCD_HOME."
            exit 1
        fi
    fi
fi

if [ -z "$HCD_CONF" ]; then
    for dir in "$HCD_HOME/resources/hcd/conf" \
               "$HCD_HOME/conf" \
               "/etc/hcd" \
               "/usr/share/hcd" \
               "/usr/share/hcd/conf" \
               "/usr/local/share/hcd" \
               "/usr/local/share/hcd/conf" \
               "/opt/hcd/conf"; do
        if [ -r "$dir/hcd.yaml" ]; then
            export HCD_CONF="$dir"
            break
        fi
    done
    if [ -z "$HCD_CONF" ]; then
        echo "Cannot determine HCD_CONF."
        exit 1
    fi
fi

#include the HCDRC environment script (pulls in credentials for basic authentication)
if [ -r "$HCD_HOME/bin/hcdrc-env.sh" ]; then
    . "$HCD_HOME/bin/hcdrc-env.sh"
elif [ -r "$HCD_CONF/hcdrc-env.sh" ]; then
    . "$HCD_CONF/hcdrc-env.sh"
else
    echo "Location pointed by HCDRC_ENV not readable: $HCDRC_ENV"
    exit 1
fi


if [ -z "$HCD_LOG_ROOT" ]; then
    HCD_LOG_ROOT_DEFAULT="/var/log"
    if [ -w "$HCD_HOME/logs" ]; then
        export HCD_LOG_ROOT="$HCD_HOME/logs"
    else
        export HCD_LOG_ROOT="$HCD_LOG_ROOT_DEFAULT"
    fi
fi

if [ -z "$HCD_LIB" ]; then
    for dir in "$HCD_HOME/build" \
               "$HCD_HOME/lib" \
               "$HCD_HOME/resources/hcd/lib" \
               "/usr/share/hcd" \
               "/usr/share/hcd/common" \
               "/opt/hcd" \
               "/opt/hcd/common"; do

        if [ -r "$dir" ]; then
            export HCD_LIB="$HCD_LIB
                            $dir"
        fi
    done
fi

#
# Add HCD jars to the classpath
#
for dir in $HCD_LIB; do
    for jar in "$dir"/*.jar; do
        if [ -r "$jar" ]; then
            HCD_CLASSPATH="$HCD_CLASSPATH:$jar"
        fi
    done

    for jar in "$dir"/hcd*.jar; do
        if [ -r "$jar" ]; then
            found_hcd_jars="$found_hcd_jars:$jar"
        fi
    done
done

export HCD_JARS="$found_hcd_jars"

# check if there are jars from older/other HCD versions in the classpath
HCD_JARS_ERROR_CHECK=$(echo $HCD_JARS | \
    tr ':' '\n' | \
    ( while read jar; do basename "$jar" ; done ) | \
    awk -v dirs="$(echo "$HCD_LIB" | tr '\n' ' ' )" '
        BEGIN { old_hcd = 0; hcd_core = 0 }
        /^hcd-[0-9]/ { old_hcd++ }
        /^hcd\.jar/ { old_hcd++ }
        /^hcd-core-[0-9]/ { hcd_core++ }
        END {
            if (old_hcd != 0) {
                printf "Found HCD jar from an older HCD version in %s. Please remove it.", dirs
            } else if (hcd_core == 0) {
                printf "Found no HCD core jar files in %s. Please make sure there is one.", dirs
            } else if (hcd_core > 1) {
                printf "Found multiple HCD core jar files in %s. Please make sure there is only one.", dirs
            }
        }
    ' )

if [ ! -z "$HCD_JARS_ERROR_CHECK" ]; then
    echo $HCD_JARS_ERROR_CHECK
    exit 1
fi

if [ -r $HCD_HOME/build/classes ]; then
    HCD_CLASSPATH="$HCD_HOME/build/classes:$HCD_CLASSPATH"
fi

if [ -r $HCD_HOME/build/classes/main ]; then
    HCD_CLASSPATH="$HCD_HOME/build/classes/main:$HCD_CLASSPATH"
fi

#
# Add HCD conf
#
HCD_CLASSPATH=$HCD_CLASSPATH:$HCD_CONF

export HCD_CLASSPATH=$(remove_duplicates "$HCD_CLASSPATH")

#########################################
# Setup Cassandra env
#########################################

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
if [ -z "$cassandra_storagedir" ]; then
    export cassandra_storagedir="$HCD_HOME/data"
fi

if [ -z "$CASSANDRA_LOG_DIR" ]; then
    if [ -w "$HCD_LOG_ROOT" ] || [ -w "$HCD_LOG_ROOT/cassandra" ]; then
        export CASSANDRA_LOG_DIR="$HCD_LOG_ROOT/cassandra"
    else
        export CASSANDRA_LOG_DIR="/var/log/cassandra"
    fi
fi

if [ -z "$CASSANDRA_HOME" -o ! -d "$CASSANDRA_HOME"/tools/lib ]; then
    for dir in $HCD_HOME/resources/cassandra \
               $HCD_HOME/cassandra \
               /usr/share/hcd/cassandra \
               /usr/local/share/hcd/cassandra \
               /opt/cassandra; do

        if [ -r "$dir" ]; then
            export CASSANDRA_HOME="$dir"
            # Resetting java agent... otherwise it's likely to point
            # to an invalid file
            export JAVA_AGENT=
            break
        fi
    done
    if [ -z "$CASSANDRA_HOME" ]; then
        echo "Cannot determine CASSANDRA_HOME."
        exit 1
    fi
fi

if [ -z "$CASSANDRA_BIN" -o ! -x "$CASSANDRA_BIN"/cassandra ]; then
    for dir in $CASSANDRA_HOME/bin /usr/bin /usr/sbin; do
        if [ -x "$dir/cassandra" ]; then
            export CASSANDRA_BIN="$dir"
            break
        fi
    done
    if [ -z "$CASSANDRA_BIN" ]; then
        echo "Cannot determine CASSANDRA_BIN."
        exit 1
    fi
fi

if [ -z "$CASSANDRA_CONF" -o ! -r "$CASSANDRA_CONF"/cassandra.yaml ]; then
    for dir in $CASSANDRA_HOME/conf \
               /etc/hcd/cassandra \
               /etc/hcd/ \
               /etc/cassandra; do
        if [ -r "$dir/cassandra.yaml" ]; then
            export CASSANDRA_CONF="$dir"
            break
        fi
    done
    if [ -z "$CASSANDRA_CONF" ]; then
        echo "Cannot determine CASSANDRA_CONF."
        exit 1
    fi
fi

if [ -z "$CASSANDRA_DRIVER_CLASSPATH" ]; then
    for jar in "$CASSANDRA_HOME"/../driver/lib/*.jar; do
        CASSANDRA_DRIVER_CLASSPATH="$CASSANDRA_DRIVER_CLASSPATH:$jar"
    done
fi

export CASSANDRA_DRIVER_CLASSPATH

if [ -z "$CASSANDRA_CLASSPATH" ]; then
    CASSANDRA_STRESS_CLASSPATH=""
    for jar in "$CASSANDRA_HOME"/tools/lib/*.jar; do
        CASSANDRA_STRESS_CLASSPATH="$CASSANDRA_STRESS_CLASSPATH:$jar"
    done
    CASSANDRA_CLASSPATH="$CASSANDRA_CONF"
    FOUND_CASSANDRA_JAR=0
    for jar in "$CASSANDRA_HOME"/lib/*.jar; do
        if filematch "*/lib/dse-db-all-*" "$jar" ; then
            if [ "$FOUND_CASSANDRA_JAR" != "0" ]; then
                echo "Found multiple DS DB jar files in $(dirname "$jar"). Please make sure there is only one."
                exit 1
            fi
            FOUND_CASSANDRA_JAR=1
        fi
        if filematch "*/lib/cassandra-all-*" "$jar" ; then
            if [ "$FOUND_CASSANDRA_JAR" != "0" ]; then
                echo "Found multiple Cassandra jar files in $(dirname "$jar"). Please make sure there is only one."
                exit 1
            fi
            FOUND_CASSANDRA_JAR=1
        fi
        CASSANDRA_CLASSPATH="$CASSANDRA_CLASSPATH:$jar"
    done
    CASSANDRA_CLASSPATH="$CASSANDRA_CLASSPATH:$CASSANDRA_DRIVER_CLASSPATH"
    for dir in $HCD_LIB; do
        for jar in $dir/slf4j*; do
            if [ -r "$jar" ]; then
                CASSANDRA_CLASSPATH="$CASSANDRA_CLASSPATH:$jar"
            fi
        done
    done
fi

export JAVA_LIBRARY_PATH="$CASSANDRA_HOME/lib/sigar-bin:$JAVA_LIBRARY_PATH"

export CASSANDRA_CLASSPATH=$(remove_duplicates "$CASSANDRA_CLASSPATH")
export CASSANDRA_STRESS_CLASSPATH

# Set JAVA_AGENT option like we do in cassandra.in.sh
# as some tools (e.g., nodetool) don't call that
if [ -z "$HCD_TOOL" ]; then
    export JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.3.2.jar"
fi

case "`uname`" in
    Linux)
        system_memory_in_mb=`free -m | awk '/:/ {print $2;exit}'`
        system_cpu_cores=`egrep -c 'processor([[:space:]]+):.*' /proc/cpuinfo`
    ;;
    FreeBSD)
        system_memory_in_bytes=`sysctl hw.physmem | awk '{print $2}'`
        system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
        system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
    ;;
    SunOS)
        system_memory_in_mb=`prtconf | awk '/Memory size:/ {print $3}'`
        system_cpu_cores=`psrinfo | wc -l`
    ;;
    Darwin)
        system_memory_in_bytes=`sysctl hw.memsize | awk '{print $2}'`
        system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
        system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
    ;;
    *)
        # assume reasonable defaults for e.g. a modern desktop or
        # cheap server
        system_memory_in_mb="2048"
        system_cpu_cores="2"
    ;;
esac

# Include HCD's custom configuration loader (unless set)
#if [ -z "$HCD_CONFIG_LOADER" ]; then
#  HCD_CONFIG_LOADER="-Dcassandra.config.loader=com.datastax.hcd.config.HcdConfigurationLoader"
#fi

HCD_OPTS="$HCD_OPTS $HCD_CONFIG_LOADER"

# May be needed for kerberos.
if [ -n "$DS_LOGIN_CONFIG" ]; then
    HCD_OPTS="$HCD_OPTS -Djava.security.auth.login.config=$DS_LOGIN_CONFIG"
fi

export HCD_OPTS

#########################################
# Add all components classpaths
# to global CLASSPATH
#########################################

export CLASSPATH="$(sanitize_classpath "$HCD_CLASSPATH:$CASSANDRA_CLASSPATH")"
