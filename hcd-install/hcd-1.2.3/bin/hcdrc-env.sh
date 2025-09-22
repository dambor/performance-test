#!/bin/sh

############################################
# Pull in HCDRC_FILE if it exists
# and extract the credentials
############################################
HADOOP_CREDENTIALS=""
HCD_CLIENT_TOOL_CREDENTIALS=""
HCDTOOL_CREDENTIALS=""
if [ -z "$HCDRC_FILE" ]; then
    HCDRC_FILE="$HOME/.hcdrc"
fi

read_password()
{
    stty -echo
    trap "stty echo; kill -9 $$" INT
    read "$@"
    stty echo
    trap - INT
    echo
}

set_credentials() {
    if [ -z $DS_CREDENTIALS_SUPPLIED ]; then
        if [ -f "$HCDRC_FILE" ]; then
            username=$(echo `cat $HCDRC_FILE | grep -E ^username` | awk  '{ string=substr($0, index($0, "=") + 1); print string; }' )
            password=$(echo `cat $HCDRC_FILE | grep -E ^password` | awk  '{ string=substr($0, index($0, "=") + 1); print string; }' )
            sasl_protocol=$(echo `cat $HCDRC_FILE | grep -E ^sasl_protocol` | awk  '{ string=substr($0, index($0, "=") + 1); print string; }' )
            login_config=$(echo `cat $HCDRC_FILE | grep -E ^login_config` | awk  '{ string=substr($0, index($0, "=") + 1); print string; }' )

            DS_USERNAME="${username:-$DS_USERNAME}"
            DS_PASSWORD="${password:-$DS_PASSWORD}"
            DS_SASL_PROTOCOL="${sasl_protocol:-$DS_SASL_PROTOCOL}"
            DS_LOGIN_CONFIG="${login_config:-$DS_LOGIN_CONFIG}"
        fi
        if [ ! -z $ds_username ]; then
            DS_USERNAME="$ds_username"
            if [ -z $ds_password ]; then
                printf "Password: "
                read_password ds_password
                export ds_password
            fi
            DS_PASSWORD="$ds_password"
        fi
        if [ ! -z $DS_USERNAME ]; then
            export HCD_CLIENT_TOOL_CREDENTIALS="-u $DS_USERNAME -p $DS_PASSWORD"
            export HCDTOOL_CREDENTIALS="-l $DS_USERNAME -p $DS_PASSWORD"
        fi
    fi
    if [ ! -z $ds_jmx_username ]; then
        DS_JMX_USERNAME="$ds_jmx_username"
        if [ -z $ds_jmx_password ]; then
            printf "JMX Password: "
            read_password ds_jmx_password
            export ds_jmx_password
        fi
        DS_JMX_PASSWORD="$ds_jmx_password"
    elif [ -f "$HCDRC_FILE" ]; then
        DS_JMX_USERNAME=$(echo `cat $HCDRC_FILE | grep -E ^jmx_username` | awk  '{ string=substr($0, index($0, "=") + 1); print string; }' )
        DS_JMX_PASSWORD=$(echo `cat $HCDRC_FILE | grep -E ^jmx_password` | awk  '{ string=substr($0, index($0, "=") + 1); print string; }' )
    fi
    if [ ! -z $DS_JMX_USERNAME ]; then
        DS_JMX_CREDENTIALS="-a $DS_JMX_USERNAME -b $DS_JMX_PASSWORD"
        CASSANDRA_JMX_CREDENTIALS="-u $DS_JMX_USERNAME -pw $DS_JMX_PASSWORD"
    fi

    if [ -z "$DS_USERNAME" ]; then
        unset DS_USERNAME
    else
        export DS_USERNAME
    fi

    if [ -z "$DS_PASSWORD" ]; then
        unset DS_PASSWORD
    else
        export DS_PASSWORD
    fi

    if [ -z "$DS_SASL_PROTOCOL" ]; then
        unset DS_SASL_PROTOCOL
    else
        export DS_SASL_PROTOCOL
    fi
}

set_credentials
