#!/bin/sh

# add any environment overrides you need here. This is where users
# may set thirdparty variables.

# This is here so the installer can force set HCD_HOME
#HCD_HOME="/usr/share/hcd"
#export HCD_HOME

# ==================================
# don't change after this.
export HCD_MODE="1"

if [ "$HCD_HOME" = "" ]; then
  if [ -x "/usr/share/hcd" ]; then
    HCD_HOME="/usr/share/hcd"
    export HCD_HOME
  fi
fi

if [ -r "`dirname "$0"`/hcd.in.sh" ]; then
    # File is right where the executable is
    . "`dirname "$0"`/hcd.in.sh"
elif [ -r "/usr/share/hcd/hcd.in.sh" ]; then
    # Package install location
    . "/usr/share/hcd/hcd.in.sh"
elif [ -r "$HCD_HOME/bin/hcd.in.sh" ]; then
    # Package install location
    . "$HCD_HOME/bin/hcd.in.sh"
else
    # Go up directory tree from where we are and see if we find it
    DIR="`dirname $0`"
    for i in 1 2 3 4 5 6; do
        if [ -r "$DIR/bin/hcd.in.sh" ]; then
            HCD_IN="$DIR/bin/hcd.in.sh"
            break
        fi
        DIR="$DIR/.."
    done
    if [ -r "$HCD_IN" ]; then
        . "$HCD_IN"
    else
        echo "Cannot determine location of hcd.in.sh"
        exit 1
    fi
fi
