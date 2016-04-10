#!/usr/bin/env bash
set -o verbose

if [ -z "$HADOOP_HOME" ]; then
    echo "HADOOP_HOME not set - abort" >&2
    exit 1
fi

echo "Using ${HADOOP_DISTRO} distribution of Hadoop from ${HADOOP_HOME}"

pwd

mkdir ~/airflow/

echo "CHECKING env variables"
echo "TRAVIS"
echo "${TRAVIS}"
echo "TRAVIS_CACHE"
echo "${TRAVIS_CACHE}"
echo "NIGHTLIES"
echo "${NIGHTLIES}"

if [ "${TRAVIS}" ]; then
    echo "Using travis airflow.cfg"
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    cp -f ${DIR}/airflow_travis.cfg ~/airflow/unittests.cfg
fi

echo Backend: $AIRFLOW__CORE__SQL_ALCHEMY_CONN
./run_unit_tests.sh
