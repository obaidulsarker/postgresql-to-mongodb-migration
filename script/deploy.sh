#!/bin/bash

# Define variables
DOCKER_REG=http://192.168.169.66
HARBOR_USER="admin"

DOCKER_REG_HOST_IP=192.168.169.66
PROJECT_NAME=postgresql-to-mongodb-migration
REPO_NAME=postgresql-to-mongodb-migration
APP_VERSION=latest

BASE_PATH="/data/app/postgresql-to-mongodb-migration"
LOGFILE=$BASE_PATH/deploy.log

echo "$(date) : [$APP_REPO] deployment is started." >> $LOGFILE;
echo "$(date) : [$APP_REPO] ***********************" >> $LOGFILE;
# Login to Harbor registry

echo "$(date) : [$APP_REPO] docker login .." >> $LOGFILE;
cat docker-reg-password.txt | docker login $DOCKER_REG --username "$HARBOR_USER" --password-stdin >> $LOGFILE;

sudo docker ps --filter status=exited -q | xargs docker rm

echo "$(date) : [$APP_REPO] docker image pull .." >> $LOGFILE;
docker pull $DOCKER_REG_HOST_IP/$PROJECT_NAME/$REPO_NAME:$APP_VERSION

echo "$(date) : [$APP_REPO] docker run .." >> $LOGFILE;
docker run -v $BASE_PATH/logs:/app/logs -v $BASE_PATH/cred:/app/cred -v $BASE_PATH/config:/app/config -d $DOCKER_REG_HOST_IP/$PROJECT_NAME/$REPO_NAME:$APP_VERSION

echo "$(date) : [$APP_REPO] deployment is completed" >> $LOGFILE;