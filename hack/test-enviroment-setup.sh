#!/usr/bin/env sh

CONTAINER=hraft-dispatcher-pg
POSTGRES_PASSWORD=qplaFceC

RUNNING=$(docker inspect --format="{{.State.Running}}" $CONTAINER 2>/dev/null)

if [ $? -eq 1 ]; then
  # $CONTAINER does not exist
  docker run --name hraft-dispatcher-pg -p 5433:5432 -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -d postgres:13.1
elif [ "$RUNNING" = "false" ]; then
  # $CONTAINER is not running
  docker start hraft-dispatcher-pg
  exit 0
fi

echo "Postgres server is running now. Connection URI: postgresql://postgres:$POSTGRES_PASSWORD@127.0.01:5433/casbin"
