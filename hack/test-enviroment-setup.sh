#!/usr/bin/env sh

CONTAINER=hraft-dispatcher-pg
POSTGRES_PASSWORD=qplaFceC

RUNNING=$(docker inspect --format="{{.State.Running}}" $CONTAINER 2>/dev/null)

if [ $? -eq 1 ]; then
  echo 'Installing PostgreSQL ...'
  # $CONTAINER does not exist
  docker run --name $CONTAINER -p 5433:5432 -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -d postgres:13.1 || exit 1
  sleep 5s
  docker exec -it $CONTAINER psql -U postgres -c "CREATE DATABASE casbin;" || exit 1
elif [ "$RUNNING" = "false" ]; then
  # $CONTAINER is not running
  docker start hraft-dispatcher-pg || exit 1
fi

echo "PostgreSQL server is running now. Connection URI: postgresql://postgres:$POSTGRES_PASSWORD@127.0.01:5433/casbin"
