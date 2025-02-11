#!/usr/bin/env bash
#set -euxo pipefail
set -x
up(){
  docker-compose --env-file clickhouse.env up
}

fill(){
  cd app
  set -a; . clickhouse.env; set +a; go run main.go
}

"$@"
