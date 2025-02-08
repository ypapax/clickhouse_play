#!/usr/bin/env bash
#set -euxo pipefail
set -x
up(){
  docker-compose up
}

"$@"
