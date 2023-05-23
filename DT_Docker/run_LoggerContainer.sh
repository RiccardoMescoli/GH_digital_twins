#! /bin/sh

HOST_IP="$(ip route get 1.1.1.1 | grep -oP 'src \K\S+')"
docker container run --rm -d -it --network=digital_twins_bridge_swarm -e HOST_IP="$HOST_IP" --name=LoggerContainer"$1" logger_image
#docker network connect digital_twins_overlay LoggerContainer"$1"