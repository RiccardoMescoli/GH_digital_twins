#! /bin/sh

CONT_NAME="NS_container"
docker container run --rm -d -it --network=digital_twins_bridge_swarm -p 9090:9090 -p 9091:9091 -e CONT_NAME=$CONT_NAME --name=$CONT_NAME ns_image
#docker network connect digital_twins_overlay NS_container