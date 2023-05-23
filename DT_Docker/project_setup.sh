#! /bin/sh

printf "\n-- Building the necessary images\n"
./build_NSImage.sh
./build_LoggerImage.sh
./build_DTImage.sh

printf "\n-- Initializing the swarm\n"
docker swarm init

printf "\n-- Deleting possible old versions of the networks\n"
docker network rm digital_twins_bridge_swarm

printf "\n-- Creating the networks\n"
docker network create --driver bridge --scope swarm --attachable  digital_twins_bridge_swarm


