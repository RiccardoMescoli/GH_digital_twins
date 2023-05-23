printf "\n-- Deleting all possible old versions of standalone containers\n"
docker container stop NS_container
docker container rm NS_container
docker container stop "$(docker container ls -aq --filter "name=LoggerContainer")"
docker container rm "$(docker container ls -aq --filter "name=LoggerContainer")"

printf "\n-- Running all standalone containers\n"
./run_NScontainer.sh
./run_LoggerContainer.sh 1

# Services section: Edit to add/modify/remove the launched services
# Format gnome-terminal --tab -- bash -c './run_DT_swarm.sh <green_house_alphabetic_code> <block_numeric_code>'
printf "\n-- Running the services and control scripts\n"
printf "/- A1\n"
gnome-terminal --tab -- bash -c './run_DT_swarm.sh A 1'
printf "/- A2\n"
gnome-terminal --tab -- bash -c './run_DT_swarm.sh A 2'
printf "/- B1\n"
gnome-terminal --tab -- bash -c './run_DT_swarm.sh B 1'
printf "/- B2\n"
gnome-terminal --tab -- bash -c './run_DT_swarm.sh B 2'