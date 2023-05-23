#! /bin/sh

echo HOST_IP="$(ip route get 1.1.1.1 | grep -oP 'src \K\S+')" > env_file.txt
port=$(shuf -i 2000-65000 -n 1)
while nc -z localhost $port; do
  port=$(shuf -i 2000-65000 -n 1)
done
echo CONT_PORT="$port" >> env_file.txt
echo GREENHOUSE_ID="$2" >> env_file.txt
echo BLOCK_ID="$3" >> env_file.txt
docker container run --rm -it --network=digital_twins_bridge_swarm -p "$port":9090/tcp --env-file=env_file.txt --name=DT_container"$1" dt_image
