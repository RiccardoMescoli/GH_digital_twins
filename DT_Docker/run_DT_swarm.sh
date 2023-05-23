#! /bin/sh

echo HOST_IP="$(ip route get 1.1.1.1 | grep -oP 'src \K\S+')" > env_file.txt
port=$(shuf -i 2000-65000 -n 1)
while nc -z localhost $port; do
  port=$(shuf -i 2000-65000 -n 1)
done
echo CONT_PORT="$port" >> env_file.txt
echo GREENHOUSE_ID="$1" >> env_file.txt
echo BLOCK_ID="$2" >> env_file.txt
python DSwarm_DT_control.py --ghid "$1" --bid "$2" --initn 3 --nincr 1 --minn 3 --net digital_twins_bridge_swarm --port "$port"