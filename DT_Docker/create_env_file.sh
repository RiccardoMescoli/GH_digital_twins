#! /bin/sh

echo HOST_IP="$(ip route get 1.1.1.1 | grep -oP 'src \K\S+')" > env_file.txt
port=$(shuf -i 2000-65000 -n 1)
while ! nc -z localhost $port; do
  port=$(shuf -i 2000-65000 -n 1)
done
echo CONT_PORT="$port" >> env_file.txt
echo "$port"
