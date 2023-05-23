import docker
import argparse
import string
import dotenv
import time

from docker.errors import APIError

DOCKER_SOCK_URI = "unix:///var/run/docker.sock"

# Docker image name and tag
image_name = 'dt_image'
image_tag = 'latest'

# Upper and lower traffic thresholds in bytes per second
highest_traffic_recorded = 51031  # 467931.6
upper_threshold = highest_traffic_recorded * .5  # 1.2  # Adjust this value as needed
lower_threshold = highest_traffic_recorded * .1  # 0.6   # Adjust this value as needed
TRAFFIC_REG_DELAY = 5

parser = argparse.ArgumentParser()
parser.add_argument("--ghid",
                    dest="greenhouseID",
                    help="A letter identifying the green house",
                    nargs=1,
                    required=True,
                    choices=list(string.ascii_letters)
                    )
parser.add_argument("--bid",
                    dest="blockID",
                    help="A digit identifying the green house block",
                    nargs=1,
                    required=True,
                    choices=list(range(1, 10)),
                    type=int
                    )
parser.add_argument("--initn",
                    dest="initialN",
                    help="Initial number of copies",
                    nargs=1,
                    required=True,
                    type=int
                    )
parser.add_argument("--nincr",
                    dest="NIncreaseStep",
                    help="Step increase/decrease in number of copies",
                    nargs=1,
                    required=True,
                    type=int
                    )
parser.add_argument("--minn",
                    dest="minimumN",
                    help="Minimum number of copies",
                    nargs=1,
                    required=True,
                    type=int
                    )
parser.add_argument("--net",
                    dest="network",
                    help="Network to run the container on",
                    nargs=1,
                    required=True,
                    type=str
                    )
parser.add_argument("--port",
                    dest="port",
                    help="Port to run the container on",
                    nargs=1,
                    required=True,
                    type=int
                    )
args = parser.parse_args()
env_dict = dict(dotenv.dotenv_values("env_file.txt"))
env_dict["GREENHOUSE_ID"] = str(*args.greenhouseID)
env_dict["BLOCK_ID"] = str(*args.blockID)
# Docker Swarm service name
service_name = 'DT-block-copies-' + env_dict["GREENHOUSE_ID"] + env_dict["BLOCK_ID"]
print(service_name)
current_copies = int(*args.initialN)
print(current_copies)
increment = int(*args.NIncreaseStep)
minimum_copies = int(*args.minimumN)

# Docker Swarm client
client = docker.DockerClient(base_url=DOCKER_SOCK_URI)

# Create a Docker Swarm service
def create_service():
    services = client.services.list(filters={"name": service_name})
    if len(services) == 0:
        print("No old version found.")
        # Handle the case where the service doesn't exist
    else:
        # Delete the service
        services[0].remove()
        print("Old service version deleted.")
        print("Delay to allow a cleanup.", end=" ", flush=True)
        for i in range(60):
            time.sleep(1)
            print(".", end=" ", flush=True)
        print("Delay ended!")
    return client.services.create(
                name=service_name,
                image=f"{image_name}:{image_tag}",
                env=env_dict,
                mode=docker.types.ServiceMode(mode="replicated", replicas=current_copies),
                endpoint_spec=docker.types.EndpointSpec(
                    ports={int(*args.port): 9090}
                ),
                networks=[*args.network],
                tty=True
            )


# Scale Docker Swarm service to a specified number of replicas
def scale_service(service, replicas):
    success = False
    while not success:
        try:
            service.reload()
            success = service.scale(replicas)
        except APIError as e:
            print(e.explanation)



# Measure average traffic for a set of containers
def measure_average_traffic(service):
    total_traffic = 0
    average_traffic = None
    tasks = service.tasks(filters={'desired-state': "Running"})
    print(f"Total tasks found: {len(tasks)}")
    for task in tasks:
        access_success = False
        while not access_success:
            try:
                container_id = task['Status']['ContainerStatus']['ContainerID']
                container = client.containers.get(container_id)
                container_id = container.id
                stats1 = container.stats(stream=False)
                time.sleep(TRAFFIC_REG_DELAY)  # Sleep for TRAFFIC_REG_DELAY second to get a time interval
                stats2 = container.stats(stream=False)
                network_stats1 = stats1['networks']
                network_stats2 = stats2['networks']
                container_traffic = 0
                access_success = True
            except KeyError as e:
                print(f"Key error on: {e} - Retrying. . .")
                time.sleep(5)
                service = client.services.get(service_name)

        for network in network_stats1.keys():
            rx_bytes1 = network_stats1[network]['rx_bytes']
            tx_bytes1 = network_stats1[network]['tx_bytes']
            rx_bytes2 = network_stats2[network]['rx_bytes']
            tx_bytes2 = network_stats2[network]['tx_bytes']
            rx_bytes_per_second = rx_bytes2 - rx_bytes1
            tx_bytes_per_second = tx_bytes2 - tx_bytes1
            container_traffic += (rx_bytes_per_second + tx_bytes_per_second)/TRAFFIC_REG_DELAY
        total_traffic += container_traffic
    if len(tasks) > 0:
        average_traffic = total_traffic / len(tasks)

    return average_traffic


# Main loop to measure traffic and scale service
service = create_service()
while True:
    avg_traffic = measure_average_traffic(service)
    if avg_traffic is not None:
        print('Average traffic: {} bytes/s'.format(avg_traffic))
        if avg_traffic > upper_threshold:
            current_copies += increment
            print(f"Current copies: {current_copies} - INCREASE")
            scale_service(service, current_copies)
        elif avg_traffic < lower_threshold and (current_copies - increment) >= minimum_copies:
            current_copies -= increment
            print(f"Current copies: {current_copies} - DECREASE")
            scale_service(service, current_copies)  # Scale down to 1 replica if average traffic falls below lower threshold
    print("CHECK COMPLETED")
    time.sleep(60)  # Sleep for 60 seconds before measuring traffic again
