import Pyro5.api as pyro

ns = pyro.locate_ns()

retry = True

data_logger = pyro.Proxy("PYROMETA:datalogger")  # use name server object lookup uri shortcut
gh_block_DT = pyro.Proxy("PYROMETA:DT:GH_block")

while retry:

    choice = int(input("(1) to test the retrieval of a single log\n" +
                       "(2) to test the retrieval of all logs of a block\n"
                       "(3) to test the retrieval of the logs of a sensor of a GH block through its DT\n >> "))
    print("\n\n")

    if choice == 1:
        block_id = input("Block ID >> ")
        sensor_feed = input("Sensor feed >> ")
        h = input("Hours >> ")
        m = input("Minutes >> ")

        print(data_logger.get_sensor_log(block_id, sensor_feed, days=0, hours=h, minutes=m, seconds=0))

    if choice == 2:
        source_id = input("Block ID >> ")
        h = input("Hours >> ")
        m = input("Minutes >> ")

        for log in data_logger.get_sensor_source_logs(source_id, hours=h, minutes=m).items():
            print(log[0], log[1])

    if choice == 3:
        bl

    retry = True if input("Do you want to retry? (Y/n)\n>> ").upper() == "Y" else False
