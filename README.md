# GH_digital_twins

Before running the project it's necessary to satisfy some pre-requisites:
1. Installing Docker Engine by following the instructions available at https://docs.docker.com/engine/install/ for your specific OS and architecture.
2. Installing Docker SDK for Python by running:
    ```
    pip install docker
    ```

Then the project can be setup and run automatically with the following commands (starting from the root directory):
1. Open the folder with all the docker-related files:
```
 cd DT_Docker/
```
2. Run the setup script:
```
./project_setup.sh
```
3. Run the run-system script: 
```
./run_system.sh
```
**NOTE:** Modify the section tagged with the comment "Services section" by following the reported instructions in order to add/modify/delete any service.

4. **OPTIONAL** - Run the script to start the dashboard used as a test client (starting from the root directory):
```
cd Client/
launch_dashboard.sh
```
5. **OPTIONAL** - to simulate the source devices (in the "Client" directory accessed in the step above):
```
python sensor_data_simulation.py
```
6. **OPTIONAL** - to test the sistem reaction when under high loads of traffic (starting from the root directory):
```
python DT_bombardier.py
```
