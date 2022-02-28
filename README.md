# storage_tools

The script calculates usage of entities inside a given container

The list of entities:

VM
Snapshot
Volume Group
Protection Domain
Images

How to use
The script needs to be placed inside /home/nutanix/bin directory
Script needs to input 1.<container_name> 2.mode(either calculate_usage or ui)
python container_usage_v3.py <ctr-name> calculate_usage
