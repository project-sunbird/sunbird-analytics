## Commands for running ansile scripts##

## Dev ##

### LPA_Provision_Dataproducts_Dev ###

1. ansible-playbook -i inventories/dev setup.yml --limit analytics --tags "analytics,cassandra"

### LPA_Provision_API_Dev ###

1.ansible-playbook -i inventories/dev setup.yml --limit api --tags "api,cassandra"

### LPA_Provision_Secor_Dev ###

1.ansible-playbook -i inventories/dev setup.yml --limit secor --tags secor

### LPA_Upgrade_Cassandra ###

1.ansible-playbook -i inventories/$host upgrade.yml --tags cassandra

### LPA_Deploy_Dataproducts_Dev ###

1.ansible-playbook -i inventories/dev deploy.yml --limit cassandra --tags cassandra

2.ansible-playbook -i inventories/dev deploy.yml --limit analytics --tags analytics

### LPA_Deploy_API_Dev ###

1.cp platform-scripts/database/data.cql platform-scripts/ansible

2.ansible-playbook -i inventories/dev deploy.yml --limit cassandra --tags cassandra

3.ansible-playbook -i inventories/dev deploy.yml --limit cassandra --tags cassandra

### LPA_Deploy_Secor_ME_Dev ###

1.ansible-playbook -i inventories/dev deploy.yml --limit secor-me --tags secor-me

### LPA_Deploy_Secor_Raw_Dev ###

1.ansible-playbook -i inventories/dev deploy.yml --limit secor --tags secor-raw



## QA same as Dev ##


## Prod ##

### LPA_Provision_Dataproducts_Prod ###

1.ansible-playbook -i inventories/prod setup.yml --limit analytics --tags analytics --skip-tags cassandra

### LPA_Provision_API_Prod ###
1.ansible-playbook -i inventories/prod setup.yml --limit api --tags api --skip-tags cassandra

### LPA_Provision_Secor_Prod ###
1.ansible-playbook -i inventories/prod setup.yml --limit secor --tags secor

### LPA_Deploy_Dataproducts_Prod ###
1.cp platform-scripts/database/data.cql platform-scripts/ansible

2.ansible-playbook -i inventories/prod deploy.yml --limit cassandra --tags cassandra

3.ansible-playbook -i inventories/prod deploy.yml --limit analytics --tags analytics

### LPA_Deploy_API_Prod ###
1.cp platform-scripts/database/data.cql platform-scripts/ansible

2.ansible-playbook -i inventories/prod deploy.yml --limit cassandra --tags cassandra

3.ansible-playbook -i inventories/prod deploy.yml --limit api --tags api

### LPA_Deploy_Secor_ME_Prod ###
1.ansible-playbook -i inventories/prod deploy.yml --limit secor-me --tags secor-me

### LPA_Deploy_Secor_Raw_Prod ###
1.ansible-playbook -i inventories/prod deploy.yml --limit secor --tags secor-raw
