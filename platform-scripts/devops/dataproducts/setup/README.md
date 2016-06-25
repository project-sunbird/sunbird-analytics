ansible-playbook -i hosts install.yml --tags "sanbox"
ansible-playbook -i hosts install.yml --tags "prod-spark"
ansible-playbook -i hosts install.yml --tags "prod-cassandra"