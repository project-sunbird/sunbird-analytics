ansible-playbook -i hosts deploy.yml --tags "dev"
ansible-playbook -i hosts deploy.yml --tags "prod"
ansible-playbook -i hosts deploy.yml --tags "sandbox"