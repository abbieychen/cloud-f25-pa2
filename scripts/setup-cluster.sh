#!/bin/bash

echo "=== Step 1: Configure firewall ==="
ansible-playbook -i hosts.ini configure-k8s-firewall.yml --limit k8s_cluster

echo "=== Step 2: Install Docker ==="
ansible-playbook -i hosts.ini install-docker-rockylinux.yml --limit k8s_cluster

echo "=== Step 3: Install Kubernetes components ==="
ansible-playbook -i hosts.ini install-kubernetes-rockylinux.yml --limit k8s_cluster

echo "=== Step 4: Initialize Kubernetes cluster ==="
ansible-playbook -i hosts.ini k8s-cluster-setup.yml

echo "=== Step 5: Verify cluster ==="
ansible-playbook -i hosts.ini k8s-verify-cluster.yml