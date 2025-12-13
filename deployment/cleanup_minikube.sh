#!/bin/bash

# Cleanup Script for Minikube

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}CLEANUP MINIKUBE RESOURCES      ${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

echo -e "${YELLOW}Stopping Minikube...${NC}"
minikube stop

echo -e "${YELLOW}Deleting Minikube cluster...${NC}"
minikube delete

echo -e "${GREEN}Cleanup complete! ✅${NC}"
