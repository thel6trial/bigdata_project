#!/bin/bash
docker rm $(docker ps -qa)
docker-compose up -d