#!/bin/bash
#PROD#kubectl apply -f storages/shared_spider.yml
kubectl apply -f accounts/spider-accounts.yaml
kubectl apply -f deployments/spider-redis.yaml
kubectl apply -f deployments/spider-redis-cp.yaml
kubectl apply -f deployments/spider-flower.yaml
kubectl apply -f service/spider-redis.yaml
kubectl apply -f service/spider-redis-cp.yaml
kubectl apply -f service/spider-flower.yaml
kubectl apply -f deployments/spider-worker.yaml
kubectl apply -f cronjobs/spider-cron-affiliation.yaml
kubectl apply -f cronjobs/spider-cron-queues.yaml