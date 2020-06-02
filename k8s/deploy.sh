#!/bin/bash
kubectl apply -f k8s/storages/shared_spider.yml
kubectl apply -f k8s/accounts/spider-accounts.yaml
kubectl apply -f k8s/deployments/spider-redis.yaml
kubectl apply -f k8s/deployments/spider-redis-cp.yaml
kubectl apply -f k8s/deployments/spider-flower.yaml
kubectl apply -f k8s/service/spider-redis.yaml
kubectl apply -f k8s/service/spider-redis-cp.yaml
kubectl apply -f k8s/service/spider-flower.yaml
kubectl apply -f k8s/deployments/spider-worker.yaml
kubectl apply -f k8s/cronjobs/spider-cron-affiliation.yaml
kubectl apply -f k8s/cronjobs/spider-cron-queues.yaml