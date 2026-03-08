# RFO Gen-2 Kubernetes Project

This project extends Project 1 into a containerized microservices deployment.

## Services
- submit-api: accepts POST /submit and publishes Kafka jobs
- recent-api: accepts GET /recent and queries PostgreSQL
- worker: consumes Kafka jobs, processes images, writes results to S3 and PostgreSQL

## Tech stack
- Docker
- Kubernetes (EKS)
- Microservices
- Amazon MSK
- Amazon RDS PostgreSQL
- Amazon S3

## Repo structure
- services/: application services
- k8s/: Kubernetes manifests
- .github/workflows/: CI/CD pipeline