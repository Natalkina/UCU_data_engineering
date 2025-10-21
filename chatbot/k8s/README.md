# Chatbot Application Deployment to Kubernetes

This directory contains Kubernetes manifests to deploy the Python Chatbot application with LLM backend to Minikube.

## Architecture

The application consists of:
* **LLM Service**: Ollama with Gemma2:2b model (internal service)
* **Webapp**: Python Dash chatbot interface (external NodePort)
* **Persistent Volume**: Storage for Ollama model data
* **ConfigMap**: Application configuration

## Resources Created

* `llm-deployment.yaml` - Ollama LLM deployment with Gemma2:2b model
* `llm-service.yaml` - Internal service for LLM communication
* `webapp-deployment.yaml` - Python Dash webapp deployment
* `webapp-service.yaml` - External NodePort service for webapp
* `ollama-persistentvolumeclaim.yaml` - Persistent storage for model data
* `configmap.yaml` - Configuration for webapp

## Prerequisites

1. **Minikube running:**
```bash
minikube start
```

2. **Docker image available:**
```bash
# Build and push webapp image (if needed)
docker build -t sokilnatalka/webapp:latest ../webapp/
docker push sokilnatalka/webapp:latest
```

## Deployment

1. **Deploy all resources:**
```bash
kubectl apply -f .
```

2. **Check deployment status:**
```bash
kubectl get pods
kubectl get services
```

3. **Wait for LLM model download (may take 5-10 minutes):**
```bash
kubectl logs -f deployment/llm
```

## Access Application

1. **Get webapp service URL:**
```bash
minikube service webapp --url
```

2. **Or use port forwarding:**
```bash
kubectl port-forward service/webapp 8050:8050
```

3. **Open in browser:**
```
http://localhost:8050
```

## Monitoring

**Check pod status:**
```bash
kubectl get pods -w
```

**View logs:**
```bash
kubectl logs -f deployment/webapp
kubectl logs -f deployment/llm
```

**Check services:**
```bash
kubectl get svc
```

## Cleanup

**Remove all resources:**
```bash
kubectl delete -f .
```

## Troubleshooting

1. **LLM pod not ready:** Check if model is still downloading
2. **Webapp connection errors:** Ensure LLM service is running
3. **Storage issues:** Check PVC status with `kubectl get pvc`

## Configuration

- **Webapp Port:** 8050 (NodePort)
- **LLM Port:** 11434 (ClusterIP)
- **Model:** Gemma2:2b (~1.6GB)
- **Storage:** 5Gi for model data