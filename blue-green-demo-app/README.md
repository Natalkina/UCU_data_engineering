# Blue-Green Deployment Demo Application

A simple Flask application demonstrating blue-green deployment strategy in Kubernetes.


## Application Details

- **Framework**: Flask (Python)
- **Port**: 5000
- **Response**: "Нехай цей день буде добрим"
- **Image**: `sokilnatalka/ucu_test_kub:blue` / `sokilnatalka/ucu_test_kub:green`

## Blue-Green Deployment Strategy

Blue-green deployment allows zero-downtime deployments by maintaining two identical production environments:

- **Blue**: Current production version
- **Green**: New version for testing
- **Switch**: Traffic routing between versions

## Automated CI/CD Pipeline

The application uses GitHub Actions for automated testing, building, and deployment.

### Pipeline Workflow (`.github/workflows/ci-cd.yaml`)

**Triggers**: Push to `main` branch

**Build & Push Job**:
1. Checkout code
2. Set up Python 3.11
3. Install dependencies
4. Run unit tests
5. Login to DockerHub
6. Build and push blue/green images

**Deploy Job**:
1. Configure AWS credentials
2. Update kubeconfig for EKS cluster
3. Deploy blue deployment
4. Deploy green deployment

### Local Testing
```bash
cd app
pip install -r requirements.txt
python main.py
# Visit http://localhost:5000
```

## Monitoring Deployment

```bash
# Check deployments
kubectl get deployments

# Check pods
kubectl get pods -l app=blue-green-demo

# Check service
kubectl get service blue-green-demo

# View logs
kubectl logs -l version=blue
kubectl logs -l version=green
```


### Required Secrets
- `DOCKER_USERNAME`: DockerHub username
- `DOCKER_PASSWORD`: DockerHub password
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
