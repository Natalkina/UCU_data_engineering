# Blue-Green Deployment Demo Application

A simple Flask application demonstrating blue-green deployment strategy in Kubernetes.

## Application Structure

```
blue-green-demo-app/
├── app/
│   ├── main.py           # Flask application
│   ├── requirements.txt  # Python dependencies
│   └── test_main.py      # Tests
├── k8s/
│   ├── deployment-blue.yaml   # Blue deployment
│   ├── deployment-green.yaml  # Green deployment
│   └── service.yml            # LoadBalancer service
└── Dockerfile            # Container image definition
```

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

### Manual Build (if needed)

```bash
# Build blue version
docker build -t sokilnatalka/ucu_test_kub:blue .
docker push sokilnatalka/ucu_test_kub:blue

# Build green version
docker build -t sokilnatalka/ucu_test_kub:green .
docker push sokilnatalka/ucu_test_kub:green
```

### 3. Switch Traffic

```bash
# Switch service to green
kubectl patch service blue-green-demo -p '{"spec":{"selector":{"version":"green"}}}'

# Switch back to blue if needed
kubectl patch service blue-green-demo -p '{"spec":{"selector":{"version":"blue"}}}'
```

## Testing

### Automated Testing
Tests run automatically in GitHub Actions pipeline:
```bash
python -m unittest discover blue-green-demo-app/app
```

### Local Testing
```bash
cd app
pip install -r requirements.txt
python main.py
# Visit http://localhost:5000
```

### Kubernetes Testing
```bash
# Get service URL (EKS)
kubectl get service blue-green-demo

# Or port forward
kubectl port-forward service/blue-green-demo 8080:80
# Visit http://localhost:8080
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

## Blue-Green Workflow

### Automated (via GitHub Actions)
1. **Push to main**: Triggers CI/CD pipeline
2. **Run Tests**: Automated unit testing
3. **Build Images**: Create blue and green Docker images
4. **Deploy to EKS**: Deploy both versions to AWS EKS cluster

### Manual Traffic Switching
5. **Switch Traffic**: Route traffic from blue to green
6. **Monitor**: Watch for issues
7. **Rollback**: Switch back to blue if problems occur
8. **Cleanup**: Remove old deployment

### Required Secrets
- `DOCKER_USERNAME`: DockerHub username
- `DOCKER_PASSWORD`: DockerHub password
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key

## Cleanup

```bash
# Remove deployments
kubectl delete -f k8s/

# Or individually
kubectl delete deployment blue-deployment
kubectl delete deployment green-deployment
kubectl delete service blue-green-demo
```

## Advantages

- **Zero Downtime**: Instant traffic switching
- **Easy Rollback**: Quick revert to previous version
- **Testing**: Test new version in production environment
- **Risk Reduction**: Minimize deployment risks