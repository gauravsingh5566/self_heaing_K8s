# 🚀 RAG Resolver Microservice - Complete Setup Guide

A comprehensive AI-powered Kubernetes error resolution microservice using RAG, Qdrant vector database, and MCP for safe command execution.

## 📋 Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Debug Results │    │   RAG Engine    │    │  EKS Cluster    │
│   (MongoDB)     │────│                 │────│  (via MCP)      │
└─────────────────┘    │  • Vector Store │    └─────────────────┘
                       │  • AI Analysis  │    
┌─────────────────┐    │  • Resolution   │    ┌─────────────────┐
│   Qdrant DB     │────│    Planning     │────│  AWS Bedrock    │
│  (Knowledge)    │    │  • Execution    │    │   (LLM/Embed)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## ✨ Key Features

- **🤖 RAG-based Resolution**: Uses stored knowledge to resolve similar issues
- **🔍 Vector Similarity Search**: Finds relevant past resolutions using embeddings
- **⚡ Safe Command Execution**: MCP protocol ensures safe kubectl operations
- **📊 Comprehensive Reports**: Detailed resolution reports for manual intervention
- **🔄 Real-time Processing**: Async resolution with status tracking
- **📈 Learning System**: Improves from feedback and successful resolutions
- **🛡️ Safety Controls**: Multiple layers of command validation
- **🌐 REST API**: Full REST API for integration

## 🛠️ Prerequisites

### Required Services
1. **MongoDB** (existing - with your debug_results collection)
2. **Qdrant Vector Database** (new - for resolution knowledge)
3. **AWS Account** with Bedrock access
4. **EKS Cluster** with kubectl access
5. **Python 3.11+**

### AWS Permissions
Your AWS profile needs:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "bedrock:InvokeModel",
                "bedrock:InvokeModelWithResponseStream"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow", 
            "Action": [
                "eks:DescribeCluster",
                "eks:ListClusters"
            ],
            "Resource": "*"
        }
    ]
}
```

## 🏗️ Installation

### Method 1: Docker Compose (Recommended)

1. **Clone and setup directory structure**:
```bash
mkdir k8s-rag-resolver
cd k8s-rag-resolver

# Create the directory structure
mkdir -p rag_resolver logs monitoring/{grafana/dashboards,grafana/datasources}
```

2. **Copy all the provided files to their locations**:
```bash
# Copy all rag_resolver/*.py files to rag_resolver/
# Copy docker-compose.yml, Dockerfile.resolver to root
```

3. **Configure environment variables** in `docker-compose.yml`:
```yaml
environment:
  # Update these with your values
  - MONGO_URI=mongodb://admin:yourpassword@your-mongo-host:27017/
  - EKS_CLUSTER_NAME=your-eks-cluster
  - AWS_PROFILE=your-aws-profile
```

4. **Start the services**:
```bash
docker-compose up -d
```

5. **Verify startup**:
```bash
# Check service health
curl http://localhost:8080/health

# Check Qdrant
curl http://localhost:6333/health
```

### Method 2: Local Development Setup

1. **Install Python dependencies**:
```bash
cd rag_resolver
pip install -r requirements.txt
```

2. **Install Qdrant locally**:
```bash
# Using Docker
docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant:v1.7.0
```

3. **Set environment variables**:
```bash
export MONGO_URI="mongodb://admin:yourpassword@your-mongo-host:27017/"
export DB_NAME="k8s_logs"
export DEBUG_COLLECTION="debug_results"
export QDRANT_HOST="localhost"
export QDRANT_PORT="6333"
export AWS_REGION="us-west-2"
export AWS_PROFILE="finchat"
export EKS_CLUSTER_NAME="your-eks-cluster"
export SERVICE_PORT="8080"
```

4. **Start the service**:
```bash
python -m rag_resolver.main serve
```

## 🧪 Testing & Verification

### 1. Component Testing
```bash
# Test all components
python -m rag_resolver.main test
```

Expected output:
```
🧪 Testing RAG Resolver Components...
==================================================
✅ Configuration loaded successfully
   - Service: k8s-rag-resolver
   - Qdrant: localhost:6333
   - EKS Cluster: my-eks-cluster
   - MongoDB: k8s_logs

🔧 Initializing RAG Engine...
✅ RAG Engine initialized successfully

📊 Testing Vector Store...
✅ Vector store connection successful
   - Total resolutions: 0
   - Success rate: 0.0%

⚙️  Testing MCP Executor...
✅ MCP Executor connection successful
   - Cluster: my-eks-cluster
   - Nodes: 3

🎯 Testing Sample Resolution...
✅ Found 0 similar resolutions for test error

✅ All component tests completed successfully!
```

### 2. API Testing
```bash
# Health check
curl http://localhost:8080/health

# Get system status
curl http://localhost:8080/cluster/info

# Get unresolved errors
curl http://localhost:8080/errors/unresolved?limit=5
```

### 3. Resolution Testing
```bash
# Test resolution with a real error ID from your debug_results
python -m rag_resolver.main resolve --error-id 507f1f77bcf86cd799439011

# Test with auto-execution (only safe commands)
python -m rag_resolver.main resolve --error-id 507f1f77bcf86cd799439011 --auto-execute

# Generate resolution report
python -m rag_resolver.main report --error-id 507f1f77bcf86cd799439011
```

## 📡 API Usage Examples

### Resolve an Error
```bash
curl -X POST "http://localhost:8080/resolve" \
  -H "Content-Type: application/json" \
  -d '{
    "error_log_id": "507f1f77bcf86cd799439011",
    "auto_execute": false,
    "priority": "normal",
    "requester": "ops-team"
  }'
```

Response:
```json
{
  "message": "Resolution started in background",
  "error_log_id": "507f1f77bcf86cd799439011",
  "priority": "normal",
  "estimated_completion": "2025-01-01T10:05:00Z"
}
```

### Check Resolution Status
```bash
curl http://localhost:8080/resolution