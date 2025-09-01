import os
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class MongoDBConfig:
    """MongoDB configuration for accessing debug results"""
    uri: str
    database_name: str
    debug_collection: str

@dataclass
class QdrantConfig:
    """Qdrant vector database configuration"""
    host: str
    port: int
    api_key: Optional[str]
    collection_name: str
    vector_size: int
    distance_metric: str

@dataclass
class AWSConfig:
    """AWS and EKS configuration"""
    region: str
    profile: str
    cluster_name: str
    bedrock_model_id: str
    bedrock_embedding_model: str

@dataclass
class MCPConfig:
    """MCP (Model Context Protocol) configuration for EKS commands"""
    server_name: str
    server_command: List[str]
    max_retries: int
    timeout_seconds: int
    allowed_namespaces: List[str]
    restricted_commands: List[str]

@dataclass
class RAGConfig:
    """RAG (Retrieval Augmented Generation) configuration"""
    similarity_threshold: float
    max_context_documents: int
    chunk_size: int
    chunk_overlap: int
    rerank_top_k: int

@dataclass
class ResolverConfig:
    """Main resolver service configuration"""
    mongodb: MongoDBConfig
    qdrant: QdrantConfig
    aws: AWSConfig
    mcp: MCPConfig
    rag: RAGConfig
    service_name: str
    service_port: int
    log_level: str
    max_concurrent_resolutions: int
    auto_execute_safe_commands: bool

def load_resolver_config() -> ResolverConfig:
    """Load resolver configuration from environment variables"""
    
    # MongoDB configuration
    mongodb_config = MongoDBConfig(
        uri=os.getenv("MONGO_URI", "mongodb://admin:SKMO7frslkjsdh546fchjkhg@35.94.146.89:27017/"),
        database_name=os.getenv("DB_NAME", "k8s_logs"),
        debug_collection=os.getenv("DEBUG_COLLECTION", "debug_results")
    )
    
    # Qdrant configuration
    qdrant_config = QdrantConfig(
        host=os.getenv("QDRANT_HOST", "localhost"),
        port=int(os.getenv("QDRANT_PORT", "6333")),
        api_key=os.getenv("QDRANT_API_KEY"),  # Optional for local/cloud
        collection_name=os.getenv("QDRANT_COLLECTION", "k8s_resolutions"),
        vector_size=int(os.getenv("VECTOR_SIZE", "1536")),  # OpenAI embedding size
        distance_metric=os.getenv("DISTANCE_METRIC", "cosine")
    )
    
    # AWS configuration
    aws_config = AWSConfig(
        region=os.getenv("AWS_REGION", "us-west-2"),
        profile=os.getenv("AWS_PROFILE", "finchat"),
        cluster_name=os.getenv("EKS_CLUSTER_NAME", "my-eks-cluster"),
        bedrock_model_id=os.getenv("BEDROCK_MODEL_ID", "openai.gpt-oss-20b-1:0"),
        bedrock_embedding_model=os.getenv("BEDROCK_EMBEDDING_MODEL", "amazon.titan-embed-text-v1")
    )
    
    # MCP configuration for EKS command execution
    mcp_config = MCPConfig(
        server_name=os.getenv("MCP_SERVER_NAME", "kubernetes-mcp"),
        server_command=[
            "npx", "-y", "@kubernetes/mcp-server@latest"
        ],
        max_retries=int(os.getenv("MCP_MAX_RETRIES", "3")),
        timeout_seconds=int(os.getenv("MCP_TIMEOUT", "30")),
        allowed_namespaces=os.getenv("MCP_ALLOWED_NAMESPACES", "default,kube-system,production").split(","),
        restricted_commands=os.getenv("MCP_RESTRICTED_COMMANDS", "delete,rm,destroy").split(",")
    )
    
    # RAG configuration
    rag_config = RAGConfig(
        similarity_threshold=float(os.getenv("RAG_SIMILARITY_THRESHOLD", "0.7")),
        max_context_documents=int(os.getenv("RAG_MAX_CONTEXT_DOCS", "5")),
        chunk_size=int(os.getenv("RAG_CHUNK_SIZE", "1000")),
        chunk_overlap=int(os.getenv("RAG_CHUNK_OVERLAP", "200")),
        rerank_top_k=int(os.getenv("RAG_RERANK_TOP_K", "10"))
    )
    
    return ResolverConfig(
        mongodb=mongodb_config,
        qdrant=qdrant_config,
        aws=aws_config,
        mcp=mcp_config,
        rag=rag_config,
        service_name=os.getenv("SERVICE_NAME", "k8s-rag-resolver"),
        service_port=int(os.getenv("SERVICE_PORT", "8080")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_concurrent_resolutions=int(os.getenv("MAX_CONCURRENT_RESOLUTIONS", "5")),
        auto_execute_safe_commands=os.getenv("AUTO_EXECUTE_SAFE_COMMANDS", "false").lower() == "true"
    )

# Safe commands that can be auto-executed
SAFE_KUBECTL_COMMANDS = [
    "get", "describe", "logs", "explain", "api-versions",
    "api-resources", "cluster-info", "version", "config",
    "top", "events"
]

# Dangerous commands that should never be auto-executed
DANGEROUS_KUBECTL_COMMANDS = [
    "delete", "rm", "destroy", "apply", "create", "replace",
    "patch", "edit", "rollout", "scale", "annotate", "label",
    "taint", "cordon", "drain", "uncordon"
]

# Resolution templates for different error types
RESOLUTION_TEMPLATES = {
    'image_pull': {
        'diagnosis_commands': [
            'kubectl describe pod {pod_name} -n {namespace}',
            'kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name}',
            'kubectl get secrets -n {namespace}',
        ],
        'common_fixes': [
            'kubectl delete pod {pod_name} -n {namespace}',  # Force recreate
            'kubectl rollout restart deployment {deployment_name} -n {namespace}',
        ],
        'report_sections': [
            'Image Registry Access',
            'Authentication Issues', 
            'Network Connectivity',
            'Image Tag Verification'
        ]
    },
    'resource_limit': {
        'diagnosis_commands': [
            'kubectl describe pod {pod_name} -n {namespace}',
            'kubectl top pod {pod_name} -n {namespace}',
            'kubectl describe nodes',
            'kubectl get limitrange -n {namespace}',
        ],
        'common_fixes': [
            'kubectl patch deployment {deployment_name} -n {namespace} -p \'{"spec":{"template":{"spec":{"containers":[{"name":"{container_name}","resources":{"limits":{"memory":"512Mi","cpu":"500m"}}}]}}}}\'',
        ],
        'report_sections': [
            'Resource Usage Analysis',
            'Cluster Capacity',
            'Resource Quotas',
            'Scaling Recommendations'
        ]
    },
    'network': {
        'diagnosis_commands': [
            'kubectl describe pod {pod_name} -n {namespace}',
            'kubectl get svc -n {namespace}',
            'kubectl get networkpolicies -n {namespace}',
            'kubectl describe endpoints -n {namespace}',
        ],
        'common_fixes': [
            'kubectl delete pod {pod_name} -n {namespace}',  # Network reset
        ],
        'report_sections': [
            'Service Discovery',
            'Network Policies',
            'DNS Configuration',
            'Connectivity Testing'
        ]
    },
    'storage': {
        'diagnosis_commands': [
            'kubectl describe pod {pod_name} -n {namespace}',
            'kubectl get pvc -n {namespace}',
            'kubectl get pv',
            'kubectl describe storageclass',
        ],
        'common_fixes': [],  # Storage issues usually need manual intervention
        'report_sections': [
            'PVC Status',
            'Storage Classes',
            'Volume Mounting',
            'Disk Space Analysis'
        ]
    },
    'node': {
        'diagnosis_commands': [
            'kubectl describe node {node_name}',
            'kubectl get nodes -o wide',
            'kubectl top nodes',
        ],
        'common_fixes': [],  # Node issues need careful handling
        'report_sections': [
            'Node Health',
            'Resource Pressure',
            'Kubelet Status',
            'System Services'
        ]
    }
}