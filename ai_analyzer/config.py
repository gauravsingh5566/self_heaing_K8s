import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class MongoDBConfig:
    """MongoDB configuration for AI analyzer"""
    uri: str
    database_name: str
    logs_collection: str
    debug_collection: str

@dataclass
class AWSConfig:
    """AWS Bedrock configuration"""
    region: str
    profile: str
    model_id: str

@dataclass
class AnalysisConfig:
    """Analysis configuration"""
    batch_size: int
    max_tokens: int
    temperature: float
    top_p: float
    hours_back_default: int
    critical_error_types: List[str]

@dataclass
class AIAnalyzerConfig:
    """Main AI analyzer configuration"""
    mongodb: MongoDBConfig
    aws: AWSConfig
    analysis: AnalysisConfig

def load_analyzer_config() -> AIAnalyzerConfig:
    """Load AI analyzer configuration from environment variables"""
    
    # MongoDB configuration
    mongodb_config = MongoDBConfig(
        uri=os.getenv("MONGO_URI", "mongodb://admin:SKMO7frslkjsdh546fchjkhg@35.94.146.89:27017/"),
        database_name=os.getenv("DB_NAME", "k8s_logs"),
        logs_collection=os.getenv("COLLECTION_NAME", "logs"),
        debug_collection=os.getenv("DEBUG_COLLECTION", "debug_results")
    )
    
    # AWS configuration
    aws_config = AWSConfig(
        region=os.getenv("AWS_REGION", "us-west-2"),
        profile=os.getenv("AWS_PROFILE", "finchat"),
        model_id=os.getenv("MODEL_ID", "openai.gpt-oss-20b-1:0")
    )
    
    # Analysis configuration
    analysis_config = AnalysisConfig(
        batch_size=int(os.getenv("ANALYSIS_BATCH_SIZE", "20")),
        max_tokens=int(os.getenv("MAX_TOKENS", "1000")),
        temperature=float(os.getenv("TEMPERATURE", "0.3")),
        top_p=float(os.getenv("TOP_P", "0.9")),
        hours_back_default=int(os.getenv("HOURS_BACK_DEFAULT", "1")),
        critical_error_types=["node", "resource_limit", "storage", "image_pull"]
    )
    
    return AIAnalyzerConfig(
        mongodb=mongodb_config,
        aws=aws_config,
        analysis=analysis_config
    )

# Error-specific prompts for different error types
ERROR_PROMPTS = {
    'image_pull': """
    You are an expert Kubernetes troubleshooter specializing in container image issues.
    
    I encountered this image pull error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide a comprehensive solution for this image pull error including:
    1. Root cause analysis
    2. Step-by-step debugging commands
    3. Multiple resolution approaches
    4. Prevention strategies
    
    Format your response with clear sections and actionable kubectl commands.
    """,
    
    'resource_limit': """
    You are an expert Kubernetes troubleshooter specializing in resource management.
    
    I encountered this resource-related error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide solutions for this resource issue including:
    1. Resource analysis and bottleneck identification
    2. Commands to check resource usage
    3. Scaling and optimization strategies
    4. Resource quota and limit recommendations
    
    Include specific kubectl commands and YAML examples.
    """,
    
    'network': """
    You are an expert Kubernetes troubleshooter specializing in networking issues.
    
    I encountered this network-related error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide comprehensive network troubleshooting including:
    1. Network connectivity diagnosis
    2. Service discovery and DNS troubleshooting
    3. Network policy analysis
    4. Ingress and load balancer debugging
    
    Include diagnostic commands and configuration examples.
    """,
    
    'container': """
    You are an expert Kubernetes troubleshooter specializing in container lifecycle issues.
    
    I encountered this container-related error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide solutions for this container issue including:
    1. Container startup and health check analysis
    2. Application debugging techniques
    3. Configuration and environment troubleshooting
    4. Restart policy and probe optimization
    
    Include debugging commands and configuration fixes.
    """,
    
    'storage': """
    You are an expert Kubernetes troubleshooter specializing in storage issues.
    
    I encountered this storage-related error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide comprehensive storage troubleshooting including:
    1. PVC and PV status analysis
    2. Storage class and provisioner debugging
    3. Volume mount and permissions issues
    4. Disk space and performance optimization
    
    Include storage diagnostic commands and YAML examples.
    """,
    
    'auth': """
    You are an expert Kubernetes troubleshooter specializing in authentication and authorization.
    
    I encountered this auth-related error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide comprehensive auth troubleshooting including:
    1. RBAC and service account analysis
    2. Permission and role binding verification
    3. Authentication mechanism debugging
    4. Security policy recommendations
    
    Include auth diagnostic commands and RBAC examples.
    """,
    
    'node': """
    You are an expert Kubernetes troubleshooter specializing in node and cluster issues.
    
    I encountered this node-related error in Kubernetes:
    
    Namespace: {namespace}
    Resource: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide comprehensive node troubleshooting including:
    1. Node health and resource analysis
    2. Kubelet and system service diagnostics
    3. Storage and disk space management
    4. Node recovery and maintenance procedures
    
    Include node management commands and monitoring strategies.
    """,
    
    'application': """
    You are an expert Kubernetes troubleshooter specializing in application errors.
    
    I encountered this application error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Provide comprehensive application troubleshooting including:
    1. Application log analysis and error identification
    2. Configuration and environment variable debugging
    3. Health check and readiness probe optimization
    4. Performance and scaling recommendations
    
    Include debugging commands and application configuration examples.
    """,
    
    'default': """
    You are an expert Kubernetes troubleshooter.
    
    I encountered this error in Kubernetes:
    
    Namespace: {namespace}
    Pod: {pod}
    Node: {node}
    Error Type: {error_type}
    Severity: {error_severity}
    Error Log:
    {message}
    
    Analyze this error and provide:
    1. Root cause analysis
    2. Step-by-step debugging approach
    3. Multiple resolution strategies
    4. Prevention recommendations
    
    Include specific kubectl commands and configuration examples.
    """
}