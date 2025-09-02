"""
RAG Resolver - AI-powered Kubernetes Error Resolution Microservice

A comprehensive microservice that combines:
- RAG (Retrieval Augmented Generation) for intelligent error analysis
- Qdrant vector database for storing resolution knowledge
- MCP (Model Context Protocol) for safe kubectl command execution
- AWS Bedrock integration for AI-powered solutions
- FastAPI service for REST API access

Features:
- Automatic error resolution with safety controls
- Comprehensive resolution reports for manual intervention
- Vector-based similarity search for past resolutions
- Safe command execution in EKS clusters
- Real-time resolution status tracking
- Feedback learning for continuous improvement
"""

__version__ = "1.0.0"
__author__ = "darkomni"
__description__ = "RAG-based Kubernetes Error Resolution Microservice"

# Export main components
from .config import load_resolver_config, ResolverConfig
from .rag_engine import RAGEngine
from .vector_store import VectorStore
from .mcp_executor import MCPExecutor
from .service import app

__all__ = [
    'ResolverConfig',
    'load_resolver_config',
    'RAGEngine',
    'VectorStore', 
    'MCPExecutor',
    'app'
]

# Service metadata
SERVICE_INFO = {
    "name": "rag_resolver",
    "version": __version__,
    "description": __description__,
    "author": __author__,
    "components": {
        "rag_engine": "Main RAG-based resolution engine",
        "vector_store": "Qdrant-based knowledge storage",
        "mcp_executor": "Safe kubectl command execution",
        "service": "FastAPI REST API service",
        "config": "Configuration management"
    },
    "capabilities": [
        "Intelligent error analysis using RAG",
        "Vector similarity search for resolutions",
        "Safe kubectl command execution via MCP",
        "Comprehensive resolution reporting",
        "Real-time resolution status tracking",
        "Feedback-based learning system",
        "Multi-error type support",
        "AWS Bedrock LLM integration",
        "EKS cluster integration",
        "Async resolution processing"
    ],
    "supported_error_types": [
        "image_pull",
        "resource_limit", 
        "network",
        "storage",
        "auth",
        "container",
        "node",
        "application"
    ]
}