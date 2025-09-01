"""
AI Analyzer Package for Kubernetes Error Analysis

This package provides comprehensive AI-powered analysis of Kubernetes errors
with multi-file architecture for better maintainability and scalability.
"""

__version__ = "2.0.0"
__author__ = "darkomni"
__description__ = "AI-powered Kubernetes error analysis system"

# Import main components for easy access
from .manager import KubernetesErrorAnalysisManager
from .config import load_analyzer_config, AIAnalyzerConfig
from .fetcher import ErrorLogFetcher
from .analyzer import AIErrorAnalyzer
from .storage import DebugResultsManager

# Export public API
__all__ = [
    'KubernetesErrorAnalysisManager',
    'load_analyzer_config', 
    'AIAnalyzerConfig',
    'ErrorLogFetcher',
    'AIErrorAnalyzer', 
    'DebugResultsManager'
]

# Package metadata
PACKAGE_INFO = {
    "name": "ai_analyzer",
    "version": __version__,
    "description": __description__,
    "author": __author__,
    "components": {
        "manager": "Main orchestration and analysis workflow management",
        "config": "Configuration management and error prompts", 
        "fetcher": "Error log fetching and filtering from MongoDB",
        "analyzer": "AI-powered error analysis using AWS Bedrock",
        "storage": "Debug results and solutions storage management"
    },
    "features": [
        "Multi-error type support (8+ error categories)",
        "Specialized AI prompts for each error type", 
        "Batch processing with priority ordering",
        "Comprehensive error statistics and trends",
        "Health monitoring and system status",
        "Automated cleanup and maintenance"
    ]
}