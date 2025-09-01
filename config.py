import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class MongoDBConfig:
    """MongoDB configuration"""
    uri: str
    database_name: str
    collection_name: str

@dataclass
class AWSConfig:
    """AWS configuration"""
    region: str
    profile: str
    cluster_name: str

@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str
    format: str
    collection_interval: int  # seconds
    tail_lines: int
    message_max_length: int
    recent_events_window: int  # seconds

@dataclass
class AppConfig:
    """Main application configuration"""
    mongodb: MongoDBConfig
    aws: AWSConfig
    logging: LoggingConfig

def load_config() -> AppConfig:
    """Load configuration from environment variables with defaults"""
    
    # MongoDB configuration
    mongodb_config = MongoDBConfig(
        uri=os.getenv('MONGODB_URI', 'mongodb://admin:SKMO7frslkjsdh546fchjkhg@35.94.146.89:27017/'),
        database_name=os.getenv('DATABASE_NAME', 'k8s_logs'),
        collection_name=os.getenv('COLLECTION_NAME', 'logs')
    )
    
    # AWS configuration
    aws_config = AWSConfig(
        region=os.getenv('AWS_REGION', 'us-west-2'),
        profile=os.getenv('AWS_PROFILE', 'finchat'),
        cluster_name=os.getenv('CLUSTER_NAME', 'my-eks-cluster')
    )
    
    # Logging configuration
    logging_config = LoggingConfig(
        level=os.getenv('LOG_LEVEL', 'INFO'),
        format=os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        collection_interval=int(os.getenv('COLLECTION_INTERVAL', '30')),
        tail_lines=int(os.getenv('TAIL_LINES', '50')),
        message_max_length=int(os.getenv('MESSAGE_MAX_LENGTH', '1000')),
        recent_events_window=int(os.getenv('RECENT_EVENTS_WINDOW', '300'))
    )
    
    return AppConfig(
        mongodb=mongodb_config,
        aws=aws_config,
        logging=logging_config
    )

# System component configurations
SYSTEM_COMPONENTS = [
    {
        "namespace": "kube-system",
        "label": "k8s-app=kube-dns",
        "component": "coredns"
    },
    {
        "namespace": "kube-system",
        "label": "k8s-app=kube-proxy",
        "component": "kube-proxy"
    },
    {
        "namespace": "kube-system",
        "label": "app.kubernetes.io/name=aws-load-balancer-controller",
        "component": "aws_lb_controller"
    }
]