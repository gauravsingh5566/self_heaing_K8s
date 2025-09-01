from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import re

@dataclass
class LogEntry:
    """Data class for log entries"""
    timestamp: datetime
    log_type: str
    source: str
    namespace: str
    resource_name: str
    message: str
    level: str
    metadata: Dict[str, Any]
    has_error: bool = False
    error_type: Optional[str] = None
    error_severity: str = "INFO"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        return asdict(self)

class ErrorDetector:
    """Class to detect and categorize errors in log messages"""
    
    # Error patterns with their categories and severity levels
    ERROR_PATTERNS = {
        # Image Pull Errors
        'image_pull': {
            'patterns': [
                r'ErrImagePull',
                r'ImagePullBackOff',
                r'Failed to pull image',
                r'pull access denied',
                r'repository does not exist',
                r'manifest unknown'
            ],
            'severity': 'HIGH'
        },
        
        # Resource Errors
        'resource_limit': {
            'patterns': [
                r'Insufficient memory',
                r'Insufficient cpu',
                r'memory limit exceeded',
                r'cpu throttling',
                r'OOMKilled',
                r'Evicted'
            ],
            'severity': 'HIGH'
        },
        
        # Network Errors
        'network': {
            'patterns': [
                r'connection refused',
                r'network unreachable',
                r'timeout',
                r'DNS resolution failed',
                r'service unavailable',
                r'connection reset'
            ],
            'severity': 'MEDIUM'
        },
        
        # Storage Errors
        'storage': {
            'patterns': [
                r'no space left on device',
                r'disk full',
                r'volume mount failed',
                r'PVC not bound',
                r'storage class not found'
            ],
            'severity': 'HIGH'
        },
        
        # Authentication/Authorization Errors
        'auth': {
            'patterns': [
                r'permission denied',
                r'unauthorized',
                r'forbidden',
                r'authentication failed',
                r'invalid credentials',
                r'access denied'
            ],
            'severity': 'MEDIUM'
        },
        
        # Pod/Container Errors
        'container': {
            'patterns': [
                r'CrashLoopBackOff',
                r'CreateContainerError',
                r'InvalidImageName',
                r'container failed',
                r'exit code',
                r'startup probe failed',
                r'readiness probe failed',
                r'liveness probe failed'
            ],
            'severity': 'HIGH'
        },
        
        # Node Errors
        'node': {
            'patterns': [
                r'node not ready',
                r'node unreachable',
                r'kubelet stopped',
                r'disk pressure',
                r'memory pressure',
                r'node cordoned'
            ],
            'severity': 'CRITICAL'
        },
        
        # Application Errors
        'application': {
            'patterns': [
                r'ERROR',
                r'FATAL',
                r'exception',
                r'stack trace',
                r'failed to start',
                r'panic',
                r'segmentation fault'
            ],
            'severity': 'MEDIUM'
        }
    }
    
    @classmethod
    def detect_error(cls, message: str, log_level: str = "INFO") -> tuple[bool, Optional[str], str]:
        """
        Detect if a log message contains an error and categorize it
        
        Returns:
            tuple: (has_error, error_type, severity)
        """
        if not message:
            return False, None, "INFO"
        
        message_lower = message.lower()
        
        # First check log level for obvious errors
        if log_level.upper() in ['ERROR', 'FATAL', 'CRITICAL']:
            return True, 'application', 'HIGH'
        
        # Check against error patterns
        for error_type, config in cls.ERROR_PATTERNS.items():
            for pattern in config['patterns']:
                if re.search(pattern, message_lower, re.IGNORECASE):
                    return True, error_type, config['severity']
        
        return False, None, "INFO"
    
    @classmethod
    def is_critical_error(cls, error_type: str, severity: str) -> bool:
        """Check if an error is critical and needs immediate attention"""
        return severity in ['CRITICAL', 'HIGH']

class ResourceParser:
    """Utility class for parsing Kubernetes resource specifications"""
    
    @staticmethod
    def parse_cpu(cpu_str: str) -> int:
        """Parse CPU string to millicores"""
        if not cpu_str or cpu_str == '0':
            return 0
        if cpu_str.endswith('m'):
            return int(cpu_str[:-1])
        return int(float(cpu_str) * 1000)
    
    @staticmethod
    def parse_memory(memory_str: str) -> int:
        """Parse memory string to MiB"""
        if not memory_str or memory_str == '0':
            return 0
        
        units = {'Ki': 1/1024, 'Mi': 1, 'Gi': 1024, 'Ti': 1024*1024}
        for unit, multiplier in units.items():
            if memory_str.endswith(unit):
                return int(int(memory_str[:-2]) * multiplier)
        
        # Assume bytes if no unit
        return int(int(memory_str) / (1024*1024))

class LogEntryFactory:
    """Factory class for creating different types of log entries"""
    
    @staticmethod
    def create_container_log(pod, logs: str, config) -> LogEntry:
        """Create a container log entry"""
        # Detect errors in the log message
        has_error, error_type, severity = ErrorDetector.detect_error(logs)
        
        return LogEntry(
            timestamp=datetime.now(timezone.utc),
            log_type="container_logs",
            source="pod",
            namespace=pod.metadata.namespace,
            resource_name=pod.metadata.name,
            message=logs[-config.logging.message_max_length:],
            level="INFO",
            metadata={
                "pod_phase": pod.status.phase,
                "node_name": pod.spec.node_name,
                "labels": pod.metadata.labels or {}
            },
            has_error=has_error,
            error_type=error_type,
            error_severity=severity
        )
    
    @staticmethod
    def create_event_log(event) -> LogEntry:
        """Create a Kubernetes event log entry"""
        event_time = event.last_timestamp.replace(tzinfo=timezone.utc) if event.last_timestamp else datetime.now(timezone.utc)
        
        # Detect errors in event message
        has_error, error_type, severity = ErrorDetector.detect_error(event.message, event.type)
        
        return LogEntry(
            timestamp=event_time,
            log_type=f"{event.involved_object.kind.lower()}_events",
            source="kubernetes_events",
            namespace=event.namespace or "default",
            resource_name=event.involved_object.name,
            message=event.message,
            level=event.type,
            metadata={
                "reason": event.reason,
                "source": event.source.component if event.source else "",
                "count": event.count,
                "kind": event.involved_object.kind,
                "api_version": event.involved_object.api_version
            },
            has_error=has_error,
            error_type=error_type,
            error_severity=severity
        )
    
    @staticmethod
    def create_node_status_log(node) -> LogEntry:
        """Create a node status log entry"""
        conditions = []
        has_error = False
        error_type = None
        severity = "INFO"
        
        if node.status.conditions:
            for condition in node.status.conditions:
                conditions.append({
                    "type": condition.type,
                    "status": condition.status,
                    "reason": condition.reason,
                    "message": condition.message
                })
                
                # Check for node errors
                if condition.status == "True" and condition.type in ["DiskPressure", "MemoryPressure", "PIDPressure"]:
                    has_error = True
                    error_type = "node"
                    severity = "CRITICAL"
                elif condition.status == "False" and condition.type == "Ready":
                    has_error = True
                    error_type = "node"
                    severity = "CRITICAL"
        
        status_message = f"Node status check: {getattr(node.status, 'phase', 'Unknown')}"
        if has_error:
            status_message += f" - Node has issues: {error_type}"
        
        return LogEntry(
            timestamp=datetime.now(timezone.utc),
            log_type="node_status",
            source="node",
            namespace="kube-system",
            resource_name=node.metadata.name,
            message=status_message,
            level="INFO",
            metadata={
                "conditions": conditions,
                "capacity": dict(node.status.capacity) if node.status.capacity else {},
                "allocatable": dict(node.status.allocatable) if node.status.allocatable else {},
                "node_info": {
                    "os_image": node.status.node_info.os_image,
                    "kernel_version": node.status.node_info.kernel_version,
                    "container_runtime_version": node.status.node_info.container_runtime_version
                } if node.status.node_info else {}
            },
            has_error=has_error,
            error_type=error_type,
            error_severity=severity
        )
    
    @staticmethod
    def create_system_component_log(pod, logs: str, component: str, config) -> LogEntry:
        """Create a system component log entry"""
        # Detect errors in system component logs
        has_error, error_type, severity = ErrorDetector.detect_error(logs)
        
        return LogEntry(
            timestamp=datetime.now(timezone.utc),
            log_type=f"{component}_logs",
            source="system_component",
            namespace=pod.metadata.namespace,
            resource_name=pod.metadata.name,
            message=logs[-500:],
            level="INFO",
            metadata={
                "component": component,
                "labels": pod.metadata.labels or {}
            },
            has_error=has_error,
            error_type=error_type,
            error_severity=severity
        )
    
    @staticmethod
    def create_eks_control_plane_log(event: Dict[str, Any], log_group: str, log_stream: str, cluster_name: str) -> LogEntry:
        """Create an EKS control plane log entry"""
        # Determine log type based on stream name
        log_type = "api_server_logs"
        if "audit" in log_stream:
            log_type = "audit_logs"
        elif "authenticator" in log_stream:
            log_type = "authenticator_logs"
        elif "controllerManager" in log_stream:
            log_type = "controller_manager_logs"
        elif "scheduler" in log_stream:
            log_type = "scheduler_logs"
        
        # Detect errors in control plane logs
        has_error, error_type, severity = ErrorDetector.detect_error(event['message'])
        
        return LogEntry(
            timestamp=datetime.fromtimestamp(event['timestamp'] / 1000, timezone.utc),
            log_type=log_type,
            source="eks_control_plane",
            namespace="kube-system",
            resource_name=cluster_name,
            message=event['message'][:1000],
            level="INFO",
            metadata={
                "log_group": log_group,
                "log_stream": log_stream
            },
            has_error=has_error,
            error_type=error_type,
            error_severity=severity
        )
    
    @staticmethod
    def create_resource_metrics_log(pod, cpu_requests: int, memory_requests: int, 
                                   cpu_limits: int, memory_limits: int, restart_count: int) -> LogEntry:
        """Create a resource metrics log entry"""
        # Check for resource-related issues
        has_error = False
        error_type = None
        severity = "INFO"
        
        if restart_count > 5:
            has_error = True
            error_type = "container"
            severity = "MEDIUM"
        
        return LogEntry(
            timestamp=datetime.now(timezone.utc),
            log_type="resource_metrics",
            source="metrics",
            namespace=pod.metadata.namespace,
            resource_name=pod.metadata.name,
            message=f"Resource allocation - CPU: {cpu_requests}m/{cpu_limits}m, Memory: {memory_requests}Mi/{memory_limits}Mi, Restarts: {restart_count}",
            level="INFO",
            metadata={
                "cpu_requests": cpu_requests,
                "memory_requests": memory_requests,
                "cpu_limits": cpu_limits,
                "memory_limits": memory_limits,
                "restart_count": restart_count
            },
            has_error=has_error,
            error_type=error_type,
            error_severity=severity
        )

class TimeUtils:
    """Utility class for time-related operations"""
    
    @staticmethod
    def is_recent_event(event_time: datetime, window_seconds: int = 300) -> bool:
        """Check if an event is within the recent time window"""
        now = datetime.now(timezone.utc)
        return (now - event_time).total_seconds() < window_seconds
    
    @staticmethod
    def get_start_time(hours_ago: int) -> datetime:
        """Get datetime for N hours ago"""
        return datetime.now(timezone.utc) - timedelta(hours=hours_ago)