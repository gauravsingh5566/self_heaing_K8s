#!/usr/bin/env python3
"""
Enhanced Log collectors with error detection for different Kubernetes components
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
from abc import ABC, abstractmethod

from kubernetes import client
import boto3
from botocore.exceptions import ClientError

# Import the enhanced models with error detection
from models import LogEntry, LogEntryFactory, ResourceParser, TimeUtils, ErrorDetector
from config import AppConfig, SYSTEM_COMPONENTS

logger = logging.getLogger(__name__)

class BaseCollector(ABC):
    """Base class for all log collectors with error detection capabilities"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.error_detector = ErrorDetector()
    
    @abstractmethod
    def collect_logs(self) -> List[LogEntry]:
        """Collect logs and return list of LogEntry objects with error detection"""
        pass

class PodLogsCollector(BaseCollector):
    """Enhanced collector for pod/container logs with comprehensive error detection"""
    
    def __init__(self, config: AppConfig, k8s_client: client.CoreV1Api):
        super().__init__(config)
        self.k8s_client = k8s_client
    
    def collect_logs(self) -> List[LogEntry]:
        """Collect pod logs with enhanced error detection and diagnostics"""
        logger.info("Collecting pod logs with error detection...")
        log_entries = []
        error_count = 0
        
        try:
            pods = self.k8s_client.list_pod_for_all_namespaces(watch=False)
            
            for pod in pods.items:
                try:
                    # ✅ Case 1: Pod is Running/Finished → Fetch normal logs
                    if pod.status.phase in ['Running', 'Failed', 'Succeeded']:
                        try:
                            logs = self.k8s_client.read_namespaced_pod_log(
                                name=pod.metadata.name,
                                namespace=pod.metadata.namespace,
                                tail_lines=self.config.logging.tail_lines,
                                timestamps=True
                            )
                            
                            if logs:
                                log_entry = LogEntryFactory.create_container_log(pod, logs, self.config)
                                log_entries.append(log_entry)
                                
                                if log_entry.has_error:
                                    error_count += 1
                                    logger.warning(f"Error detected in pod {pod.metadata.name}: {log_entry.error_type}")
                        except Exception as e:
                            logger.debug(f"Could not get logs for running pod {pod.metadata.name}: {e}")

                    # ✅ Case 2: Pod not producing logs (Pending, ImagePullBackOff, etc.)
                    # Create diagnostic entries for these pods
                    elif pod.status.phase in ['Pending', 'Unknown']:
                        # Analyze why the pod is not running
                        diagnostic_info = self._analyze_pod_issues(pod)
                        
                        has_error = True
                        error_type = diagnostic_info.get('primary_issue', 'scheduling')
                        error_severity = diagnostic_info.get('severity', 'MEDIUM')
                        
                        # Create diagnostic log entry
                        diag_entry = LogEntry(
                            timestamp=datetime.now(timezone.utc),
                            log_type="pod_diagnostics",
                            source="pod_diagnostics",
                            namespace=pod.metadata.namespace,
                            resource_name=pod.metadata.name,
                            message=diagnostic_info.get('message', f"Pod {pod.metadata.name} is {pod.status.phase}"),
                            level="WARNING" if error_severity in ['LOW', 'MEDIUM'] else "ERROR",
                            metadata={
                                "pod_phase": pod.status.phase,
                                "node_name": pod.spec.node_name,
                                "labels": pod.metadata.labels or {},
                                "diagnostic_type": "pod_status",
                                "issues": diagnostic_info.get('issues', []),
                                "node_selectors": pod.spec.node_selector or {},
                                "tolerations": [str(t) for t in (pod.spec.tolerations or [])]
                            },
                            has_error=has_error,
                            error_type=error_type,
                            error_severity=error_severity
                        )
                        log_entries.append(diag_entry)
                        error_count += 1
                        
                        logger.warning(f"Pod scheduling issue detected: {pod.metadata.name} - {error_type}: {diagnostic_info.get('message')}")

                    # ✅ Case 3: Container status issues (ImagePullBackOff, CrashLoopBackOff, etc.)
                    if pod.status.container_statuses:
                        for container_status in pod.status.container_statuses:
                            if container_status.state and not container_status.state.running:
                                container_issue = self._analyze_container_state(container_status, pod)
                                
                                if container_issue['has_error']:
                                    container_entry = LogEntry(
                                        timestamp=datetime.now(timezone.utc),
                                        log_type="container_diagnostics",
                                        source="container_diagnostics",
                                        namespace=pod.metadata.namespace,
                                        resource_name=f"{pod.metadata.name}/{container_status.name}",
                                        message=container_issue['message'],
                                        level="ERROR" if container_issue['severity'] in ['HIGH', 'CRITICAL'] else "WARNING",
                                        metadata={
                                            "container_name": container_status.name,
                                            "pod_phase": pod.status.phase,
                                            "container_state": container_issue['state_type'],
                                            "restart_count": container_status.restart_count,
                                            "image": container_status.image
                                        },
                                        has_error=True,
                                        error_type=container_issue['error_type'],
                                        error_severity=container_issue['severity']
                                    )
                                    log_entries.append(container_entry)
                                    error_count += 1

                except Exception as e:
                    logger.debug(f"Could not analyze pod {pod.metadata.name}: {e}")
                    
                    # Create an error entry for the collection failure if it seems significant
                    if "Forbidden" in str(e) or "Unauthorized" in str(e):
                        error_entry = LogEntry(
                            timestamp=datetime.now(timezone.utc),
                            log_type="collector_errors",
                            source="pod_collector",
                            namespace=pod.metadata.namespace,
                            resource_name=pod.metadata.name,
                            message=f"Failed to collect logs: {str(e)}",
                            level="ERROR",
                            metadata={"collection_error": True},
                            has_error=True,
                            error_type="auth",
                            error_severity="MEDIUM"
                        )
                        log_entries.append(error_entry)
                        error_count += 1
                        
        except Exception as e:
            logger.error(f"Error collecting pod logs: {e}")
        
        logger.info(f"Collected {len(log_entries)} pod log entries, {error_count} with errors")
        return log_entries

    def _analyze_pod_issues(self, pod) -> Dict[str, Any]:
        """Analyze why a pod is not running and return diagnostic information"""
        issues = []
        primary_issue = "unknown"
        severity = "MEDIUM"
        message = f"Pod {pod.metadata.name} is {pod.status.phase}"
        
        # Check node selector issues
        if pod.spec.node_selector:
            issues.append(f"Node selector required: {pod.spec.node_selector}")
            primary_issue = "node_selector_mismatch"
            message = f"Pod requires nodes with labels {pod.spec.node_selector} but none available"
            severity = "HIGH"
        
        # Check scheduling conditions
        if pod.status.conditions:
            for condition in pod.status.conditions:
                if condition.type == "PodScheduled" and condition.status == "False":
                    if condition.reason:
                        issues.append(f"Scheduling issue: {condition.reason}")
                        if condition.message:
                            issues.append(f"Details: {condition.message}")
                            message = condition.message
                    
                    # Determine specific scheduling issues
                    if "node affinity" in condition.message.lower() or "node selector" in condition.message.lower():
                        primary_issue = "node_selector_mismatch"
                        severity = "HIGH"
                    elif "insufficient" in condition.message.lower():
                        primary_issue = "resource_shortage"
                        severity = "CRITICAL"
                    elif "taint" in condition.message.lower():
                        primary_issue = "node_taints"
                        severity = "MEDIUM"
        
        # Check resource requests vs availability
        if pod.spec.containers:
            for container in pod.spec.containers:
                if container.resources and container.resources.requests:
                    cpu_req = container.resources.requests.get('cpu')
                    memory_req = container.resources.requests.get('memory')
                    if cpu_req or memory_req:
                        issues.append(f"Resource requests: CPU={cpu_req}, Memory={memory_req}")
        
        # Check tolerations
        if pod.spec.tolerations:
            issues.append(f"Has {len(pod.spec.tolerations)} tolerations")
        
        # Check if stuck for too long
        if pod.metadata.creation_timestamp:
            age = datetime.now(timezone.utc) - pod.metadata.creation_timestamp.replace(tzinfo=timezone.utc)
            if age.total_seconds() > 300:  # 5 minutes
                issues.append(f"Pod stuck for {age}")
                if age.total_seconds() > 900:  # 15 minutes
                    severity = "HIGH"
        
        return {
            "issues": issues,
            "primary_issue": primary_issue,
            "severity": severity,
            "message": message,
            "node_selector": pod.spec.node_selector or {},
            "pod_phase": pod.status.phase,
            "age_seconds": age.total_seconds() if pod.metadata.creation_timestamp else 0
        }

    def _analyze_container_state(self, container_status, pod) -> Dict[str, Any]:
        """Analyze container state issues"""
        has_error = False
        error_type = "unknown"
        severity = "MEDIUM"
        message = f"Container {container_status.name} status unknown"
        state_type = "unknown"
        
        if container_status.state.waiting:
            has_error = True
            state_type = "waiting"
            reason = container_status.state.waiting.reason
            
            if reason == "ImagePullBackOff":
                error_type = "image_pull"
                severity = "HIGH"
                message = f"Cannot pull image: {container_status.image}"
            elif reason == "ErrImagePull":
                error_type = "image_pull"
                severity = "HIGH"
                message = f"Error pulling image: {container_status.image}"
            elif reason == "InvalidImageName":
                error_type = "image_pull"
                severity = "CRITICAL"
                message = f"Invalid image name: {container_status.image}"
            elif reason == "CreateContainerConfigError":
                error_type = "config"
                severity = "HIGH"
                message = f"Container configuration error: {container_status.state.waiting.message}"
            else:
                error_type = "container_waiting"
                message = f"Container waiting: {reason}"
        
        elif container_status.state.terminated:
            has_error = True
            state_type = "terminated"
            reason = container_status.state.terminated.reason
            exit_code = container_status.state.terminated.exit_code
            
            if exit_code != 0:
                error_type = "container_crash"
                severity = "HIGH" if container_status.restart_count > 3 else "MEDIUM"
                message = f"Container crashed with exit code {exit_code}, restarts: {container_status.restart_count}"
            else:
                has_error = False  # Normal termination
                message = f"Container terminated normally: {reason}"
        
        return {
            "has_error": has_error,
            "error_type": error_type,
            "severity": severity,
            "message": message,
            "state_type": state_type,
            "restart_count": container_status.restart_count
        }

class EventsCollector(BaseCollector):
    """Enhanced collector for Kubernetes events with error classification"""
    
    def __init__(self, config: AppConfig, k8s_client: client.CoreV1Api):
        super().__init__(config)
        self.k8s_client = k8s_client
    
    def collect_logs(self) -> List[LogEntry]:
        """Collect Kubernetes events with enhanced error detection"""
        logger.info("Collecting Kubernetes events with error detection...")
        log_entries = []
        error_count = 0
        
        try:
            events = self.k8s_client.list_event_for_all_namespaces(watch=False)
            
            for event in events.items:
                if event.last_timestamp:
                    event_time = event.last_timestamp.replace(tzinfo=timezone.utc)
                    
                    if TimeUtils.is_recent_event(event_time, self.config.logging.recent_events_window):
                        log_entry = LogEntryFactory.create_event_log(event)
                        log_entries.append(log_entry)
                        
                        if log_entry.has_error:
                            error_count += 1
                            logger.info(f"Error event detected: {event.reason} for {event.involved_object.kind}/{event.involved_object.name}")
                            
        except Exception as e:
            logger.error(f"Error collecting events: {e}")
        
        logger.info(f"Collected {len(log_entries)} event entries, {error_count} with errors")
        return log_entries

class NodeLogsCollector(BaseCollector):
    """Enhanced collector for node-related logs with health monitoring"""
    
    def __init__(self, config: AppConfig, k8s_client: client.CoreV1Api):
        super().__init__(config)
        self.k8s_client = k8s_client
    
    def collect_logs(self) -> List[LogEntry]:
        """Collect node logs with comprehensive health checking"""
        logger.info("Collecting node logs with error detection...")
        log_entries = []
        error_count = 0
        
        try:
            nodes = self.k8s_client.list_node(watch=False)
            
            for node in nodes.items:
                log_entry = LogEntryFactory.create_node_status_log(node)
                log_entries.append(log_entry)
                
                if log_entry.has_error:
                    error_count += 1
                    logger.warning(f"Node error detected: {node.metadata.name} - {log_entry.error_type}")
                    
        except Exception as e:
            logger.error(f"Error collecting node logs: {e}")
        
        logger.info(f"Collected {len(log_entries)} node entries, {error_count} with errors")
        return log_entries

class SystemComponentLogsCollector(BaseCollector):
    """Enhanced collector for system component logs with error tracking"""
    
    def __init__(self, config: AppConfig, k8s_client: client.CoreV1Api):
        super().__init__(config)
        self.k8s_client = k8s_client
    
    def collect_logs(self) -> List[LogEntry]:
        """Collect system component logs with error detection"""
        logger.info("Collecting system component logs with error detection...")
        log_entries = []
        error_count = 0
        
        for comp in SYSTEM_COMPONENTS:
            try:
                pods = self.k8s_client.list_namespaced_pod(
                    namespace=comp["namespace"],
                    label_selector=comp["label"]
                )
                
                for pod in pods.items:
                    if pod.status.phase == 'Running':
                        try:
                            logs = self.k8s_client.read_namespaced_pod_log(
                                name=pod.metadata.name,
                                namespace=comp["namespace"],
                                tail_lines=20,
                                timestamps=True
                            )
                            
                            if logs:
                                log_entry = LogEntryFactory.create_system_component_log(
                                    pod, logs, comp["component"], self.config
                                )
                                log_entries.append(log_entry)
                                
                                if log_entry.has_error:
                                    error_count += 1
                                    logger.warning(f"System component error: {comp['component']} - {log_entry.error_type}")
                                
                        except Exception as e:
                            logger.debug(f"Could not get logs for {comp['component']} pod {pod.metadata.name}: {e}")
                            
                            # Create error entry for system component collection failure
                            error_entry = LogEntry(
                                timestamp=datetime.now(timezone.utc),
                                log_type=f"{comp['component']}_logs",
                                source="system_component_collector",
                                namespace=comp["namespace"],
                                resource_name=pod.metadata.name,
                                message=f"Failed to collect {comp['component']} logs: {str(e)}",
                                level="ERROR",
                                metadata={"component": comp["component"], "collection_error": True},
                                has_error=True,
                                error_type="container",
                                error_severity="MEDIUM"
                            )
                            log_entries.append(error_entry)
                            error_count += 1
                            
            except Exception as e:
                logger.error(f"Error collecting {comp['component']} logs: {e}")
        
        logger.info(f"Collected {len(log_entries)} system component entries, {error_count} with errors")
        return log_entries

class EKSControlPlaneCollector(BaseCollector):
    """Enhanced collector for EKS control plane logs with error analysis"""
    
    def __init__(self, config: AppConfig, logs_client):
        super().__init__(config)
        self.logs_client = logs_client
    
    def collect_logs(self) -> List[LogEntry]:
        """Collect EKS control plane logs with error detection"""
        if not self.logs_client:
            return []
            
        logger.info("Collecting EKS control plane logs with error detection...")
        log_entries = []
        error_count = 0
        
        log_groups = [f"/aws/eks/{self.config.aws.cluster_name}/cluster"]
        
        for log_group in log_groups:
            try:
                streams = self.logs_client.describe_log_streams(
                    logGroupName=log_group,
                    orderBy='LastEventTime',
                    descending=True,
                    limit=5
                )
                
                for stream in streams['logStreams']:
                    try:
                        events = self.logs_client.get_log_events(
                            logGroupName=log_group,
                            logStreamName=stream['logStreamName'],
                            startTime=int((datetime.now().timestamp() - self.config.logging.recent_events_window) * 1000),
                            limit=10
                        )
                        
                        for event in events['events']:
                            log_entry = LogEntryFactory.create_eks_control_plane_log(
                                event, log_group, stream['logStreamName'], self.config.aws.cluster_name
                            )
                            log_entries.append(log_entry)
                            
                            if log_entry.has_error:
                                error_count += 1
                                logger.warning(f"Control plane error detected in {stream['logStreamName']}: {log_entry.error_type}")
                            
                    except Exception as e:
                        logger.debug(f"Could not get events from stream {stream['logStreamName']}: {e}")
                        
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logger.debug(f"Log group {log_group} not found - control plane logging may not be enabled")
                else:
                    logger.error(f"Error accessing log group {log_group}: {e}")
            except Exception as e:
                logger.error(f"Error collecting control plane logs: {e}")
        
        logger.info(f"Collected {len(log_entries)} control plane entries, {error_count} with errors")
        return log_entries

class ResourceMetricsCollector(BaseCollector):
    """Enhanced collector for resource usage metrics with anomaly detection"""
    
    def __init__(self, config: AppConfig, k8s_client: client.CoreV1Api):
        super().__init__(config)
        self.k8s_client = k8s_client
        self.parser = ResourceParser()
    
    def collect_logs(self) -> List[LogEntry]:
        """Collect resource metrics with error detection for resource issues"""
        logger.info("Collecting resource metrics with error detection...")
        log_entries = []
        error_count = 0
        
        try:
            pods = self.k8s_client.list_pod_for_all_namespaces(watch=False)
            
            for pod in pods.items:
                if pod.status.phase == 'Running':
                    cpu_requests, memory_requests = 0, 0
                    cpu_limits, memory_limits = 0, 0
                    
                    if pod.spec.containers:
                        for container in pod.spec.containers:
                            if container.resources:
                                if container.resources.requests:
                                    cpu_req = container.resources.requests.get('cpu', '0')
                                    mem_req = container.resources.requests.get('memory', '0')
                                    cpu_requests += self.parser.parse_cpu(cpu_req)
                                    memory_requests += self.parser.parse_memory(mem_req)
                                    
                                if container.resources.limits:
                                    cpu_lim = container.resources.limits.get('cpu', '0')
                                    mem_lim = container.resources.limits.get('memory', '0')
                                    cpu_limits += self.parser.parse_cpu(cpu_lim)
                                    memory_limits += self.parser.parse_memory(mem_lim)
                    
                    restart_count = sum(
                        container.restart_count for container in (pod.status.container_statuses or [])
                    )
                    
                    log_entry = LogEntryFactory.create_resource_metrics_log(
                        pod, cpu_requests, memory_requests, cpu_limits, memory_limits, restart_count
                    )
                    log_entries.append(log_entry)
                    
                    if log_entry.has_error:
                        error_count += 1
                        logger.warning(f"Resource issue detected for pod {pod.metadata.name}: high restart count ({restart_count})")
                    
        except Exception as e:
            logger.error(f"Error collecting resource metrics: {e}")
        
        logger.info(f"Collected {len(log_entries)} resource metric entries, {error_count} with errors")
        return log_entries

class LogCollectorManager:
    """Enhanced manager class that coordinates all collectors with error tracking"""
    
    def __init__(self, config: AppConfig, k8s_client: client.CoreV1Api, logs_client=None):
        self.config = config
        self.collectors = [
            PodLogsCollector(config, k8s_client),
            EventsCollector(config, k8s_client),
            NodeLogsCollector(config, k8s_client),
            SystemComponentLogsCollector(config, k8s_client),
            ResourceMetricsCollector(config, k8s_client),
        ]
        
        if logs_client:
            self.collectors.append(EKSControlPlaneCollector(config, logs_client))
    
    def collect_all_logs(self) -> List[LogEntry]:
        """Collect logs from all collectors with comprehensive error tracking"""
        all_logs = []
        total_errors = 0
        
        for collector in self.collectors:
            try:
                collector_name = collector.__class__.__name__
                logger.info(f"Running {collector_name}...")
                
                logs = collector.collect_logs()
                all_logs.extend(logs)
                
                # Count errors from this collector
                collector_errors = sum(1 for log in logs if log.has_error)
                total_errors += collector_errors
                
                logger.info(f"{collector_name} completed: {len(logs)} logs, {collector_errors} errors")
                
            except Exception as e:
                logger.error(f"Error in collector {collector.__class__.__name__}: {e}")
        
        logger.info(f"Collection completed: {len(all_logs)} total logs, {total_errors} with errors")
        return all_logs
    
    def get_error_summary(self, logs: List[LogEntry]) -> Dict[str, Any]:
        """Get summary of errors found during collection"""
        error_logs = [log for log in logs if log.has_error]
        
        if not error_logs:
            return {"total_errors": 0, "error_types": {}}
        
        # Group by error type and severity
        error_summary = {}
        for log in error_logs:
            error_type = log.error_type or "unknown"
            severity = log.error_severity
            
            if error_type not in error_summary:
                error_summary[error_type] = {"total": 0, "severities": {}}
            
            error_summary[error_type]["total"] += 1
            if severity not in error_summary[error_type]["severities"]:
                error_summary[error_type]["severities"][severity] = 0
            error_summary[error_type]["severities"][severity] += 1
        
        return {
            "total_errors": len(error_logs),
            "error_types": error_summary,
            "critical_errors": len([log for log in error_logs if log.error_severity in ["CRITICAL", "HIGH"]]),
            "collection_timestamp": datetime.now(timezone.utc)
        }