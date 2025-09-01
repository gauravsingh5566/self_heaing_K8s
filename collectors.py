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

                    # ✅ Case 2: Pod not producing logs (Pending, ImagePullBackOff, etc.)
                    else:
                        diagnostic_messages = []
                        detected_errors = []

                        # Container status diagnostics with error detection
                        for cs in pod.status.container_statuses or []:
                            if cs.state.waiting:
                                message = f"Container {cs.name} is waiting: {cs.state.waiting.reason} - {cs.state.waiting.message}"
                                diagnostic_messages.append(message)
                                
                                # Check if this is an error condition
                                has_error, error_type, severity = self.error_detector.detect_error(message)
                                if has_error:
                                    detected_errors.append((error_type, severity))
                            
                            elif cs.state.terminated:
                                message = f"Container {cs.name} terminated: exit code {cs.state.terminated.exit_code}, reason: {cs.state.terminated.reason}"
                                diagnostic_messages.append(message)
                                
                                # Terminated with non-zero exit code is usually an error
                                if cs.state.terminated.exit_code != 0:
                                    detected_errors.append(("container", "HIGH"))

                        # Pod condition diagnostics
                        if pod.status.conditions:
                            for condition in pod.status.conditions:
                                if condition.status == "False" and condition.type in ["PodReadyCondition", "ContainersReady"]:
                                    message = f"Pod condition {condition.type}: {condition.reason} - {condition.message}"
                                    diagnostic_messages.append(message)
                                    detected_errors.append(("container", "MEDIUM"))

                        # Pod-related events with error detection
                        try:
                            events = self.k8s_client.list_namespaced_event(namespace=pod.metadata.namespace)
                            for event in events.items:
                                if (event.involved_object.name == pod.metadata.name and 
                                    event.involved_object.kind == "Pod"):
                                    event_msg = f"Event: [{event.last_timestamp}] {event.message}"
                                    diagnostic_messages.append(event_msg)
                                    
                                    # Check event for errors
                                    has_error, error_type, severity = self.error_detector.detect_error(event.message, event.type)
                                    if has_error:
                                        detected_errors.append((error_type, severity))
                        except Exception as e:
                            logger.debug(f"Could not fetch events for pod {pod.metadata.name}: {e}")

                        # Create diagnostic log entry if we have messages
                        if diagnostic_messages:
                            diagnostics_text = "\n".join(diagnostic_messages)
                            
                            # Determine overall error status
                            has_error = len(detected_errors) > 0
                            error_type = None
                            error_severity = "INFO"
                            
                            if detected_errors:
                                # Use the most severe error
                                severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}
                                most_severe = min(detected_errors, key=lambda x: severity_order.get(x[1], 4))
                                error_type = most_severe[0]
                                error_severity = most_severe[1]
                                error_count += 1
                            
                            # Create the diagnostic entry manually since we need custom error info
                            diag_entry = LogEntry(
                                timestamp=datetime.now(timezone.utc),
                                log_type="container_logs",
                                source="pod_diagnostics",
                                namespace=pod.metadata.namespace,
                                resource_name=pod.metadata.name,
                                message=diagnostics_text,
                                level="WARNING" if has_error else "INFO",
                                metadata={
                                    "pod_phase": pod.status.phase,
                                    "node_name": pod.spec.node_name,
                                    "labels": pod.metadata.labels or {},
                                    "diagnostic_type": "pod_status"
                                },
                                has_error=has_error,
                                error_type=error_type,
                                error_severity=error_severity
                            )
                            log_entries.append(diag_entry)
                            
                            if has_error:
                                logger.warning(f"Error detected in pod {pod.metadata.name} diagnostics: {error_type}")

                except Exception as e:
                    logger.debug(f"Could not get logs for pod {pod.metadata.name}: {e}")
                    
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