#!/usr/bin/env python3
"""
Enhanced Kubernetes Log Tracker - Main Application
Collects various Kubernetes logs with error detection and stores them in MongoDB
"""

import time
import logging
import threading
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from kubernetes import client, config
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import boto3

from config import load_config
from models import LogEntry
from collectors import LogCollectorManager

class EnhancedKubernetesLogTracker:
    """Enhanced main class for tracking Kubernetes logs with error detection"""
    
    def __init__(self):
        self.config = load_config()
        self._setup_logging()
        
        # Initialize connections
        self._init_mongodb()
        self._init_kubernetes()
        self._init_aws()
        
        # Initialize enhanced log collector manager
        self.collector_manager = LogCollectorManager(
            self.config, self.v1, self.logs_client
        )
        
        # Tracking flags and statistics
        self.running = False
        self.threads = []
        self.collection_stats = {
            "total_collections": 0,
            "total_logs": 0,
            "total_errors": 0,
            "last_collection": None,
            "error_trends": {}
        }
        
    def _setup_logging(self):
        """Setup enhanced logging configuration"""
        logging.basicConfig(
            level=getattr(logging, self.config.logging.level),
            format=self.config.logging.format
        )
        self.logger = logging.getLogger(__name__)
        
        # Add file logging for error tracking
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        file_handler = logging.FileHandler('logs/k8s_tracker.log')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Separate error log file
        error_handler = logging.FileHandler('logs/k8s_errors.log')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        self.logger.addHandler(error_handler)
        
    def _init_mongodb(self):
        """Initialize MongoDB connection with enhanced error handling"""
        try:
            self.mongo_client = MongoClient(self.config.mongodb.uri)
            self.db = self.mongo_client[self.config.mongodb.database_name]
            self.collection = self.db[self.config.mongodb.collection_name]
            
            # Create indexes for better error querying performance
            self._create_mongodb_indexes()
            
            # Test connection
            self.mongo_client.admin.command('ping')
            self.logger.info("Connected to MongoDB successfully")
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
            
    def _create_mongodb_indexes(self):
        """Create MongoDB indexes for efficient error querying"""
        try:
            # Index for error logs
            self.collection.create_index([
                ("has_error", 1), 
                ("timestamp", -1)
            ], name="error_timestamp_idx")
            
            # Index for error types
            self.collection.create_index([
                ("error_type", 1),
                ("error_severity", 1),
                ("timestamp", -1)
            ], name="error_type_severity_idx")
            
            # Index for namespace and resource queries
            self.collection.create_index([
                ("namespace", 1),
                ("resource_name", 1),
                ("timestamp", -1)
            ], name="resource_timestamp_idx")
            
            # Index for log types
            self.collection.create_index([
                ("log_type", 1),
                ("timestamp", -1)
            ], name="log_type_timestamp_idx")
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.warning(f"Could not create MongoDB indexes: {e}")
            
    def _init_kubernetes(self):
        """Initialize Kubernetes client"""
        try:
            # Try to load in-cluster config first, then local config
            try:
                config.load_incluster_config()
                self.logger.info("Loaded in-cluster Kubernetes config")
            except config.ConfigException:
                config.load_kube_config()
                self.logger.info("Loaded local Kubernetes config")
                
            self.v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
            
            # Test connection
            version = self.v1.get_api_resources()
            self.logger.info("Kubernetes client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kubernetes client: {e}")
            raise
            
    def _init_aws(self):
        """Initialize AWS clients"""
        try:
            session = boto3.Session(profile_name=self.config.aws.profile)
            self.logs_client = session.client('logs', region_name=self.config.aws.region)
            self.ec2_client = session.client('ec2', region_name=self.config.aws.region)
            self.logger.info("Initialized AWS clients successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize AWS clients: {e}")
            self.logs_client = None
            self.ec2_client = None
    
    def _store_logs(self, log_entries: list[LogEntry]):
        """Store multiple log entries in MongoDB with error tracking"""
        if not log_entries:
            return
            
        try:
            documents = [log_entry.to_dict() for log_entry in log_entries]
            result = self.collection.insert_many(documents)
            
            # Update statistics
            error_count = sum(1 for entry in log_entries if entry.has_error)
            self.collection_stats["total_logs"] += len(documents)
            self.collection_stats["total_errors"] += error_count
            
            self.logger.info(f"Stored {len(result.inserted_ids)} log entries ({error_count} with errors)")
            
            # Log error details if any critical errors found
            if error_count > 0:
                critical_errors = [entry for entry in log_entries if entry.has_error and entry.error_severity in ["CRITICAL", "HIGH"]]
                if critical_errors:
                    self.logger.warning(f"Found {len(critical_errors)} critical/high severity errors")
                    for error in critical_errors[:5]:  # Log first 5 critical errors
                        self.logger.warning(f"Critical error in {error.namespace}/{error.resource_name}: {error.error_type} - {error.message[:100]}...")
            
        except Exception as e:
            self.logger.error(f"Failed to store log entries: {e}")
    
    def _log_collection_cycle(self):
        """Enhanced main log collection cycle with error tracking"""
        while self.running:
            try:
                cycle_start = datetime.now(timezone.utc)
                self.logger.info("Starting enhanced log collection cycle...")
                
                # Collect logs from all collectors
                log_entries = self.collector_manager.collect_all_logs()
                
                # Get error summary from this collection
                error_summary = self.collector_manager.get_error_summary(log_entries)
                
                # Store all collected logs
                self._store_logs(log_entries)
                
                # Update collection statistics
                self.collection_stats["total_collections"] += 1
                self.collection_stats["last_collection"] = cycle_start
                
                # Track error trends
                self._update_error_trends(error_summary)
                
                cycle_duration = (datetime.now(timezone.utc) - cycle_start).total_seconds()
                
                self.logger.info(
                    f"Collection cycle completed in {cycle_duration:.2f}s - "
                    f"collected {len(log_entries)} entries, {error_summary['total_errors']} with errors"
                )
                
                # Log error summary if significant errors found
                if error_summary['critical_errors'] > 0:
                    self.logger.warning(f"⚠️  {error_summary['critical_errors']} critical errors detected in this cycle")
                    self._log_error_breakdown(error_summary)
                
                # Wait for configured interval before next cycle
                time.sleep(self.config.logging.collection_interval)
                
            except Exception as e:
                self.logger.error(f"Error in log collection cycle: {e}")
                time.sleep(self.config.logging.collection_interval)
    
    def _update_error_trends(self, error_summary: Dict[str, Any]):
        """Update error trend tracking"""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00")  # Hour-based trending
        
        if timestamp not in self.collection_stats["error_trends"]:
            self.collection_stats["error_trends"][timestamp] = {
                "total_errors": 0,
                "critical_errors": 0,
                "error_types": {}
            }
        
        trend_data = self.collection_stats["error_trends"][timestamp]
        trend_data["total_errors"] += error_summary["total_errors"]
        trend_data["critical_errors"] += error_summary["critical_errors"]
        
        for error_type, details in error_summary.get("error_types", {}).items():
            if error_type not in trend_data["error_types"]:
                trend_data["error_types"][error_type] = 0
            trend_data["error_types"][error_type] += details["total"]
    
    def _log_error_breakdown(self, error_summary: Dict[str, Any]):
        """Log detailed error breakdown for monitoring"""
        self.logger.info("Error breakdown for this cycle:")
        for error_type, details in error_summary.get("error_types", {}).items():
            severity_info = ", ".join([f"{sev}: {count}" for sev, count in details["severities"].items()])
            self.logger.info(f"  {error_type}: {details['total']} ({severity_info})")
    
    def start_tracking(self):
        """Start the enhanced log tracking process"""
        self.logger.info("Starting enhanced Kubernetes log tracking with error detection...")
        self.running = True
        
        # Start the main collection thread
        collection_thread = threading.Thread(target=self._log_collection_cycle, daemon=True)
        collection_thread.start()
        self.threads.append(collection_thread)
        
        # Start error monitoring thread
        monitoring_thread = threading.Thread(target=self._error_monitoring_cycle, daemon=True)
        monitoring_thread.start()
        self.threads.append(monitoring_thread)
        
        self.logger.info("Enhanced log tracking started successfully")
    
    def _error_monitoring_cycle(self):
        """Separate thread for monitoring error trends and alerts"""
        while self.running:
            try:
                time.sleep(300)  # Check every 5 minutes
                
                # Get recent error statistics
                recent_errors = self.get_recent_error_summary(minutes=30)
                
                # Check for error rate spikes
                if recent_errors["error_rate_per_minute"] > 5:  # More than 5 errors per minute
                    self.logger.warning(f"🚨 High error rate detected: {recent_errors['error_rate_per_minute']:.1f} errors/min")
                
                # Check for new critical error types
                critical_types = [
                    error_type for error_type, details in recent_errors.get("error_types", {}).items()
                    if any(severity in ["CRITICAL", "HIGH"] for severity in details.get("severities", {}))
                ]
                
                if critical_types:
                    self.logger.warning(f"🔥 Critical error types active: {', '.join(critical_types)}")
                
            except Exception as e:
                self.logger.error(f"Error in monitoring cycle: {e}")
    
    def stop_tracking(self):
        """Stop the log tracking process"""
        self.logger.info("Stopping enhanced Kubernetes log tracking...")
        self.running = False
        
        # Wait for threads to complete
        for thread in self.threads:
            thread.join(timeout=10)
        
        # Close connections
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()
        
        self.logger.info("Enhanced log tracking stopped")
    
    def get_logs_summary(self, hours: int = 1) -> Dict[str, Any]:
        """Get an enhanced summary of collected logs with error analysis"""
        try:
            # Get logs from the last N hours
            start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
            
            # Enhanced pipeline with error analysis
            pipeline = [
                {"$match": {"timestamp": {"$gte": start_time}}},
                {"$group": {
                    "_id": {
                        "log_type": "$log_type",
                        "has_error": "$has_error",
                        "error_type": "$error_type",
                        "error_severity": "$error_severity"
                    },
                    "count": {"$sum": 1},
                    "latest": {"$max": "$timestamp"},
                    "namespaces": {"$addToSet": "$namespace"}
                }},
                {"$sort": {"count": -1}}
            ]
            
            results = list(self.collection.aggregate(pipeline))
            
            # Process results
            total_logs = sum(result["count"] for result in results)
            error_logs = sum(result["count"] for result in results if result["_id"]["has_error"])
            critical_logs = sum(
                result["count"] for result in results 
                if result["_id"]["has_error"] and result["_id"].get("error_severity") in ["CRITICAL", "HIGH"]
            )
            
            # Group by error types
            error_types = {}
            for result in results:
                if result["_id"]["has_error"] and result["_id"]["error_type"]:
                    error_type = result["_id"]["error_type"]
                    if error_type not in error_types:
                        error_types[error_type] = {"count": 0, "severities": {}}
                    error_types[error_type]["count"] += result["count"]
                    severity = result["_id"].get("error_severity", "UNKNOWN")
                    error_types[error_type]["severities"][severity] = error_types[error_type]["severities"].get(severity, 0) + result["count"]
            
            return {
                "total_logs": total_logs,
                "error_logs": error_logs,
                "critical_logs": critical_logs,
                "error_rate_percentage": round((error_logs / total_logs * 100) if total_logs > 0 else 0, 2),
                "time_period_hours": hours,
                "error_types": error_types,
                "log_type_breakdown": results,
                "collection_time": datetime.now(timezone.utc),
                "tracker_stats": self.collection_stats
            }
            
        except Exception as e:
            self.logger.error(f"Error getting logs summary: {e}")
            return {}
    
    def get_recent_error_summary(self, minutes: int = 30) -> Dict[str, Any]:
        """Get summary of recent errors for monitoring"""
        try:
            start_time = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            
            pipeline = [
                {"$match": {
                    "has_error": True,
                    "timestamp": {"$gte": start_time}
                }},
                {"$group": {
                    "_id": {
                        "error_type": "$error_type",
                        "error_severity": "$error_severity"
                    },
                    "count": {"$sum": 1},
                    "latest": {"$max": "$timestamp"},
                    "resources": {"$addToSet": {"namespace": "$namespace", "name": "$resource_name"}}
                }}
            ]
            
            results = list(self.collection.aggregate(pipeline))
            total_errors = sum(r["count"] for r in results)
            
            # Calculate error rate
            error_rate_per_minute = total_errors / minutes if minutes > 0 else 0
            
            # Group by error type
            error_types = {}
            for result in results:
                error_type = result["_id"]["error_type"] or "unknown"
                if error_type not in error_types:
                    error_types[error_type] = {"count": 0, "severities": {}}
                error_types[error_type]["count"] += result["count"]
                severity = result["_id"]["error_severity"]
                error_types[error_type]["severities"][severity] = error_types[error_type]["severities"].get(severity, 0) + result["count"]
            
            return {
                "total_errors": total_errors,
                "error_rate_per_minute": error_rate_per_minute,
                "time_period_minutes": minutes,
                "error_types": error_types,
                "check_time": datetime.now(timezone.utc)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting recent error summary: {e}")
            return {"total_errors": 0, "error_rate_per_minute": 0}
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get enhanced health status of the log tracker"""
        recent_errors = self.get_recent_error_summary(minutes=15)
        
        # Determine health status
        health_status = "HEALTHY"
        if recent_errors["error_rate_per_minute"] > 10:
            health_status = "CRITICAL"
        elif recent_errors["error_rate_per_minute"] > 3:
            health_status = "WARNING"
        elif recent_errors["total_errors"] > 20:
            health_status = "DEGRADED"
        
        return {
            "running": self.running,
            "health_status": health_status,
            "threads_active": len([t for t in self.threads if t.is_alive()]),
            "mongodb_connected": self.mongo_client is not None,
            "aws_clients_available": self.logs_client is not None,
            "recent_error_rate": recent_errors["error_rate_per_minute"],
            "collection_stats": self.collection_stats,
            "last_check": datetime.now(timezone.utc)
        }


def main():
    """Enhanced main function with better error handling"""
    # Create and start the enhanced log tracker
    tracker = EnhancedKubernetesLogTracker()
    
    try:
        tracker.start_tracking()
        
        # Keep the main thread alive with enhanced monitoring
        cycle_count = 0
        while True:
            time.sleep(60)
            cycle_count += 1
            
            # Print summary every minute
            summary = tracker.get_logs_summary()
            if summary:
                tracker.logger.info(
                    f"📊 Hourly stats: {summary.get('total_logs', 0)} logs, "
                    f"{summary.get('error_logs', 0)} errors "
                    f"({summary.get('error_rate_percentage', 0)}%)"
                )
            
            # Print detailed health status every 5 minutes
            if cycle_count % 5 == 0:
                health = tracker.get_health_status()
                status_emoji = {"HEALTHY": "✅", "DEGRADED": "⚠️", "WARNING": "🔶", "CRITICAL": "🚨"}.get(health["health_status"], "❓")
                tracker.logger.info(f"{status_emoji} Health: {health['health_status']}, Error rate: {health['recent_error_rate']:.1f}/min")
                
                # Log error trends if available
                if tracker.collection_stats["error_trends"]:
                    latest_hour = max(tracker.collection_stats["error_trends"].keys())
                    hour_data = tracker.collection_stats["error_trends"][latest_hour]
                    if hour_data["total_errors"] > 0:
                        top_errors = sorted(hour_data["error_types"].items(), key=lambda x: x[1], reverse=True)[:3]
                        tracker.logger.info(f"🔝 Top error types this hour: {', '.join([f'{k}({v})' for k, v in top_errors])}")
                
    except KeyboardInterrupt:
        tracker.logger.info("Received interrupt signal, stopping...")
    except Exception as e:
        tracker.logger.error(f"Unexpected error: {e}")
    finally:
        tracker.stop_tracking()


if __name__ == "__main__":
    main()