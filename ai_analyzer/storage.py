#!/usr/bin/env python3
"""
Storage module for AI analysis results
Handles storing debug results and solutions in MongoDB
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from .config import AIAnalyzerConfig

logger = logging.getLogger(__name__)

class DebugResultsManager:
    """Class to manage debug results and AI solutions in MongoDB"""
    
    def __init__(self, config: AIAnalyzerConfig):
        self.config = config
        self.client = MongoClient(config.mongodb.uri)
        self.db = self.client[config.mongodb.database_name]
        self.debug_collection = self.db[config.mongodb.debug_collection]
        
        # Create indexes for better performance
        self._create_indexes()
    
    def _create_indexes(self):
        """Create indexes for efficient querying"""
        try:
            # Index for finding by error log ID
            self.debug_collection.create_index("error_log_id", unique=True)
            
            # Index for querying by error type and status
            self.debug_collection.create_index([
                ("error_type", 1),
                ("status", 1),
                ("created_at", -1)
            ])
            
            # Index for priority-based queries
            self.debug_collection.create_index([
                ("priority", 1),
                ("created_at", -1)
            ])
            
            # Index for namespace and error severity
            self.debug_collection.create_index([
                ("namespace", 1),
                ("error_severity", 1),
                ("created_at", -1)
            ])
            
            logger.info("Debug collection indexes created successfully")
        except Exception as e:
            logger.warning(f"Could not create debug collection indexes: {e}")
    
    def store_debug_result(self, log_doc: Dict[str, Any], suggested_fix: str, 
                          analysis_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Store a single debug result with enhanced metadata"""
        try:
            # Extract metadata safely
            metadata = log_doc.get("metadata", {})
            labels = {}
            node_name = "unknown"
            
            if isinstance(metadata, dict):
                labels = metadata.get("labels", {})
                node_name = metadata.get("node_name", "unknown")
            
            record = {
                "error_log_id": str(log_doc.get("_id")),
                "namespace": log_doc.get("namespace", "default"),
                "pod_name": log_doc.get("resource_name", "unknown"),
                "node_name": node_name,
                "error_type": log_doc.get("error_type"),
                "error_severity": log_doc.get("error_severity", "UNKNOWN"),
                "error_message": log_doc.get("message", "")[:2000],  # Limit message length
                "log_type": log_doc.get("log_type", ""),
                "source": log_doc.get("source", ""),
                "suggested_fix": suggested_fix,
                "status": "pending",
                "priority": self._calculate_priority(
                    log_doc.get("error_severity", "INFO"), 
                    log_doc.get("error_type")
                ),
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "metadata": {
                    "original_timestamp": log_doc.get("timestamp"),
                    "labels": labels,
                    "analysis_version": "2.0",
                    "analysis_metadata": analysis_metadata or {}
                }
            }
            
            # Use upsert to avoid duplicates
            result = self.debug_collection.replace_one(
                {"error_log_id": record["error_log_id"]},
                record,
                upsert=True
            )
            
            if result.upserted_id:
                record["_id"] = result.upserted_id
                logger.info(f"Created new debug result for {record['pod_name']}")
            else:
                logger.info(f"Updated existing debug result for {record['pod_name']}")
            
            return record
            
        except Exception as e:
            logger.error(f"Failed to store debug result: {e}")
            raise
    
    def store_batch_debug_results(self, analysis_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Store multiple debug results in batch with transaction-like behavior"""
        if not analysis_results:
            logger.warning("No analysis results provided for batch storage")
            return []
        
        stored_records = []
        failed_records = []
        
        logger.info(f"Storing {len(analysis_results)} debug results in batch")
        
        for result in analysis_results:
            try:
                # Extract analysis metadata
                analysis_metadata = {
                    "analysis_success": result.get("analysis_success", False),
                    "analysis_timestamp": result.get("analysis_timestamp"),
                }
                
                record = self.store_debug_result(
                    result['log_doc'], 
                    result['suggested_fix'],
                    analysis_metadata
                )
                stored_records.append(record)
                
            except Exception as e:
                logger.error(f"Failed to store individual debug result: {e}")
                failed_records.append({
                    'log_doc': result.get('log_doc', {}),
                    'error': str(e)
                })
        
        success_count = len(stored_records)
        failure_count = len(failed_records)
        
        logger.info(f"Batch storage completed: {success_count} successful, {failure_count} failed")
        
        if failed_records:
            logger.warning(f"Failed to store {failure_count} records")
            for failed in failed_records[:3]:  # Log first 3 failures
                resource_name = failed['log_doc'].get('resource_name', 'unknown')
                logger.warning(f"Storage failure for {resource_name}: {failed['error']}")
        
        return stored_records
    
    def update_result_status(self, error_log_id: str, status: str, notes: str = "") -> bool:
        """Update the status of a debug result"""
        try:
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow()
            }
            
            if notes:
                update_data["status_notes"] = notes
            
            result = self.debug_collection.update_one(
                {"error_log_id": error_log_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated status for error_log_id {error_log_id} to {status}")
                return True
            else:
                logger.warning(f"No document found with error_log_id {error_log_id}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to update result status: {e}")
            return False
    
    def get_debug_results(self, hours_back: int = 24, status: str = None, 
                         error_type: str = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Get debug results with filtering options"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        query = {"created_at": {"$gte": start_time}}
        
        if status:
            query["status"] = status
        if error_type:
            query["error_type"] = error_type
        
        try:
            results = list(
                self.debug_collection.find(query)
                .sort([("priority", 1), ("created_at", -1)])
                .limit(limit)
            )
            
            logger.info(f"Retrieved {len(results)} debug results")
            return results
            
        except Exception as e:
            logger.error(f"Failed to get debug results: {e}")
            return []
    
    def get_debug_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get comprehensive summary of debug results"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        try:
            pipeline = [
                {"$match": {"created_at": {"$gte": start_time}}},
                {"$group": {
                    "_id": {
                        "error_type": "$error_type",
                        "status": "$status",
                        "priority": "$priority"
                    },
                    "count": {"$sum": 1},
                    "latest": {"$max": "$created_at"},
                    "namespaces": {"$addToSet": "$namespace"},
                    "nodes": {"$addToSet": "$node_name"}
                }},
                {"$sort": {"count": -1}}
            ]
            
            results = list(self.debug_collection.aggregate(pipeline))
            
            # Calculate totals
            total_analyzed = sum(r["count"] for r in results)
            pending_count = sum(r["count"] for r in results if r["_id"]["status"] == "pending")
            resolved_count = sum(r["count"] for r in results if r["_id"]["status"] == "resolved")
            
            # Group by error type
            by_error_type = {}
            for result in results:
                error_type = result["_id"]["error_type"] or "unknown"
                if error_type not in by_error_type:
                    by_error_type[error_type] = {"total": 0, "by_status": {}, "by_priority": {}}
                
                by_error_type[error_type]["total"] += result["count"]
                status = result["_id"]["status"]
                priority = result["_id"]["priority"]
                
                by_error_type[error_type]["by_status"][status] = by_error_type[error_type]["by_status"].get(status, 0) + result["count"]
                by_error_type[error_type]["by_priority"][priority] = by_error_type[error_type]["by_priority"].get(priority, 0) + result["count"]
            
            return {
                "total_analyzed": total_analyzed,
                "pending_count": pending_count,
                "resolved_count": resolved_count,
                "resolution_rate": (resolved_count / total_analyzed * 100) if total_analyzed > 0 else 0,
                "time_period_hours": hours_back,
                "by_error_type": by_error_type,
                "breakdown": results,
                "summary_timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to get debug summary: {e}")
            return {
                "total_analyzed": 0,
                "pending_count": 0,
                "resolved_count": 0,
                "resolution_rate": 0,
                "time_period_hours": hours_back,
                "by_error_type": {},
                "breakdown": [],
                "summary_timestamp": datetime.utcnow()
            }
    
    def get_high_priority_results(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get high priority debug results that need attention"""
        try:
            results = list(
                self.debug_collection.find({
                    "priority": {"$lte": 2},  # Priority 1 or 2 (highest)
                    "status": "pending"
                })
                .sort([("priority", 1), ("created_at", -1)])
                .limit(limit)
            )
            
            logger.info(f"Retrieved {len(results)} high priority debug results")
            return results
            
        except Exception as e:
            logger.error(f"Failed to get high priority results: {e}")
            return []
    
    def cleanup_old_results(self, days_old: int = 30) -> int:
        """Clean up old debug results to prevent collection growth"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(days=days_old)
            
            # Only delete resolved results older than cutoff
            result = self.debug_collection.delete_many({
                "created_at": {"$lt": cutoff_time},
                "status": "resolved"
            })
            
            deleted_count = result.deleted_count
            logger.info(f"Cleaned up {deleted_count} old debug results (older than {days_old} days)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old results: {e}")
            return 0
    
    def _calculate_priority(self, severity: str, error_type: str) -> int:
        """Calculate priority score for triage (1=highest, 5=lowest)"""
        severity_scores = {
            "CRITICAL": 1,
            "HIGH": 2,
            "MEDIUM": 3,
            "LOW": 4,
            "INFO": 5
        }
        
        base_priority = severity_scores.get(severity, 3)
        
        # Critical error types get higher priority (lower number)
        critical_types = self.config.analysis.critical_error_types
        if error_type in critical_types:
            base_priority = max(1, base_priority - 1)
        
        return base_priority
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get storage statistics"""
        try:
            total_count = self.debug_collection.count_documents({})
            
            # Get recent activity (last 24 hours)
            recent_time = datetime.utcnow() - timedelta(hours=24)
            recent_count = self.debug_collection.count_documents({"created_at": {"$gte": recent_time}})
            
            # Get status breakdown
            status_pipeline = [
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }}
            ]
            status_results = list(self.debug_collection.aggregate(status_pipeline))
            status_breakdown = {result["_id"]: result["count"] for result in status_results}
            
            return {
                "total_results": total_count,
                "recent_results_24h": recent_count,
                "status_breakdown": status_breakdown,
                "collection_name": self.config.mongodb.debug_collection,
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage statistics: {e}")
            return {
                "total_results": 0,
                "recent_results_24h": 0,
                "status_breakdown": {},
                "collection_name": self.config.mongodb.debug_collection,
                "timestamp": datetime.utcnow()
            }
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("Closed debug results MongoDB connection")