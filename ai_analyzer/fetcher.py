#!/usr/bin/env python3
"""
Enhanced Error log fetcher module with improved duplicate prevention
Handles fetching and filtering error logs from MongoDB
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
from pymongo import MongoClient
from bson import ObjectId

from .config import AIAnalyzerConfig

logger = logging.getLogger(__name__)

class ErrorLogFetcher:
    """Class to fetch and categorize error logs from MongoDB with enhanced duplicate prevention"""
    
    def __init__(self, config: AIAnalyzerConfig):
        self.config = config
        self.client = MongoClient(config.mongodb.uri)
        self.db = self.client[config.mongodb.database_name]
        self.logs_collection = self.db[config.mongodb.logs_collection]
        self.debug_collection = self.db[config.mongodb.debug_collection]
        
        # Cache for processed IDs to improve performance
        self._processed_ids_cache = None
        self._cache_timestamp = None
        self._cache_duration = 300  # 5 minutes
    
    def fetch_unprocessed_errors(self, hours_back: int = None, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch unprocessed error logs with enhanced duplicate prevention"""
        hours_back = hours_back or self.config.analysis.hours_back_default
        limit = limit or self.config.analysis.batch_size
        
        # Calculate time window
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        # Get processed IDs with caching
        processed_ids = self._get_processed_log_ids_cached()
        
        # Enhanced query to prevent duplicates
        query = {
            "has_error": True,
            "timestamp": {"$gte": start_time},
            # Exclude already processed logs
            "_id": {"$nin": processed_ids}
        }
        
        # Additional deduplication: group by similar errors and take only the most recent
        pipeline = [
            {"$match": query},
            {"$sort": {"timestamp": -1}},  # Most recent first
            # Group by error signature to prevent duplicate analysis of same error
            {"$group": {
                "_id": {
                    "error_type": "$error_type",
                    "namespace": "$namespace", 
                    "resource_name": "$resource_name",
                    "error_message_hash": {"$substr": ["$message", 0, 100]}  # First 100 chars as signature
                },
                "latest_doc": {"$first": "$$ROOT"},  # Take most recent
                "count": {"$sum": 1},
                "timestamps": {"$push": "$timestamp"}
            }},
            {"$replaceRoot": {"newRoot": "$latest_doc"}},  # Replace with the actual document
            {"$sort": {"error_severity": 1, "timestamp": -1}},  # Priority: CRITICAL, HIGH, etc.
            {"$limit": limit}
        ]
        
        try:
            docs = list(self.logs_collection.aggregate(pipeline))
            
            # Filter out any remaining duplicates based on exact content similarity
            unique_docs = self._filter_similar_errors(docs)
            
            logger.info(f"Found {len(unique_docs)} unique unprocessed error logs from last {hours_back} hours (filtered from {len(docs)} total)")
            
            return unique_docs
            
        except Exception as e:
            logger.error(f"Error fetching unprocessed logs: {e}")
            # Fallback to simple query if aggregation fails
            return self._fallback_fetch_unprocessed(query, limit)
    
    def _filter_similar_errors(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out very similar errors to prevent analyzing essentially the same issue multiple times"""
        if not docs:
            return docs
        
        unique_docs = []
        seen_signatures = set()
        
        for doc in docs:
            # Create a signature for this error
            signature = self._create_error_signature(doc)
            
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                unique_docs.append(doc)
            else:
                logger.debug(f"Filtered duplicate error signature: {doc.get('resource_name', 'unknown')} - {doc.get('error_type', 'unknown')}")
        
        logger.info(f"Filtered out {len(docs) - len(unique_docs)} similar errors")
        return unique_docs
    
    def _create_error_signature(self, doc: Dict[str, Any]) -> str:
        """Create a unique signature for an error to identify duplicates"""
        # Combine key fields to create a signature
        namespace = doc.get("namespace", "")
        resource_name = doc.get("resource_name", "")
        error_type = doc.get("error_type", "")
        
        # Get first 50 characters of error message (normalized)
        message = doc.get("message", "")
        normalized_message = message.lower().strip()[:50]
        
        # Create signature
        signature = f"{namespace}:{resource_name}:{error_type}:{normalized_message}"
        return signature
    
    def _fallback_fetch_unprocessed(self, query: Dict[str, Any], limit: int) -> List[Dict[str, Any]]:
        """Fallback method if aggregation pipeline fails"""
        try:
            docs = list(self.logs_collection.find(query).sort([
                ("error_severity", 1),  # CRITICAL first
                ("timestamp", -1)
            ]).limit(limit))
            
            logger.warning(f"Used fallback fetch method, got {len(docs)} documents")
            return docs
        except Exception as e:
            logger.error(f"Fallback fetch also failed: {e}")
            return []
    
    def fetch_errors_by_type(self, error_type: str, hours_back: int = 24, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch error logs of a specific type with deduplication"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        # Use aggregation to get unique errors by type
        pipeline = [
            {"$match": {
                "has_error": True,
                "error_type": error_type,
                "timestamp": {"$gte": start_time}
            }},
            {"$sort": {"timestamp": -1}},
            # Group by resource to avoid analyzing same pod multiple times
            {"$group": {
                "_id": {
                    "namespace": "$namespace",
                    "resource_name": "$resource_name"
                },
                "latest_error": {"$first": "$$ROOT"},
                "error_count": {"$sum": 1}
            }},
            {"$replaceRoot": {"newRoot": "$latest_error"}},
            {"$sort": {"timestamp": -1}},
            {"$limit": limit}
        ]
        
        try:
            docs = list(self.logs_collection.aggregate(pipeline))
            logger.info(f"Found {len(docs)} unique {error_type} errors in last {hours_back} hours")
            return docs
        except Exception as e:
            logger.error(f"Error fetching {error_type} logs: {e}")
            # Fallback to simple query
            query = {
                "has_error": True,
                "error_type": error_type,
                "timestamp": {"$gte": start_time}
            }
            return list(self.logs_collection.find(query).sort([("timestamp", -1)]).limit(limit))
    
    def fetch_errors_by_severity(self, severity: str, hours_back: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """Fetch error logs by severity level with deduplication"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        pipeline = [
            {"$match": {
                "has_error": True,
                "error_severity": severity,
                "timestamp": {"$gte": start_time}
            }},
            {"$sort": {"timestamp": -1}},
            # Group by error signature to avoid duplicates
            {"$group": {
                "_id": {
                    "error_type": "$error_type",
                    "namespace": "$namespace",
                    "resource_name": "$resource_name"
                },
                "latest_error": {"$first": "$$ROOT"},
                "occurrence_count": {"$sum": 1}
            }},
            {"$replaceRoot": {"newRoot": "$latest_error"}},
            {"$sort": {"timestamp": -1}},
            {"$limit": limit}
        ]
        
        try:
            docs = list(self.logs_collection.aggregate(pipeline))
            logger.info(f"Found {len(docs)} unique {severity} severity errors in last {hours_back} hours")
            return docs
        except Exception as e:
            logger.error(f"Error fetching {severity} severity logs: {e}")
            return []
    
    def fetch_critical_errors(self, hours_back: int = 1, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch critical and high severity errors with smart deduplication"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        processed_ids = self._get_processed_log_ids_cached()
        
        pipeline = [
            {"$match": {
                "has_error": True,
                "error_severity": {"$in": ["CRITICAL", "HIGH"]},
                "timestamp": {"$gte": start_time},
                "_id": {"$nin": processed_ids}
            }},
            {"$sort": {"timestamp": -1}},
            # Group by error pattern to avoid analyzing same critical issue multiple times
            {"$group": {
                "_id": {
                    "error_type": "$error_type",
                    "error_severity": "$error_severity",
                    "namespace": "$namespace",
                    "resource_pattern": {"$substr": ["$resource_name", 0, 20]}  # Group similar pod names
                },
                "most_recent": {"$first": "$$ROOT"},
                "critical_count": {"$sum": 1}
            }},
            {"$replaceRoot": {"newRoot": "$most_recent"}},
            {"$sort": {
                "error_severity": 1,  # CRITICAL before HIGH
                "timestamp": -1
            }},
            {"$limit": limit}
        ]
        
        try:
            docs = list(self.logs_collection.aggregate(pipeline))
            logger.info(f"Found {len(docs)} unique critical/high severity errors from last {hours_back} hours")
            return docs
        except Exception as e:
            logger.error(f"Error fetching critical errors: {e}")
            return []
    
    def fetch_errors_by_namespace(self, namespace: str, hours_back: int = 24, limit: int = 20) -> List[Dict[str, Any]]:
        """Fetch errors from a specific namespace with deduplication"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        pipeline = [
            {"$match": {
                "has_error": True,
                "namespace": namespace,
                "timestamp": {"$gte": start_time}
            }},
            {"$sort": {"timestamp": -1}},
            # Group by error type and resource to avoid duplicates
            {"$group": {
                "_id": {
                    "error_type": "$error_type",
                    "resource_name": "$resource_name"
                },
                "latest_error": {"$first": "$$ROOT"},
                "error_count": {"$sum": 1}
            }},
            {"$replaceRoot": {"newRoot": "$latest_error"}},
            {"$sort": {"error_severity": 1, "timestamp": -1}},
            {"$limit": limit}
        ]
        
        try:
            docs = list(self.logs_collection.aggregate(pipeline))
            logger.info(f"Found {len(docs)} unique errors in namespace '{namespace}' from last {hours_back} hours")
            return docs
        except Exception as e:
            logger.error(f"Error fetching errors for namespace {namespace}: {e}")
            return []
    
    def get_error_statistics(self, hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get comprehensive statistics about error types and patterns"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        pipeline = [
            {"$match": {
                "has_error": True,
                "timestamp": {"$gte": start_time}
            }},
            {"$group": {
                "_id": {
                    "error_type": "$error_type",
                    "error_severity": "$error_severity",
                    "namespace": "$namespace"
                },
                "count": {"$sum": 1},
                "latest_occurrence": {"$max": "$timestamp"},
                "earliest_occurrence": {"$min": "$timestamp"},
                "resources": {"$addToSet": "$resource_name"},
                "sources": {"$addToSet": "$source"},
                "log_types": {"$addToSet": "$log_type"}
            }},
            {"$sort": {"count": -1}}
        ]
        
        try:
            results = list(self.logs_collection.aggregate(pipeline))
            logger.info(f"Generated error statistics: {len(results)} different error patterns found")
            return results
        except Exception as e:
            logger.error(f"Error generating error statistics: {e}")
            return []
    
    def get_error_trends(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get error trends over time (hourly breakdown)"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        pipeline = [
            {"$match": {
                "has_error": True,
                "timestamp": {"$gte": start_time}
            }},
            {"$group": {
                "_id": {
                    "hour": {"$dateToString": {"format": "%Y-%m-%d %H:00", "date": "$timestamp"}},
                    "error_type": "$error_type",
                    "error_severity": "$error_severity"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.hour": -1, "count": -1}}
        ]
        
        try:
            results = list(self.logs_collection.aggregate(pipeline))
            
            # Process results into trend data
            trends = {}
            for result in results:
                hour = result["_id"]["hour"]
                if hour not in trends:
                    trends[hour] = {"total": 0, "by_type": {}, "by_severity": {}}
                
                error_type = result["_id"]["error_type"] or "unknown"
                severity = result["_id"]["error_severity"]
                count = result["count"]
                
                trends[hour]["total"] += count
                trends[hour]["by_type"][error_type] = trends[hour]["by_type"].get(error_type, 0) + count
                trends[hour]["by_severity"][severity] = trends[hour]["by_severity"].get(severity, 0) + count
            
            logger.info(f"Generated error trends for {len(trends)} time periods")
            return trends
        except Exception as e:
            logger.error(f"Error generating error trends: {e}")
            return {}
    
    def get_top_error_resources(self, hours_back: int = 24, limit: int = 10) -> List[Dict[str, Any]]:
        """Get resources with the most errors"""
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        pipeline = [
            {"$match": {
                "has_error": True,
                "timestamp": {"$gte": start_time}
            }},
            {"$group": {
                "_id": {
                    "namespace": "$namespace",
                    "resource_name": "$resource_name"
                },
                "error_count": {"$sum": 1},
                "error_types": {"$addToSet": "$error_type"},
                "severities": {"$addToSet": "$error_severity"},
                "latest_error": {"$max": "$timestamp"}
            }},
            {"$sort": {"error_count": -1}},
            {"$limit": limit}
        ]
        
        try:
            results = list(self.logs_collection.aggregate(pipeline))
            logger.info(f"Found top {len(results)} error-prone resources")
            return results
        except Exception as e:
            logger.error(f"Error getting top error resources: {e}")
            return []
    
    def _get_processed_log_ids_cached(self) -> List[ObjectId]:
        """Get processed log IDs with caching to improve performance"""
        now = datetime.utcnow()
        
        # Check if cache is valid
        if (self._processed_ids_cache is not None and 
            self._cache_timestamp is not None and
            (now - self._cache_timestamp).total_seconds() < self._cache_duration):
            return self._processed_ids_cache
        
        # Refresh cache
        try:
            # Only get recent processed IDs to keep cache size reasonable
            recent_time = now - timedelta(hours=24)  # Only check last 24 hours
            
            processed_docs = self.debug_collection.find(
                {"created_at": {"$gte": recent_time}}, 
                {"error_log_id": 1}
            )
            
            processed_ids = []
            for doc in processed_docs:
                try:
                    log_id = doc["error_log_id"]
                    # Convert string IDs to ObjectId for proper comparison
                    if isinstance(log_id, str):
                        processed_ids.append(ObjectId(log_id))
                    elif isinstance(log_id, ObjectId):
                        processed_ids.append(log_id)
                except Exception as e:
                    logger.debug(f"Invalid log ID format: {log_id}")
                    continue
            
            # Update cache
            self._processed_ids_cache = processed_ids
            self._cache_timestamp = now
            
            logger.debug(f"Refreshed processed IDs cache with {len(processed_ids)} IDs")
            return processed_ids
            
        except Exception as e:
            logger.error(f"Error getting processed log IDs: {e}")
            return []
    
    def _get_processed_log_ids(self):
        """Get list of log IDs that have already been processed by AI (deprecated, use cached version)"""
        return self._get_processed_log_ids_cached()
    
    def get_unprocessed_count(self, hours_back: int = None) -> int:
        """Get count of unprocessed errors without fetching all documents"""
        hours_back = hours_back or self.config.analysis.hours_back_default
        start_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        processed_ids = self._get_processed_log_ids_cached()
        
        query = {
            "has_error": True,
            "timestamp": {"$gte": start_time},
            "_id": {"$nin": processed_ids}
        }
        
        try:
            count = self.logs_collection.count_documents(query)
            logger.info(f"Found {count} unprocessed errors from last {hours_back} hours")
            return count
        except Exception as e:
            logger.error(f"Error counting unprocessed errors: {e}")
            return 0
    
    def clear_processed_cache(self):
        """Clear the processed IDs cache to force refresh"""
        self._processed_ids_cache = None
        self._cache_timestamp = None
        logger.info("Cleared processed IDs cache")
    
    def get_duplicate_analysis_stats(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get statistics about potential duplicate analyses"""
        try:
            # Find errors that appear multiple times in debug results
            pipeline = [
                {"$match": {
                    "created_at": {"$gte": datetime.utcnow() - timedelta(hours=hours_back)}
                }},
                {"$group": {
                    "_id": {
                        "namespace": "$namespace",
                        "pod_name": "$pod_name", 
                        "error_type": "$error_type"
                    },
                    "analysis_count": {"$sum": 1},
                    "analysis_dates": {"$push": "$created_at"}
                }},
                {"$match": {"analysis_count": {"$gt": 1}}},  # Only duplicates
                {"$sort": {"analysis_count": -1}}
            ]
            
            duplicates = list(self.debug_collection.aggregate(pipeline))
            
            total_duplicates = len(duplicates)
            total_duplicate_analyses = sum(dup["analysis_count"] - 1 for dup in duplicates)  # Subtract 1 for the original
            
            return {
                "duplicate_patterns": total_duplicates,
                "redundant_analyses": total_duplicate_analyses,
                "top_duplicates": duplicates[:5],
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Error getting duplicate analysis stats: {e}")
            return {"error": str(e)}
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("Closed MongoDB connection")