#!/usr/bin/env python3
"""
Main manager module for AI analysis
Orchestrates error fetching, analysis, and storage
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from .config import load_analyzer_config, AIAnalyzerConfig
from .fetcher import ErrorLogFetcher
from .analyzer import AIErrorAnalyzer
from .storage import DebugResultsManager

logger = logging.getLogger(__name__)

class KubernetesErrorAnalysisManager:
    """Main manager class that orchestrates the entire error analysis process"""
    
    def __init__(self, config: AIAnalyzerConfig = None):
        self.config = config or load_analyzer_config()
        
        # Initialize components
        self.log_fetcher = ErrorLogFetcher(self.config)
        self.ai_analyzer = AIErrorAnalyzer(self.config)
        self.debug_manager = DebugResultsManager(self.config)
        
        # Manager statistics
        self.manager_stats = {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "last_run": None,
            "last_run_results": {}
        }
        
        logger.info("Kubernetes Error Analysis Manager initialized")
    
    def analyze_all_unprocessed_errors(self, hours_back: int = None, 
                                     batch_size: int = None) -> Dict[str, Any]:
        """Analyze all unprocessed errors in batches"""
        hours_back = hours_back or self.config.analysis.hours_back_default
        batch_size = batch_size or self.config.analysis.batch_size
        
        run_start = datetime.utcnow()
        logger.info(f"Starting analysis of all unprocessed errors (last {hours_back} hours, batch size: {batch_size})")
        
        try:
            # Check unprocessed count first
            unprocessed_count = self.log_fetcher.get_unprocessed_count(hours_back)
            if unprocessed_count == 0:
                logger.info("No unprocessed errors found")
                return self._create_run_result(0, 0, [], run_start, "No unprocessed errors")
            
            logger.info(f"Found {unprocessed_count} unprocessed errors to analyze")
            
            # Fetch unprocessed errors
            error_logs = self.log_fetcher.fetch_unprocessed_errors(hours_back, batch_size)
            
            if not error_logs:
                logger.info("No error logs retrieved")
                return self._create_run_result(0, 0, [], run_start, "No error logs retrieved")
            
            logger.info(f"Retrieved {len(error_logs)} error logs for analysis")
            
            # Analyze errors using AI
            analysis_results = self.ai_analyzer.batch_analyze_errors(error_logs)
            
            # Store debug results
            stored_results = self.debug_manager.store_batch_debug_results(analysis_results)
            
            # Update manager statistics
            self._update_stats(True, len(stored_results))
            
            run_duration = (datetime.utcnow() - run_start).total_seconds()
            logger.info(f"Completed analysis of {len(stored_results)} errors in {run_duration:.2f} seconds")
            
            return self._create_run_result(
                len(stored_results), 
                len(error_logs) - len(stored_results),
                stored_results, 
                run_start,
                "Analysis completed successfully"
            )
            
        except Exception as e:
            logger.error(f"Error in analyze_all_unprocessed_errors: {e}")
            self._update_stats(False, 0)
            return self._create_run_result(0, 1, [], run_start, f"Analysis failed: {str(e)}")
    
    def analyze_specific_error_type(self, error_type: str, hours_back: int = 24) -> Dict[str, Any]:
        """Analyze errors of a specific type"""
        run_start = datetime.utcnow()
        logger.info(f"Analyzing {error_type} errors from last {hours_back} hours")
        
        try:
            # Fetch errors of specific type
            error_logs = self.log_fetcher.fetch_errors_by_type(error_type, hours_back)
            
            if not error_logs:
                logger.info(f"No {error_type} errors found")
                return {
                    "error_type": error_type,
                    "processed": 0,
                    "failed": 0,
                    "analysis_timestamp": run_start,
                    "duration_seconds": 0,
                    "message": f"No {error_type} errors found"
                }
            
            # Analyze errors
            analysis_results = self.ai_analyzer.batch_analyze_errors(error_logs)
            
            # Store results
            stored_results = self.debug_manager.store_batch_debug_results(analysis_results)
            
            run_duration = (datetime.utcnow() - run_start).total_seconds()
            logger.info(f"Completed {error_type} analysis: {len(stored_results)} processed in {run_duration:.2f} seconds")
            
            return {
                "error_type": error_type,
                "processed": len(stored_results),
                "failed": len(analysis_results) - len(stored_results),
                "analysis_timestamp": run_start,
                "duration_seconds": run_duration,
                "results": stored_results,
                "message": "Analysis completed successfully"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing {error_type} errors: {e}")
            return {
                "error_type": error_type,
                "processed": 0,
                "failed": 1,
                "analysis_timestamp": run_start,
                "duration_seconds": (datetime.utcnow() - run_start).total_seconds(),
                "message": f"Analysis failed: {str(e)}"
            }
    
    def analyze_critical_errors_only(self, hours_back: int = 1) -> Dict[str, Any]:
        """Analyze only critical and high severity errors"""
        run_start = datetime.utcnow()
        logger.info(f"Analyzing critical/high severity errors from last {hours_back} hours")
        
        try:
            # Fetch critical errors
            error_logs = self.log_fetcher.fetch_critical_errors(hours_back, self.config.analysis.batch_size)
            
            if not error_logs:
                logger.info("No critical errors found")
                return self._create_run_result(0, 0, [], run_start, "No critical errors found")
            
            # Analyze with priority ordering
            analysis_results = self.ai_analyzer.analyze_by_priority(error_logs)
            
            # Store results
            stored_results = self.debug_manager.store_batch_debug_results(analysis_results)
            
            run_duration = (datetime.utcnow() - run_start).total_seconds()
            logger.info(f"Completed critical error analysis: {len(stored_results)} processed in {run_duration:.2f} seconds")
            
            return self._create_run_result(
                len(stored_results),
                len(error_logs) - len(stored_results), 
                stored_results,
                run_start,
                "Critical error analysis completed"
            )
            
        except Exception as e:
            logger.error(f"Error in critical error analysis: {e}")
            return self._create_run_result(0, 1, [], run_start, f"Critical analysis failed: {str(e)}")
    
    def analyze_namespace_errors(self, namespace: str, hours_back: int = 24) -> Dict[str, Any]:
        """Analyze errors from a specific namespace"""
        run_start = datetime.utcnow()
        logger.info(f"Analyzing errors in namespace '{namespace}' from last {hours_back} hours")
        
        try:
            # Fetch namespace errors
            error_logs = self.log_fetcher.fetch_errors_by_namespace(namespace, hours_back)
            
            if not error_logs:
                return {
                    "namespace": namespace,
                    "processed": 0,
                    "failed": 0,
                    "analysis_timestamp": run_start,
                    "message": f"No errors found in namespace '{namespace}'"
                }
            
            # Analyze errors
            analysis_results = self.ai_analyzer.batch_analyze_errors(error_logs)
            
            # Store results
            stored_results = self.debug_manager.store_batch_debug_results(analysis_results)
            
            return {
                "namespace": namespace,
                "processed": len(stored_results),
                "failed": len(analysis_results) - len(stored_results),
                "analysis_timestamp": run_start,
                "duration_seconds": (datetime.utcnow() - run_start).total_seconds(),
                "results": stored_results,
                "message": "Namespace analysis completed"
            }
            
        except Exception as e:
            logger.error(f"Error analyzing namespace {namespace}: {e}")
            return {
                "namespace": namespace,
                "processed": 0,
                "failed": 1,
                "analysis_timestamp": run_start,
                "message": f"Namespace analysis failed: {str(e)}"
            }
    
    def get_comprehensive_report(self) -> Dict[str, Any]:
        """Generate a comprehensive error analysis report"""
        logger.info("Generating comprehensive error analysis report")
        
        try:
            # Get error statistics from logs
            error_stats = self.log_fetcher.get_error_statistics()
            
            # Get error trends
            error_trends = self.log_fetcher.get_error_trends()
            
            # Get top error resources
            top_resources = self.log_fetcher.get_top_error_resources()
            
            # Get debug summary
            debug_summary = self.debug_manager.get_debug_summary()
            
            # Get AI analyzer statistics
            analyzer_stats = self.ai_analyzer.get_analysis_stats()
            
            # Calculate coverage
            total_errors = sum(stat["count"] for stat in error_stats)
            total_analyzed = debug_summary.get("total_analyzed", 0)
            coverage_percentage = (total_analyzed / total_errors * 100) if total_errors > 0 else 0
            
            # Generate recommendations
            recommendations = self._generate_recommendations(error_stats, debug_summary)
            
            return {
                "report_timestamp": datetime.utcnow(),
                "summary": {
                    "total_errors": total_errors,
                    "total_analyzed": total_analyzed,
                    "coverage_percentage": round(coverage_percentage, 2),
                    "pending_analyses": debug_summary.get("pending_count", 0),
                    "resolution_rate": debug_summary.get("resolution_rate", 0)
                },
                "error_statistics": error_stats,
                "error_trends": error_trends,
                "top_error_resources": top_resources,
                "debug_summary": debug_summary,
                "analyzer_statistics": analyzer_stats,
                "manager_statistics": self.get_manager_stats(),
                "recommendations": recommendations
            }
            
        except Exception as e:
            logger.error(f"Error generating comprehensive report: {e}")
            return {
                "report_timestamp": datetime.utcnow(),
                "error": f"Report generation failed: {str(e)}"
            }
    
    def get_system_health_status(self) -> Dict[str, Any]:
        """Get overall system health status based on error analysis"""
        try:
            # Get recent error summary
            debug_summary = self.debug_manager.get_debug_summary(hours_back=1)
            error_stats = self.log_fetcher.get_error_statistics(hours_back=1)
            
            # Calculate health metrics
            total_recent_errors = sum(stat["count"] for stat in error_stats)
            critical_errors = sum(
                stat["count"] for stat in error_stats 
                if stat["_id"]["error_severity"] in ["CRITICAL", "HIGH"]
            )
            
            pending_high_priority = len(self.debug_manager.get_high_priority_results(limit=100))
            
            # Determine health status
            if critical_errors > 10 or pending_high_priority > 20:
                health_status = "CRITICAL"
            elif critical_errors > 5 or total_recent_errors > 50:
                health_status = "WARNING"  
            elif total_recent_errors > 20:
                health_status = "DEGRADED"
            else:
                health_status = "HEALTHY"
            
            return {
                "health_status": health_status,
                "total_recent_errors": total_recent_errors,
                "critical_errors_count": critical_errors,
                "pending_high_priority": pending_high_priority,
                "coverage_rate": debug_summary.get("resolution_rate", 0),
                "last_analysis": self.manager_stats.get("last_run"),
                "system_recommendations": self._get_health_recommendations(health_status, critical_errors, pending_high_priority),
                "timestamp": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Error getting system health status: {e}")
            return {
                "health_status": "UNKNOWN", 
                "error": str(e),
                "timestamp": datetime.utcnow()
            }
    
    def _create_run_result(self, processed: int, failed: int, results: List[Dict[str, Any]], 
                          start_time: datetime, message: str) -> Dict[str, Any]:
        """Create standardized run result"""
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        result = {
            "processed": processed,
            "failed": failed,
            "total_attempted": processed + failed,
            "success_rate": (processed / (processed + failed) * 100) if (processed + failed) > 0 else 0,
            "analysis_timestamp": start_time,
            "duration_seconds": round(duration, 2),
            "message": message,
            "results": results[:10] if results else []  # Limit results for response size
        }
        
        # Store last run results
        self.manager_stats["last_run"] = start_time
        self.manager_stats["last_run_results"] = result
        
        return result
    
    def _update_stats(self, success: bool, processed_count: int):
        """Update manager statistics"""
        self.manager_stats["total_runs"] += 1
        if success:
            self.manager_stats["successful_runs"] += 1
        else:
            self.manager_stats["failed_runs"] += 1
    
    def _generate_recommendations(self, error_stats: List[Dict[str, Any]], 
                                debug_summary: Dict[str, Any]) -> List[str]:
        """Generate high-level recommendations based on error patterns and analysis results"""
        recommendations = []
        
        if not error_stats:
            recommendations.append("System appears stable with minimal errors detected.")
            return recommendations
        
        # Group by error type for analysis
        error_type_counts = {}
        critical_counts = {}
        
        for stat in error_stats:
            error_type = stat["_id"]["error_type"]
            severity = stat["_id"]["error_severity"]
            count = stat["count"]
            
            if error_type:
                error_type_counts[error_type] = error_type_counts.get(error_type, 0) + count
                if severity in ["CRITICAL", "HIGH"]:
                    critical_counts[error_type] = critical_counts.get(error_type, 0) + count
        
        # Generate recommendations based on most common errors
        sorted_errors = sorted(error_type_counts.items(), key=lambda x: x[1], reverse=True)
        
        for error_type, count in sorted_errors[:5]:  # Top 5 error types
            critical_count = critical_counts.get(error_type, 0)
            
            if error_type == "image_pull":
                if count > 10:
                    recommendations.append(f"🔴 High image pull errors ({count} total, {critical_count} critical). Consider implementing image registry caching, pre-pulling critical images, and reviewing image tag policies.")
                elif count > 5:
                    recommendations.append(f"🟡 Moderate image pull errors ({count}). Review image repository access and consider image pre-pulling strategies.")
                    
            elif error_type == "resource_limit":
                if count > 5:
                    recommendations.append(f"🔴 Resource limit issues detected ({count} total, {critical_count} critical). Immediate review of resource requests/limits and cluster scaling recommended.")
                elif count > 2:
                    recommendations.append(f"🟡 Resource pressure detected ({count}). Monitor cluster capacity and consider resource optimization.")
                    
            elif error_type == "network":
                if count > 8:
                    recommendations.append(f"🔴 Network connectivity issues ({count} total). Review service mesh configuration, network policies, and DNS settings.")
                elif count > 3:
                    recommendations.append(f"🟡 Network issues detected ({count}). Check service discovery and connectivity between components.")
                    
            elif error_type == "node":
                if count > 1:
                    recommendations.append(f"🔴 Node-level issues detected ({count}). Immediate cluster health review and node maintenance recommended.")
                    
            elif error_type == "storage":
                if count > 3:
                    recommendations.append(f"🔴 Storage issues detected ({count}). Review PVC bindings, storage classes, and disk space.")
                elif count > 1:
                    recommendations.append(f"🟡 Storage problems detected ({count}). Monitor storage capacity and provisioning.")
                    
            elif error_type == "container":
                if count > 15:
                    recommendations.append(f"🟡 High container lifecycle issues ({count}). Review application health checks, startup configurations, and restart policies.")
                    
            elif error_type == "auth":
                if count > 2:
                    recommendations.append(f"🟡 Authentication/authorization issues ({count}). Review RBAC policies and service account configurations.")
        
        # Analysis coverage recommendations
        total_analyzed = debug_summary.get("total_analyzed", 0)
        total_errors = sum(error_type_counts.values())
        coverage = (total_analyzed / total_errors * 100) if total_errors > 0 else 0
        
        if coverage < 50:
            recommendations.append(f"📊 Low analysis coverage ({coverage:.1f}%). Consider increasing AI analysis frequency to improve error resolution.")
        elif coverage > 90:
            recommendations.append(f"✅ Excellent analysis coverage ({coverage:.1f}%). Error analysis system is performing well.")
        
        # Pending analysis recommendations
        pending_count = debug_summary.get("pending_count", 0)
        if pending_count > 50:
            recommendations.append(f"⏳ High number of pending analyses ({pending_count}). Consider implementing automated resolution workflows.")
        
        if len(recommendations) == 0:
            recommendations.append("✅ System appears stable with well-managed error patterns.")
        
        return recommendations
    
    def _get_health_recommendations(self, health_status: str, critical_errors: int, 
                                  pending_high_priority: int) -> List[str]:
        """Get system health specific recommendations"""
        recommendations = []
        
        if health_status == "CRITICAL":
            recommendations.append("🚨 IMMEDIATE ACTION REQUIRED: System in critical state")
            if critical_errors > 10:
                recommendations.append(f"Address {critical_errors} critical errors immediately")
            if pending_high_priority > 20:
                recommendations.append(f"Review {pending_high_priority} high-priority pending analyses")
                
        elif health_status == "WARNING":
            recommendations.append("⚠️ System requires attention")
            recommendations.append("Increase monitoring frequency and prepare for potential issues")
            
        elif health_status == "DEGRADED":
            recommendations.append("📉 System performance is degraded")
            recommendations.append("Review error trends and implement preventive measures")
            
        else:  # HEALTHY
            recommendations.append("✅ System is operating normally")
            recommendations.append("Continue regular monitoring and maintenance")
        
        return recommendations
    
    def get_manager_stats(self) -> Dict[str, Any]:
        """Get manager statistics"""
        success_rate = 0
        if self.manager_stats["total_runs"] > 0:
            success_rate = (self.manager_stats["successful_runs"] / self.manager_stats["total_runs"]) * 100
        
        return {
            **self.manager_stats,
            "success_rate": round(success_rate, 2),
            "analyzer_stats": self.ai_analyzer.get_analysis_stats(),
            "storage_stats": self.debug_manager.get_statistics()
        }
    
    def test_system_components(self) -> Dict[str, Any]:
        """Test all system components"""
        test_results = {
            "timestamp": datetime.utcnow(),
            "overall_status": "HEALTHY",
            "components": {}
        }
        
        # Test MongoDB connections
        try:
            self.log_fetcher.get_unprocessed_count(hours_back=1)
            test_results["components"]["log_fetcher"] = {"status": "OK", "message": "MongoDB connection successful"}
        except Exception as e:
            test_results["components"]["log_fetcher"] = {"status": "FAILED", "message": str(e)}
            test_results["overall_status"] = "FAILED"
        
        try:
            self.debug_manager.get_statistics()
            test_results["components"]["debug_storage"] = {"status": "OK", "message": "Debug storage accessible"}
        except Exception as e:
            test_results["components"]["debug_storage"] = {"status": "FAILED", "message": str(e)}
            test_results["overall_status"] = "FAILED"
        
        # Test AI analyzer
        try:
            if self.ai_analyzer.test_connection():
                test_results["components"]["ai_analyzer"] = {"status": "OK", "message": "Bedrock connection successful"}
            else:
                test_results["components"]["ai_analyzer"] = {"status": "FAILED", "message": "Bedrock connection failed"}
                test_results["overall_status"] = "FAILED"
        except Exception as e:
            test_results["components"]["ai_analyzer"] = {"status": "FAILED", "message": str(e)}
            test_results["overall_status"] = "FAILED"
        
        return test_results
    
    def cleanup_old_data(self, days_old: int = 30) -> Dict[str, Any]:
        """Clean up old analysis data"""
        try:
            deleted_count = self.debug_manager.cleanup_old_results(days_old)
            logger.info(f"Cleanup completed: {deleted_count} old results removed")
            
            return {
                "cleanup_timestamp": datetime.utcnow(),
                "deleted_count": deleted_count,
                "days_threshold": days_old,
                "status": "SUCCESS"
            }
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return {
                "cleanup_timestamp": datetime.utcnow(),
                "deleted_count": 0,
                "days_threshold": days_old,
                "status": "FAILED",
                "error": str(e)
            }
    
    def close(self):
        """Close all connections and cleanup resources"""
        try:
            self.log_fetcher.close()
            self.debug_manager.close()
            logger.info("All connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")