#!/usr/bin/env python3
"""
AI analyzer module for generating solutions to Kubernetes errors
Handles communication with AWS Bedrock and error analysis logic
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.session import Session

from .config import AIAnalyzerConfig, ERROR_PROMPTS

logger = logging.getLogger(__name__)

class AIErrorAnalyzer:
    """Class to analyze errors using AI and generate solutions"""
    
    def __init__(self, config: AIAnalyzerConfig):
        self.config = config
        self.bedrock_client = self._init_bedrock_client()
        self.analysis_stats = {
            "total_analyzed": 0,
            "successful_analyses": 0,
            "failed_analyses": 0,
            "analyses_by_type": {}
        }
    
    def _init_bedrock_client(self):
        """Initialize AWS Bedrock client"""
        try:
            session = Session(
                profile_name=self.config.aws.profile,
                region_name=self.config.aws.region
            )
            client = session.client("bedrock-runtime")
            
            # Test the connection
            # Note: We can't easily test without making a request, so we'll test on first use
            logger.info(f"Initialized Bedrock client for region {self.config.aws.region}")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise
    
    def analyze_error(self, log_doc: Dict[str, Any]) -> str:
        """Analyze a single error log and get AI suggestions"""
        error_type = log_doc.get("error_type", "default")
        
        try:
            # Select appropriate prompt based on error type
            prompt = self._build_prompt(log_doc, error_type)
            
            # Make API call to Bedrock
            response = self._call_bedrock_api(prompt)
            
            # Update statistics
            self.analysis_stats["successful_analyses"] += 1
            self._update_type_stats(error_type, True)
            
            logger.info(f"Successfully analyzed {error_type} error for {log_doc.get('resource_name', 'unknown')}")
            return response
            
        except Exception as e:
            error_msg = f"Analysis failed: {str(e)}"
            logger.error(f"Failed to analyze error for {log_doc.get('resource_name', 'unknown')}: {e}")
            
            # Update statistics
            self.analysis_stats["failed_analyses"] += 1
            self._update_type_stats(error_type, False)
            
            return error_msg
        finally:
            self.analysis_stats["total_analyzed"] += 1
    
    def batch_analyze_errors(self, log_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze multiple errors in batch with improved error handling"""
        if not log_docs:
            logger.warning("No log documents provided for batch analysis")
            return []
        
        logger.info(f"Starting batch analysis of {len(log_docs)} errors")
        results = []
        
        for i, log_doc in enumerate(log_docs, 1):
            try:
                logger.debug(f"Analyzing error {i}/{len(log_docs)}: {log_doc.get('error_type', 'unknown')} in {log_doc.get('namespace', 'unknown')}/{log_doc.get('resource_name', 'unknown')}")
                
                suggested_fix = self.analyze_error(log_doc)
                
                result = {
                    'log_doc': log_doc,
                    'suggested_fix': suggested_fix,
                    'analysis_timestamp': datetime.utcnow(),
                    'analysis_success': not suggested_fix.startswith("Analysis failed:"),
                    'error_type': log_doc.get('error_type'),
                    'error_severity': log_doc.get('error_severity')
                }
                results.append(result)
                
                # Log progress for large batches
                if len(log_docs) >= 10 and i % 5 == 0:
                    logger.info(f"Batch progress: {i}/{len(log_docs)} analyzed")
                    
            except Exception as e:
                logger.error(f"Unexpected error analyzing log {i}: {e}")
                results.append({
                    'log_doc': log_doc,
                    'suggested_fix': f"Unexpected analysis error: {str(e)}",
                    'analysis_timestamp': datetime.utcnow(),
                    'analysis_success': False,
                    'error_type': log_doc.get('error_type'),
                    'error_severity': log_doc.get('error_severity')
                })
        
        success_count = sum(1 for r in results if r['analysis_success'])
        logger.info(f"Batch analysis completed: {success_count}/{len(results)} successful analyses")
        
        return results
    
    def analyze_by_priority(self, log_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze errors prioritized by severity and type"""
        if not log_docs:
            return []
        
        # Sort by priority (severity and critical error types)
        prioritized_logs = self._prioritize_errors(log_docs)
        
        logger.info(f"Analyzing {len(prioritized_logs)} errors in priority order")
        return self.batch_analyze_errors(prioritized_logs)
    
    def _build_prompt(self, log_doc: Dict[str, Any], error_type: str) -> str:
        """Build the appropriate prompt for the error type"""
        prompt_template = ERROR_PROMPTS.get(error_type, ERROR_PROMPTS['default'])
        
        # Extract metadata safely
        metadata = log_doc.get("metadata", {})
        node_name = "unknown"
        
        if isinstance(metadata, dict):
            node_name = metadata.get("node_name", "unknown")
        
        # Fill in the prompt template
        prompt = prompt_template.format(
            namespace=log_doc.get("namespace", "default"),
            pod=log_doc.get("resource_name", "unknown"),
            node=node_name,
            error_type=error_type or "unknown",
            error_severity=log_doc.get("error_severity", "UNKNOWN"),
            message=log_doc.get("message", "")[:2000]  # Limit message length
        )
        
        return prompt
    
    def _call_bedrock_api(self, prompt: str) -> str:
        """Make API call to AWS Bedrock"""
        try:
            request_body = {
                "messages": [
                    {"role": "user", "content": [{"type": "text", "text": prompt}]}
                ],
                "max_completion_tokens": self.config.analysis.max_tokens,
                "temperature": self.config.analysis.temperature,
                "top_p": self.config.analysis.top_p
            }
            
            response = self.bedrock_client.invoke_model(
                modelId=self.config.aws.model_id,
                body=json.dumps(request_body),
                contentType="application/json",
                accept="application/json",
            )
            
            response_body = json.loads(response["body"].read())
            
            # Extract the response content
            choices = response_body.get("choices", [])
            if choices:
                message = choices[0].get("message", {})
                content = message.get("content", "No response content")
                return content
            else:
                return "No response from AI model"
                
        except (BotoCoreError, ClientError) as e:
            error_msg = f"AWS Bedrock API error: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse Bedrock response: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Unexpected Bedrock error: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
    
    def _prioritize_errors(self, log_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prioritize errors by severity and type"""
        def priority_score(log_doc):
            # Severity priority (lower score = higher priority)
            severity_scores = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4, "INFO": 5}
            severity_score = severity_scores.get(log_doc.get("error_severity", "INFO"), 5)
            
            # Error type priority (critical types get boost)
            error_type = log_doc.get("error_type", "")
            type_bonus = 0
            if error_type in self.config.analysis.critical_error_types:
                type_bonus = -1  # Negative to increase priority
            
            # Timestamp (more recent = higher priority)
            timestamp = log_doc.get("timestamp")
            time_score = 0
            if timestamp:
                # Convert to minutes ago (more recent = lower score)
                time_diff = (datetime.utcnow() - timestamp).total_seconds() / 60
                time_score = min(time_diff / 60, 24)  # Cap at 24 hours
            
            return severity_score + type_bonus + (time_score * 0.1)
        
        return sorted(log_docs, key=priority_score)
    
    def _update_type_stats(self, error_type: str, success: bool):
        """Update statistics for error type analysis"""
        if error_type not in self.analysis_stats["analyses_by_type"]:
            self.analysis_stats["analyses_by_type"][error_type] = {
                "total": 0, "successful": 0, "failed": 0
            }
        
        stats = self.analysis_stats["analyses_by_type"][error_type]
        stats["total"] += 1
        if success:
            stats["successful"] += 1
        else:
            stats["failed"] += 1
    
    def get_analysis_stats(self) -> Dict[str, Any]:
        """Get current analysis statistics"""
        return {
            **self.analysis_stats,
            "success_rate": (
                self.analysis_stats["successful_analyses"] / self.analysis_stats["total_analyzed"] * 100
                if self.analysis_stats["total_analyzed"] > 0 else 0
            ),
            "timestamp": datetime.utcnow()
        }
    
    def reset_stats(self):
        """Reset analysis statistics"""
        self.analysis_stats = {
            "total_analyzed": 0,
            "successful_analyses": 0,
            "failed_analyses": 0,
            "analyses_by_type": {}
        }
        logger.info("Analysis statistics reset")
    
    def test_connection(self) -> bool:
        """Test the Bedrock connection with a simple request"""
        try:
            test_prompt = "Test connection - respond with 'OK'"
            response = self._call_bedrock_api(test_prompt)
            logger.info("Bedrock connection test successful")
            return True
        except Exception as e:
            logger.error(f"Bedrock connection test failed: {e}")
            return False