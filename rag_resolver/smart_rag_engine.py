#!/usr/bin/env python3
"""
Smart RAG Engine that can intelligently execute AWS and Kubernetes commands
to automatically resolve issues instead of just generating reports
"""

import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import boto3
from botocore.exceptions import ClientError

from .config import ResolverConfig, RESOLUTION_TEMPLATES, parse_bedrock_response
from .vector_store import VectorStore
from .mcp_executor import MCPExecutor
from .aws_intelligence import AWSIntelligence
from .aws_executor import AWSExecutor

logger = logging.getLogger(__name__)

class SmartRAGEngine:
    """Smart RAG-based intelligent resolution engine with actual execution capabilities"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.vector_store = VectorStore(config)
        self.mcp_executor = MCPExecutor(config)  # Keep for safety validation
        self.aws_intelligence = AWSIntelligence(config)
        self.aws_executor = AWSExecutor(config)  # New intelligent executor
        self.bedrock_client = self._init_bedrock_client()
        
        # Execution settings
        self.enable_smart_execution = True
        self.max_retries = 3
        self.retry_delay = 2
    
    def _init_bedrock_client(self):
        """Initialize AWS Bedrock client for LLM inference"""
        try:
            session = boto3.Session(
                profile_name=self.config.aws.profile,
                region_name=self.config.aws.region
            )
            return session.client('bedrock-runtime')
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise
    
    def validate_error_data(self, error_data: Dict[str, Any]) -> bool:
        """Validate error data structure"""
        required_fields = ["error_type", "namespace", "pod_name"]
        return all(field in error_data and error_data[field] for field in required_fields)
    
    async def resolve_error_smartly(self, error_data: Dict[str, Any], 
                                  auto_execute: bool = True) -> Dict[str, Any]:
        """
        SMART: Intelligently resolve errors by actually executing AWS/K8s commands
        """
        if not self.validate_error_data(error_data):
            return {
                "resolution_id": None,
                "success": False,
                "error": "Invalid error data: missing required fields",
                "resolution_type": "failed"
            }
        
        resolution_start = datetime.utcnow()
        error_type = error_data.get('error_type', 'unknown')
        
        logger.info(f"🚀 Starting SMART resolution for {error_type} error")
        
        try:
            # Step 1: AWS Intelligence Investigation
            aws_investigation = await self._perform_aws_investigation(error_data)
            logger.info(f"AWS Investigation confidence: {aws_investigation.get('resolution_confidence', 0.0):.2f}")
            
            # Step 2: Execute smart resolution based on error type
            if self.enable_smart_execution and auto_execute:
                execution_results = await self._execute_smart_resolution_by_type(
                    error_data, aws_investigation
                )
            else:
                execution_results = {
                    "success": False,
                    "reason": "Smart execution disabled or auto_execute=False",
                    "steps": []
                }
            
            # Step 3: Validate resolution success
            validation_results = await self._validate_smart_resolution(
                error_data, execution_results
            )
            
            # Step 4: Store resolution knowledge
            smart_resolution_data = {
                **error_data,
                "aws_investigation": aws_investigation,
                "execution_results": execution_results,
                "validation_results": validation_results,
                "resolution_duration": (datetime.utcnow() - resolution_start).total_seconds(),
                "smart_execution": True,
                "auto_resolved": execution_results.get("success", False)
            }
            
            resolution_id = await self._store_resolution_knowledge(smart_resolution_data)
            
            return {
                "resolution_id": resolution_id,
                "success": validation_results.get("success", False),
                "resolution_type": "smart_execution",
                "steps_executed": len(execution_results.get("steps", [])),
                "execution_results": execution_results,
                "validation_results": validation_results,
                "aws_investigation": aws_investigation,
                "auto_executed": True,
                "resolution_duration": (datetime.utcnow() - resolution_start).total_seconds(),
                "recommendations": execution_results.get("recommendations", []),
                "resolution_summary": execution_results.get("resolution_summary", ""),
                "intelligence_confidence": aws_investigation.get("resolution_confidence", 0.0),
                "actual_changes_made": execution_results.get("changes_made", [])
            }
            
        except Exception as e:
            logger.error(f"Smart resolution failed: {e}")
            return {
                "resolution_id": None,
                "success": False,
                "error": str(e),
                "resolution_type": "failed",
                "resolution_duration": (datetime.utcnow() - resolution_start).total_seconds(),
                "smart_execution": True
            }
    
    async def _execute_smart_resolution_by_type(self, error_data: Dict[str, Any], 
                                              aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Execute smart resolution based on error type"""
        error_type = error_data.get('error_type', 'unknown')
        
        logger.info(f"🎯 Executing smart resolution for {error_type} error")
        
        try:
            if error_type == 'image_pull':
                return await self.aws_executor.intelligent_image_pull_resolution(error_data)
            
            elif error_type == 'resource_limit':
                return await self.aws_executor.intelligent_resource_limit_resolution(error_data)
            
            elif error_type == 'network':
                return await self.aws_executor.intelligent_network_resolution(error_data)
            
            elif error_type == 'storage':
                return await self._execute_storage_resolution(error_data)
            
            elif error_type == 'node':
                return await self._execute_node_resolution(error_data, aws_investigation)
            
            else:
                # Generic resolution approach
                return await self._execute_generic_resolution(error_data, aws_investigation)
        
        except Exception as e:
            logger.error(f"Error executing smart resolution: {e}")
            return {
                "success": False,
                "error": str(e),
                "steps": []
            }
    
    async def _execute_storage_resolution(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Smartly resolve storage issues"""
        pod_name = error_data.get('pod_name', '')
        namespace = error_data.get('namespace', 'default')
        
        resolution_steps = []
        
        try:
            # Step 1: Check PVC status
            logger.info("💾 Step 1: Checking PVC status...")
            pvc_cmd = f"kubectl get pvc -n {namespace} -o json"
            pvc_result = await self.aws_executor._execute_command(pvc_cmd)
            
            if pvc_result.success:
                pvc_data = json.loads(pvc_result.output)
                
                for pvc in pvc_data.get('items', []):
                    pvc_name = pvc['metadata']['name']
                    pvc_status = pvc['status']['phase']
                    
                    resolution_steps.append({
                        "step": f"Check PVC {pvc_name}",
                        "success": True,
                        "pvc_status": pvc_status
                    })
                    
                    # If PVC is pending, try to fix it
                    if pvc_status == 'Pending':
                        logger.info(f"🔧 Attempting to fix pending PVC {pvc_name}")
                        
                        # Check storage classes
                        sc_cmd = "kubectl get storageclass -o json"
                        sc_result = await self.aws_executor._execute_command(sc_cmd)
                        
                        if sc_result.success:
                            sc_data = json.loads(sc_result.output)
                            
                            # Find default storage class
                            default_sc = None
                            for sc in sc_data.get('items', []):
                                annotations = sc.get('metadata', {}).get('annotations', {})
                                if annotations.get('storageclass.kubernetes.io/is-default-class') == 'true':
                                    default_sc = sc['metadata']['name']
                                    break
                            
                            if default_sc:
                                # Update PVC to use default storage class
                                patch_cmd = f'kubectl patch pvc {pvc_name} -n {namespace} -p \'{{"spec":{{"storageClassName":"{default_sc}"}}}}\''
                                patch_result = await self.aws_executor._execute_command(patch_cmd)
                                
                                resolution_steps.append({
                                    "step": f"Update PVC storage class",
                                    "success": patch_result.success,
                                    "storage_class": default_sc
                                })
            
            # Step 2: Restart pod to retry mounting
            logger.info("🔄 Step 2: Restarting pod to retry storage mounting...")
            delete_cmd = f"kubectl delete pod {pod_name} -n {namespace}"
            delete_result = await self.aws_executor._execute_command(delete_cmd)
            
            resolution_steps.append({
                "step": "Restart pod for storage retry",
                "success": delete_result.success,
                "result": delete_result.output
            })
            
            return {
                "success": delete_result.success,
                "steps": resolution_steps,
                "resolution_summary": "Fixed storage issues and restarted pod",
                "changes_made": [f"Restarted pod {pod_name} to retry storage mounting"]
            }
            
        except Exception as e:
            resolution_steps.append({
                "step": "Storage resolution failed",
                "success": False,
                "error": str(e)
            })
        
        return {"success": False, "steps": resolution_steps}
    
    async def _execute_node_resolution(self, error_data: Dict[str, Any], 
                                     aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Smartly resolve node issues using AWS services"""
        resolution_steps = []
        
        try:
            # Step 1: Check if we need to scale node groups
            logger.info("🖥️ Step 1: Checking EKS node group capacity...")
            
            cluster_name = self.config.aws.cluster_name
            
            # List node groups
            list_ng_cmd = f"aws eks list-nodegroups --cluster-name {cluster_name} --region {self.config.aws.region} --profile {self.config.aws.profile}"
            ng_result = await self.aws_executor._execute_command(list_ng_cmd)
            
            if ng_result.success:
                ng_data = json.loads(ng_result.output)
                
                for ng_name in ng_data.get('nodegroups', []):
                    # Get node group details
                    describe_cmd = f"aws eks describe-nodegroup --cluster-name {cluster_name} --nodegroup-name {ng_name} --region {self.config.aws.region} --profile {self.config.aws.profile}"
                    ng_details_result = await self.aws_executor._execute_command(describe_cmd)
                    
                    if ng_details_result.success:
                        ng_details = json.loads(ng_details_result.output)
                        nodegroup = ng_details['nodegroup']
                        
                        scaling_config = nodegroup.get('scalingConfig', {})
                        desired = scaling_config.get('desiredSize', 0)
                        max_size = scaling_config.get('maxSize', 0)
                        
                        resolution_steps.append({
                            "step": f"Check node group {ng_name}",
                            "success": True,
                            "desired_size": desired,
                            "max_size": max_size
                        })
                        
                        # If we can scale up, do it
                        if desired < max_size:
                            logger.info(f"⬆️ Scaling up node group {ng_name} from {desired} to {desired + 1}")
                            
                            scale_cmd = f"aws eks update-nodegroup-config --cluster-name {cluster_name} --nodegroup-name {ng_name} --scaling-config desiredSize={desired + 1} --region {self.config.aws.region} --profile {self.config.aws.profile}"
                            scale_result = await self.aws_executor._execute_command(scale_cmd)
                            
                            resolution_steps.append({
                                "step": f"Scale up node group {ng_name}",
                                "success": scale_result.success,
                                "new_desired_size": desired + 1
                            })
                            
                            if scale_result.success:
                                return {
                                    "success": True,
                                    "steps": resolution_steps,
                                    "resolution_summary": f"Scaled up node group {ng_name} to add capacity",
                                    "changes_made": [f"Node group {ng_name} scaled from {desired} to {desired + 1}"]
                                }
            
            # Step 2: Check for problematic nodes and cordon them
            logger.info("🔧 Step 2: Checking for problematic nodes...")
            nodes_cmd = "kubectl get nodes -o json"
            nodes_result = await self.aws_executor._execute_command(nodes_cmd)
            
            if nodes_result.success:
                nodes_data = json.loads(nodes_result.output)
                
                for node in nodes_data.get('items', []):
                    node_name = node['metadata']['name']
                    conditions = node.get('status', {}).get('conditions', [])
                    
                    # Check if node is in bad state
                    is_ready = False
                    has_issues = False
                    
                    for condition in conditions:
                        if condition['type'] == 'Ready':
                            is_ready = condition['status'] == 'True'
                        elif condition['type'] in ['MemoryPressure', 'DiskPressure', 'PIDPressure']:
                            if condition['status'] == 'True':
                                has_issues = True
                    
                    if not is_ready or has_issues:
                        logger.info(f"🚫 Node {node_name} has issues, cordoning...")
                        
                        # Cordon the problematic node
                        cordon_cmd = f"kubectl cordon {node_name}"
                        cordon_result = await self.aws_executor._execute_command(cordon_cmd)
                        
                        resolution_steps.append({
                            "step": f"Cordon problematic node {node_name}",
                            "success": cordon_result.success,
                            "node_issues": {"ready": is_ready, "pressure": has_issues}
                        })
            
            return {
                "success": len([s for s in resolution_steps if s["success"]]) > 0,
                "steps": resolution_steps,
                "resolution_summary": "Performed node maintenance and capacity adjustments",
                "changes_made": [f"Performed {len(resolution_steps)} node maintenance actions"]
            }
            
        except Exception as e:
            resolution_steps.append({
                "step": "Node resolution failed",
                "success": False,
                "error": str(e)
            })
        
        return {"success": False, "steps": resolution_steps}
    
    async def _execute_generic_resolution(self, error_data: Dict[str, Any], 
                                        aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Generic smart resolution for unknown error types"""
        pod_name = error_data.get('pod_name', '')
        namespace = error_data.get('namespace', 'default')
        deployment_name = pod_name.split('-')[0] if '-' in pod_name else pod_name
        
        resolution_steps = []
        
        try:
            # Step 1: Get comprehensive pod information
            logger.info("🔍 Step 1: Gathering comprehensive pod information...")
            describe_cmd = f"kubectl describe pod {pod_name} -n {namespace}"
            describe_result = await self.aws_executor._execute_command(describe_cmd)
            
            resolution_steps.append({
                "step": "Gather pod information",
                "success": describe_result.success,
                "info_length": len(describe_result.output) if describe_result.success else 0
            })
            
            # Step 2: Check events for clues
            events_cmd = f"kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name} --sort-by='.lastTimestamp'"
            events_result = await self.aws_executor._execute_command(events_cmd)
            
            resolution_steps.append({
                "step": "Check pod events",
                "success": events_result.success,
                "events_found": len(events_result.output.split('\n')) if events_result.success else 0
            })
            
            # Step 3: Apply generic fix - restart deployment
            logger.info("🔄 Step 3: Applying generic fix - restart deployment...")
            restart_cmd = f"kubectl rollout restart deployment/{deployment_name} -n {namespace}"
            restart_result = await self.aws_executor._execute_command(restart_cmd)
            
            resolution_steps.append({
                "step": "Restart deployment",
                "success": restart_result.success,
                "result": restart_result.output if restart_result.success else restart_result.error
            })
            
            # Step 4: Wait for rollout
            if restart_result.success:
                logger.info("⏳ Step 4: Waiting for deployment rollout...")
                status_cmd = f"kubectl rollout status deployment/{deployment_name} -n {namespace} --timeout=60s"
                status_result = await self.aws_executor._execute_command(status_cmd)
                
                resolution_steps.append({
                    "step": "Wait for rollout completion",
                    "success": status_result.success,
                    "result": status_result.output
                })
                
                return {
                    "success": status_result.success,
                    "steps": resolution_steps,
                    "resolution_summary": f"Applied generic fix: restarted deployment {deployment_name}",
                    "changes_made": [f"Deployment {deployment_name} restarted"]
                }
            
            return {
                "success": restart_result.success,
                "steps": resolution_steps,
                "resolution_summary": "Attempted generic resolution"
            }
            
        except Exception as e:
            resolution_steps.append({
                "step": "Generic resolution failed",
                "success": False,
                "error": str(e)
            })
        
        return {"success": False, "steps": resolution_steps}
    
    async def _validate_smart_resolution(self, error_data: Dict[str, Any], 
                                       execution_results: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if the smart resolution was successful"""
        if not execution_results.get("success", False):
            return {
                "success": False,
                "validation_type": "execution_failed",
                "message": "Resolution execution failed"
            }
        
        try:
            # Wait a bit for changes to take effect
            await asyncio.sleep(15)
            
            pod_name = error_data.get('pod_name', '')
            namespace = error_data.get('namespace', 'default')
            deployment_name = pod_name.split('-')[0] if '-' in pod_name else pod_name
            
            # Check if deployment is healthy
            status_cmd = f"kubectl get deployment {deployment_name} -n {namespace} -o json"
            status_result = await self.aws_executor._execute_command(status_cmd)
            
            if status_result.success:
                deployment_data = json.loads(status_result.output)
                status = deployment_data.get('status', {})
                
                ready_replicas = status.get('readyReplicas', 0)
                desired_replicas = status.get('replicas', 1)
                
                is_healthy = ready_replicas >= desired_replicas
                
                return {
                    "success": is_healthy,
                    "validation_type": "deployment_health_check",
                    "ready_replicas": ready_replicas,
                    "desired_replicas": desired_replicas,
                    "message": "Deployment is healthy" if is_healthy else "Deployment still has issues"
                }
            
            return {
                "success": False,
                "validation_type": "validation_error",
                "message": "Could not validate deployment status"
            }
            
        except Exception as e:
            logger.error(f"Error validating resolution: {e}")
            return {
                "success": False,
                "validation_type": "validation_error",
                "message": f"Validation failed: {str(e)}"
            }
    
    # Keep all the existing methods from the original RAG engine
    async def _perform_aws_investigation(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform intelligent AWS investigation based on error type"""
        error_type = error_data.get('error_type', 'unknown')
        
        logger.info(f"Starting AWS intelligence investigation for {error_type} error")
        
        try:
            if error_type == 'image_pull':
                return await self.aws_intelligence.investigate_image_pull_error(error_data)
            elif error_type == 'resource_limit':
                return await self.aws_intelligence.investigate_resource_limit_error(error_data)
            elif error_type == 'network':
                return await self.aws_intelligence.investigate_network_error(error_data)
            elif error_type == 'node':
                return await self.aws_intelligence.investigate_node_error(error_data)
            else:
                return {
                    "error_type": error_type,
                    "findings": ["AWS intelligence investigation not available for this error type"],
                    "recommendations": ["Use smart execution approach"],
                    "suggested_commands": [],
                    "resolution_confidence": 0.7  # Higher confidence for smart execution
                }
        except Exception as e:
            logger.error(f"AWS investigation failed: {e}")
            return {
                "error_type": error_type,
                "findings": [f"AWS investigation error: {str(e)}"],
                "recommendations": ["Proceed with smart execution"],
                "suggested_commands": [],
                "resolution_confidence": 0.5
            }
    
    async def _store_resolution_knowledge(self, resolution_data: Dict[str, Any]) -> str:
        """Store the resolution knowledge in vector store"""
        try:
            resolution_id = self.vector_store.store_resolution(resolution_data)
            logger.info(f"Stored resolution knowledge: {resolution_id}")
            return resolution_id
        except Exception as e:
            logger.error(f"Error storing resolution knowledge: {e}")
            return ""
    
    # Wrapper method to maintain compatibility
    async def resolve_error(self, error_data: Dict[str, Any], 
                          auto_execute: bool = None) -> Dict[str, Any]:
        """Main resolve_error method that uses smart execution"""
        auto_execute = auto_execute if auto_execute is not None else True
        return await self.resolve_error_smartly(error_data, auto_execute)
    
    # Keep all other existing methods for compatibility
    def get_resolution_stats(self) -> Dict[str, Any]:
        """Get resolution statistics"""
        try:
            return self.vector_store.get_resolution_stats()
        except Exception as e:
            logger.error(f"Error getting resolution stats: {e}")
            return {"error": str(e)}
    
    async def get_resolution_status(self, resolution_id: str) -> Dict[str, Any]:
        """Get the status of a resolution"""
        try:
            resolution_data = self.vector_store.get_resolution_by_id(resolution_id)
            
            if not resolution_data:
                return {"error": "Resolution not found"}
            
            metadata = resolution_data.get("metadata", {})
            
            return {
                "resolution_id": resolution_id,
                "error_type": metadata.get("error_type"),
                "success": metadata.get("success", False),
                "auto_resolved": metadata.get("auto_resolved", False),
                "created_at": metadata.get("created_at"),
                "namespace": metadata.get("namespace"),
                "resolution_type": metadata.get("resolution_type", "unknown"),
                "smart_execution": metadata.get("intelligence_used", False)
            }
            
        except Exception as e:
            logger.error(f"Error getting resolution status: {e}")
            return {"error": str(e)}
    
    async def update_resolution_feedback(self, resolution_id: str, success: bool, 
                                       feedback: str = None) -> bool:
        """Update resolution feedback for learning"""
        try:
            self.vector_store.update_resolution_success(resolution_id, success, feedback)
            logger.info(f"Updated resolution feedback: {resolution_id} -> {success}")
            return True
        except Exception as e:
            logger.error(f"Error updating resolution feedback: {e}")
            return False
    
    def close(self):
        """Close all connections"""
        try:
            self.vector_store.close()
            # Clean up any active MCP sessions
            for session_id in list(self.mcp_executor.active_sessions.keys()):
                self.mcp_executor.close_session(session_id)
            logger.info("Smart RAG engine closed successfully")
        except Exception as e:
            logger.error(f"Error closing smart RAG engine: {e}")