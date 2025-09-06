#!/usr/bin/env python3
"""
AI-Powered Smart RAG Engine that acts like an expert EKS administrator
Generates intelligent, dynamic commands based on error context and cluster state
"""

import asyncio
import json
import logging
import boto3
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class AIResolutionPlan:
    """AI-generated resolution plan with intelligent commands"""
    error_analysis: Dict[str, Any]
    diagnosis_strategy: List[str]
    diagnosis_commands: List[str]
    fix_strategy: List[str]
    fix_commands: List[str]
    validation_commands: List[str]
    risk_assessment: Dict[str, Any]
    confidence_score: float
    reasoning: str

class ExpertEKSAI:
    """AI that acts like an expert EKS administrator"""
    
    def __init__(self, config):
        self.config = config
        self.bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=config.aws.region,
            profile_name=config.aws.profile
        )
        
        # EKS expert knowledge base
        self.eks_knowledge = {
            "node_selectors": {
                "common_labels": ["kubernetes.io/arch", "kubernetes.io/os", "node.kubernetes.io/instance-type", 
                                "topology.kubernetes.io/zone", "eks.amazonaws.com/nodegroup"],
                "troubleshooting_steps": [
                    "Check node labels and taints",
                    "Verify nodeSelector matches available nodes", 
                    "Check for resource constraints",
                    "Validate node group configuration"
                ]
            },
            "image_pull_errors": {
                "common_causes": ["ECR authentication", "Network policies", "Image not found", "Registry down"],
                "investigation_areas": ["IAM roles", "VPC endpoints", "Security groups", "Image URI format"]
            },
            "resource_limits": {
                "memory_patterns": ["OOMKilled", "memory pressure", "limit exceeded"],
                "cpu_patterns": ["throttling", "cpu limit", "performance degradation"],
                "resolution_strategies": ["increase limits", "optimize resources", "horizontal scaling"]
            }
        }
    
    async def analyze_error_with_ai(self, error_data: Dict[str, Any], 
                                  cluster_context: Dict[str, Any] = None) -> AIResolutionPlan:
        """Generate intelligent resolution plan using AI with EKS expertise"""
        
        # Build comprehensive context for AI analysis
        context = await self._build_intelligent_context(error_data, cluster_context)
        
        # Generate AI-powered analysis
        expert_prompt = self._create_expert_eks_prompt(error_data, context)
        
        try:
            # Call AI model for expert analysis
            ai_response = await self._call_ai_model(expert_prompt)
            
            # Parse AI response into structured plan
            resolution_plan = self._parse_ai_resolution_plan(ai_response, error_data)
            
            logger.info(f"🤖 AI generated resolution plan with {resolution_plan.confidence_score:.2f} confidence")
            return resolution_plan
            
        except Exception as e:
            logger.error(f"AI analysis failed, falling back to rule-based approach: {e}")
            return await self._fallback_expert_analysis(error_data, context)
    
    async def _build_intelligent_context(self, error_data: Dict[str, Any], 
                                       cluster_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build comprehensive context for AI analysis"""
        
        context = {
            "error_details": error_data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cluster_info": cluster_context or {},
            "similar_errors": [],
            "cluster_state": {}
        }
        
        # Add EKS-specific context based on error type
        error_type = error_data.get('error_type', 'unknown')
        
        if error_type in self.eks_knowledge:
            context["eks_knowledge"] = self.eks_knowledge[error_type]
        
        # Add resource context if available
        if error_data.get('pod_name'):
            context["resource_context"] = {
                "pod": error_data.get('pod_name'),
                "namespace": error_data.get('namespace', 'default'),
                "node": error_data.get('node_name'),
                "container_info": error_data.get('container_info', {})
            }
        
        return context
    
    def _create_expert_eks_prompt(self, error_data: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Create expert-level prompt for AI analysis"""
        
        error_type = error_data.get('error_type', 'unknown')
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        error_message = error_data.get('error_message', '')
        node_name = error_data.get('node_name', 'unknown')
        
        prompt = f"""
You are a Senior AWS EKS Administrator with 10+ years of experience managing large-scale Kubernetes clusters on AWS. 
You have deep expertise in:
- EKS cluster architecture and networking
- AWS IAM, VPC, and security best practices  
- Kubernetes workload troubleshooting
- Container image management with ECR
- Node group management and auto-scaling
- AWS Load Balancers and Ingress controllers

CURRENT SITUATION:
Error Type: {error_type}
Pod: {pod_name}
Namespace: {namespace}
Node: {node_name}
Error Message: {error_message}

CLUSTER CONTEXT:
{json.dumps(context.get('cluster_info', {}), indent=2)}

RESOURCE CONTEXT:
{json.dumps(context.get('resource_context', {}), indent=2)}

As an expert, you need to:

1. ANALYZE the root cause with deep EKS knowledge
2. CREATE a strategic diagnosis plan with specific kubectl/aws commands
3. GENERATE intelligent fix commands that address the actual problem
4. PROVIDE validation steps to ensure the fix works
5. ASSESS risks and provide rollback strategies

Please provide your expert analysis in this EXACT JSON format:

{{
    "error_analysis": {{
        "root_cause": "Detailed root cause analysis",
        "contributing_factors": ["factor1", "factor2"],
        "impact_assessment": "High/Medium/Low",
        "similar_patterns": ["pattern1", "pattern2"]
    }},
    "diagnosis_strategy": [
        "Strategy step 1: why this step",
        "Strategy step 2: what we're looking for",
        "Strategy step 3: how this helps"
    ],
    "diagnosis_commands": [
        "kubectl get nodes -l kubernetes.io/arch=amd64 --show-labels",
        "kubectl describe pod {pod_name} -n {namespace}",
        "kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name} --sort-by='.lastTimestamp'"
    ],
    "fix_strategy": [
        "Fix approach 1: explanation",
        "Fix approach 2: why this works",
        "Fix approach 3: expected outcome"
    ],
    "fix_commands": [
        "kubectl patch deployment deployment-name -n {namespace} -p '{{\\"spec\\": {{\\"template\\": {{\\"spec\\": {{\\"nodeSelector\\": {{\\"kubernetes.io/arch\\": \\"amd64\\"}}}}}}}}}'",
        "kubectl rollout restart deployment/deployment-name -n {namespace}",
        "kubectl scale deployment deployment-name --replicas=3 -n {namespace}"
    ],
    "validation_commands": [
        "kubectl get pods -n {namespace} -l app=app-name",
        "kubectl describe pod -n {namespace} -l app=app-name",
        "kubectl logs -n {namespace} -l app=app-name --tail=10"
    ],
    "risk_assessment": {{
        "risk_level": "Low/Medium/High", 
        "potential_impacts": ["impact1", "impact2"],
        "rollback_commands": ["rollback1", "rollback2"],
        "safety_checks": ["check1", "check2"]
    }},
    "confidence_score": 0.85,
    "reasoning": "Detailed explanation of why this approach will work and the expected outcomes"
}}

IMPORTANT: 
- Replace {{pod_name}}, {{namespace}} etc. with actual values: {pod_name}, {namespace}
- Use REAL kubectl commands that will work on an actual EKS cluster
- Be specific about node selectors, labels, and AWS-specific configurations
- Consider EKS-specific features like managed node groups, Fargate, etc.
- Include AWS CLI commands if needed for ECR, IAM, or VPC troubleshooting
"""
        
        return prompt
    
    async def _call_ai_model(self, prompt: str) -> str:
        """Call AI model for expert analysis"""
        
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4000,
            "temperature": 0.2,  # Low temperature for consistent expert responses
            "messages": [
                {
                    "role": "user", 
                    "content": prompt
                }
            ]
        }
        
        try:
            response = self.bedrock_client.invoke_model(
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                body=json.dumps(request_body)
            )
            
            response_body = json.loads(response['body'].read())
            return response_body['content'][0]['text']
            
        except Exception as e:
            logger.error(f"Error calling AI model: {e}")
            raise
    
    def _parse_ai_resolution_plan(self, ai_response: str, error_data: Dict[str, Any]) -> AIResolutionPlan:
        """Parse AI response into structured resolution plan"""
        
        try:
            # Extract JSON from AI response
            json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
            if not json_match:
                raise ValueError("No JSON found in AI response")
            
            ai_plan = json.loads(json_match.group())
            
            # Validate required fields
            required_fields = ['error_analysis', 'diagnosis_commands', 'fix_commands', 'confidence_score']
            for field in required_fields:
                if field not in ai_plan:
                    raise ValueError(f"Missing required field: {field}")
            
            # Create structured resolution plan
            resolution_plan = AIResolutionPlan(
                error_analysis=ai_plan.get('error_analysis', {}),
                diagnosis_strategy=ai_plan.get('diagnosis_strategy', []),
                diagnosis_commands=ai_plan.get('diagnosis_commands', []),
                fix_strategy=ai_plan.get('fix_strategy', []),
                fix_commands=ai_plan.get('fix_commands', []),
                validation_commands=ai_plan.get('validation_commands', []),
                risk_assessment=ai_plan.get('risk_assessment', {}),
                confidence_score=float(ai_plan.get('confidence_score', 0.5)),
                reasoning=ai_plan.get('reasoning', 'AI-generated resolution plan')
            )
            
            return resolution_plan
            
        except Exception as e:
            logger.error(f"Error parsing AI response: {e}")
            logger.error(f"AI Response: {ai_response}")
            raise
    
    async def _fallback_expert_analysis(self, error_data: Dict[str, Any], 
                                      context: Dict[str, Any]) -> AIResolutionPlan:
        """Fallback expert analysis when AI fails"""
        
        error_type = error_data.get('error_type', 'unknown')
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        
        # Generate expert-level fallback plan
        if error_type == 'node_selector_mismatch':
            return AIResolutionPlan(
                error_analysis={
                    "root_cause": "Pod nodeSelector doesn't match any available node labels",
                    "contributing_factors": ["Incorrect nodeSelector", "Node labels changed", "New deployment"],
                    "impact_assessment": "High",
                    "similar_patterns": ["scheduling", "node_affinity"]
                },
                diagnosis_strategy=[
                    "Check current node labels and available architectures",
                    "Verify pod's nodeSelector requirements",
                    "Identify the mismatch between requirements and available nodes"
                ],
                diagnosis_commands=[
                    f"kubectl get nodes --show-labels",
                    f"kubectl describe pod {pod_name} -n {namespace}",
                    f"kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name}",
                    f"kubectl get pod {pod_name} -n {namespace} -o yaml | grep -A 10 nodeSelector"
                ],
                fix_strategy=[
                    "Update nodeSelector to match available node labels",
                    "Add appropriate labels to nodes if needed",
                    "Restart deployment to apply changes"
                ],
                fix_commands=[
                    f"kubectl patch deployment {pod_name.split('-')[0]} -n {namespace} -p '{{\"spec\": {{\"template\": {{\"spec\": {{\"nodeSelector\": {{\"kubernetes.io/arch\": \"amd64\"}}}}}}}}}'",
                    f"kubectl rollout restart deployment/{pod_name.split('-')[0]} -n {namespace}"
                ],
                validation_commands=[
                    f"kubectl get pods -n {namespace} -l app={pod_name.split('-')[0]}",
                    f"kubectl rollout status deployment/{pod_name.split('-')[0]} -n {namespace}"
                ],
                risk_assessment={
                    "risk_level": "Low",
                    "potential_impacts": ["Temporary pod restart", "Brief service interruption"],
                    "rollback_commands": [f"kubectl rollout undo deployment/{pod_name.split('-')[0]} -n {namespace}"],
                    "safety_checks": ["Verify deployment has multiple replicas", "Check readiness probes"]
                },
                confidence_score=0.85,
                reasoning="Node selector mismatch is a common, well-understood issue with straightforward resolution"
            )
        
        # Add more fallback patterns for other error types
        elif error_type == 'image_pull':
            return self._generate_image_pull_plan(error_data)
        elif error_type == 'resource_limit':
            return self._generate_resource_limit_plan(error_data)
        else:
            return self._generate_generic_expert_plan(error_data)
    
    def _generate_image_pull_plan(self, error_data: Dict[str, Any]) -> AIResolutionPlan:
        """Generate expert plan for image pull errors"""
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        
        return AIResolutionPlan(
            error_analysis={
                "root_cause": "Container image cannot be pulled from registry",
                "contributing_factors": ["ECR authentication", "Network issues", "Image not found", "IAM permissions"],
                "impact_assessment": "High",
                "similar_patterns": ["registry_auth", "network_policy", "iam_role"]
            },
            diagnosis_strategy=[
                "Check image URI and registry accessibility",
                "Verify ECR authentication and IAM roles",
                "Investigate network connectivity and VPC endpoints"
            ],
            diagnosis_commands=[
                f"kubectl describe pod {pod_name} -n {namespace}",
                f"kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name}",
                f"kubectl get serviceaccount default -n {namespace} -o yaml",
                "aws ecr describe-repositories --region us-west-2",
                f"aws sts get-caller-identity"
            ],
            fix_strategy=[
                "Verify ECR repository exists and image tag is correct",
                "Ensure EKS node IAM role has ECR permissions",
                "Update image URI if needed and restart deployment"
            ],
            fix_commands=[
                f"kubectl delete pod {pod_name} -n {namespace}",
                f"kubectl rollout restart deployment/{pod_name.split('-')[0]} -n {namespace}"
            ],
            validation_commands=[
                f"kubectl get pods -n {namespace} -l app={pod_name.split('-')[0]}",
                f"kubectl logs -n {namespace} -l app={pod_name.split('-')[0]} --tail=20"
            ],
            risk_assessment={
                "risk_level": "Medium",
                "potential_impacts": ["Service downtime", "Image registry costs"],
                "rollback_commands": [f"kubectl rollout undo deployment/{pod_name.split('-')[0]} -n {namespace}"],
                "safety_checks": ["Verify image exists in ECR", "Check IAM policy attachments"]
            },
            confidence_score=0.80,
            reasoning="Image pull errors often stem from ECR authentication or IAM permission issues"
        )
    
    def _generate_resource_limit_plan(self, error_data: Dict[str, Any]) -> AIResolutionPlan:
        """Generate expert plan for resource limit errors"""
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        
        return AIResolutionPlan(
            error_analysis={
                "root_cause": "Pod exceeding memory or CPU resource limits",
                "contributing_factors": ["Undersized limits", "Memory leaks", "Traffic spikes", "Inefficient code"],
                "impact_assessment": "High",
                "similar_patterns": ["oom_killed", "cpu_throttling", "resource_quota"]
            },
            diagnosis_strategy=[
                "Analyze current resource usage and limits",
                "Check historical resource consumption patterns", 
                "Identify if this is a scaling or optimization issue"
            ],
            diagnosis_commands=[
                f"kubectl describe pod {pod_name} -n {namespace}",
                f"kubectl top pod {pod_name} -n {namespace}",
                f"kubectl get events -n {namespace} --field-selector reason=OOMKilled",
                f"kubectl describe nodes | grep -A 5 'Allocated resources'",
                f"kubectl get limitranges -n {namespace}"
            ],
            fix_strategy=[
                "Increase resource limits for the deployment",
                "Enable horizontal pod autoscaling if appropriate",
                "Consider optimizing application resource usage"
            ],
            fix_commands=[
                f"kubectl patch deployment {pod_name.split('-')[0]} -n {namespace} -p '{{\"spec\": {{\"template\": {{\"spec\": {{\"containers\": [{{\"name\": \"main\", \"resources\": {{\"limits\": {{\"memory\": \"1Gi\", \"cpu\": \"500m\"}}}}}}]}}}}}}}'",
                f"kubectl rollout restart deployment/{pod_name.split('-')[0]} -n {namespace}"
            ],
            validation_commands=[
                f"kubectl get pods -n {namespace} -l app={pod_name.split('-')[0]}",
                f"kubectl top pod -n {namespace} -l app={pod_name.split('-')[0]}"
            ],
            risk_assessment={
                "risk_level": "Medium",
                "potential_impacts": ["Increased node resource usage", "Higher costs"],
                "rollback_commands": [f"kubectl rollout undo deployment/{pod_name.split('-')[0]} -n {namespace}"],
                "safety_checks": ["Monitor node resource availability", "Set appropriate resource quotas"]
            },
            confidence_score=0.75,
            reasoning="Resource limit issues require careful balancing of application needs and cluster capacity"
        )
    
    def _generate_generic_expert_plan(self, error_data: Dict[str, Any]) -> AIResolutionPlan:
        """Generate generic expert plan for unknown error types"""
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        
        return AIResolutionPlan(
            error_analysis={
                "root_cause": "Unknown error requiring investigation",
                "contributing_factors": ["Configuration issues", "Resource constraints", "Network problems"],
                "impact_assessment": "Medium",
                "similar_patterns": ["general_troubleshooting"]
            },
            diagnosis_strategy=[
                "Gather comprehensive pod and cluster state information",
                "Check for common EKS issues and misconfigurations",
                "Analyze logs and events for error patterns"
            ],
            diagnosis_commands=[
                f"kubectl describe pod {pod_name} -n {namespace}",
                f"kubectl get events -n {namespace} --sort-by='.lastTimestamp'",
                f"kubectl logs {pod_name} -n {namespace} --tail=50",
                f"kubectl get all -n {namespace}",
                "kubectl get nodes --show-labels"
            ],
            fix_strategy=[
                "Apply general troubleshooting steps",
                "Restart the problematic workload",
                "Monitor for improvement and gather more data"
            ],
            fix_commands=[
                f"kubectl delete pod {pod_name} -n {namespace}",
                f"kubectl rollout restart deployment/{pod_name.split('-')[0]} -n {namespace}"
            ],
            validation_commands=[
                f"kubectl get pods -n {namespace} -l app={pod_name.split('-')[0]}",
                f"kubectl logs -n {namespace} -l app={pod_name.split('-')[0]} --tail=10"
            ],
            risk_assessment={
                "risk_level": "Low",
                "potential_impacts": ["Temporary service interruption"],
                "rollback_commands": [f"kubectl rollout undo deployment/{pod_name.split('-')[0]} -n {namespace}"],
                "safety_checks": ["Verify deployment health after restart"]
            },
            confidence_score=0.60,
            reasoning="Generic troubleshooting approach for unknown issues with standard EKS best practices"
        )


class AISmartRAGEngine:
    """AI-Powered Smart RAG Engine with Expert EKS Knowledge"""
    
    def __init__(self, config):
        self.config = config
        self.expert_ai = ExpertEKSAI(config)
        
        # Import other components
        from .mcp_executor import MCPExecutor
        from .aws_executor import AWSExecutor  
        from .vector_store import VectorStore
        
        self.mcp_executor = MCPExecutor(config, log_to_console=True, log_to_file=True)
        self.aws_executor = AWSExecutor(config)
        self.vector_store = VectorStore(config)
        
        logger.info("🤖 AI-Powered Smart RAG Engine initialized with Expert EKS knowledge")
    
    async def resolve_error_smartly(self, error_data: Dict[str, Any], 
                                  auto_execute: bool = True) -> Dict[str, Any]:
        """Resolve error using AI-generated expert knowledge"""
        
        start_time = datetime.now(timezone.utc)
        error_type = error_data.get('error_type', 'unknown')
        
        logger.info(f"🤖 Starting AI-powered resolution for {error_type} error")
        
        try:
            # Step 1: AI Analysis - Generate expert resolution plan
            logger.info("🧠 Step 1: AI Expert Analysis - Generating intelligent resolution plan...")
            ai_plan = await self.expert_ai.analyze_error_with_ai(error_data)
            
            logger.info(f"✅ AI generated plan with {ai_plan.confidence_score:.2f} confidence")
            logger.info(f"🎯 Root cause: {ai_plan.error_analysis.get('root_cause', 'Unknown')}")
            
            # Step 2: Execute AI-generated diagnosis commands
            logger.info("🔍 Step 2: Executing AI-generated diagnosis commands...")
            session_id = self.mcp_executor.create_session(error_data.get('namespace', 'default'))
            
            diagnosis_results = await self.mcp_executor.execute_diagnosis_commands(
                session_id, ai_plan.diagnosis_commands
            )
            
            # Step 3: Execute AI-generated fix commands (if auto_execute)
            execution_results = {"success": False, "steps": []}
            
            if auto_execute and ai_plan.confidence_score > 0.7:
                logger.info("🛠️ Step 3: Executing AI-generated fix commands...")
                
                fix_results = await self.mcp_executor.execute_fix_commands(
                    session_id, ai_plan.fix_commands, confirm_dangerous=True
                )
                
                # Convert results to steps
                steps = []
                for strategy, command in zip(ai_plan.fix_strategy, ai_plan.fix_commands):
                    result = fix_results.get(command)
                    if result:
                        steps.append({
                            "step": strategy,
                            "success": result.success,
                            "command": command,
                            "result": result.output if result.success else result.error
                        })
                
                execution_results = {
                    "success": all(r.success for r in fix_results.values() if r),
                    "steps": steps,
                    "results": fix_results
                }
            else:
                logger.info("⚠️ Step 3: Skipping auto-execution (confidence too low or disabled)")
                execution_results = {
                    "success": False,
                    "message": f"Auto-execution skipped (confidence: {ai_plan.confidence_score:.2f})",
                    "steps": []
                }
            
            # Step 4: Validation using AI-generated commands
            validation_results = {"success": False}
            if execution_results.get("success") and ai_plan.validation_commands:
                logger.info("✅ Step 4: Validating resolution using AI-generated commands...")
                
                validation_command_results = await self.mcp_executor.execute_diagnosis_commands(
                    session_id, ai_plan.validation_commands
                )
                
                # Simple validation: if validation commands succeed, resolution is likely successful
                validation_results = {
                    "success": all(r.success for r in validation_command_results.values()),
                    "validation_type": "ai_generated",
                    "commands_executed": len(validation_command_results),
                    "message": "AI-generated validation completed"
                }
            
            # Cleanup session
            self.mcp_executor.close_session(session_id)
            
            # Calculate duration
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Store resolution knowledge
            resolution_data = {
                "error_data": error_data,
                "ai_plan": {
                    "error_analysis": ai_plan.error_analysis,
                    "fix_strategy": ai_plan.fix_strategy,
                    "confidence_score": ai_plan.confidence_score,
                    "reasoning": ai_plan.reasoning
                },
                "execution_results": execution_results,
                "validation_results": validation_results,
                "resolution_type": "ai_powered",
                "duration": duration,
                "auto_executed": auto_execute,
                "success": execution_results.get("success", False)
            }
            
            resolution_id = await self._store_resolution_knowledge(resolution_data)
            
            # Build comprehensive response
            return {
                "success": execution_results.get("success", False),
                "resolution_id": resolution_id,
                "resolution_type": "ai_powered",
                "ai_confidence": ai_plan.confidence_score,
                "root_cause": ai_plan.error_analysis.get('root_cause'),
                "fix_strategy": ai_plan.fix_strategy,
                "execution_results": execution_results,
                "validation_results": validation_results,
                "resolution_duration": duration,
                "auto_executed": auto_execute,
                "intelligence_confidence": ai_plan.confidence_score,
                "resolution_summary": f"AI-powered resolution: {ai_plan.reasoning}",
                "steps_executed": len(execution_results.get("steps", [])),
                "actual_changes_made": [
                    step["step"] for step in execution_results.get("steps", []) 
                    if step.get("success")
                ],
                "recommendations": ai_plan.fix_strategy,
                "risk_assessment": ai_plan.risk_assessment
            }
            
        except Exception as e:
            logger.error(f"AI-powered resolution failed: {e}")
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return {
                "success": False,
                "resolution_type": "ai_powered_failed",
                "error": str(e),
                "resolution_duration": duration,
                "auto_executed": auto_execute,
                "intelligence_confidence": 0.0,
                "resolution_summary": f"AI-powered resolution failed: {e}"
            }
    
    async def _store_resolution_knowledge(self, resolution_data: Dict[str, Any]) -> str:
        """Store AI-generated resolution knowledge"""
        try:
            resolution_id = self.vector_store.store_resolution(resolution_data)
            logger.info(f"Stored AI resolution knowledge: {resolution_id}")
            return resolution_id
        except Exception as e:
            logger.error(f"Error storing AI resolution knowledge: {e}")
            return ""
    
    async def close(self):
        """Close all connections and cleanup"""
        try:
            await self.mcp_executor.close()
            self.vector_store.close()
            logger.info("🤖 AI Smart RAG engine closed successfully")
        except Exception as e:
            logger.error(f"Error closing AI Smart RAG engine: {e}")