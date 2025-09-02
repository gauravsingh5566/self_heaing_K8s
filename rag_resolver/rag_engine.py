
import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from rag_resolver.config import ResolverConfig, RESOLUTION_TEMPLATES
    from rag_resolver.vector_store import VectorStore
    from rag_resolver.mcp_executor import MCPExecutor
    from rag_resolver.aws_intelligence import AWSIntelligence
except ImportError:
    from config import ResolverConfig, RESOLUTION_TEMPLATES
    from vector_store import VectorStore
    from mcp_executor import MCPExecutor
    from aws_intelligence import AWSIntelligence

logger = logging.getLogger(__name__)

class RAGEngine:
    """RAG-based intelligent resolution engine with AWS Intelligence integration"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.vector_store = VectorStore(config)
        self.mcp_executor = MCPExecutor(config)
        self.aws_intelligence = AWSIntelligence(config)  # New AWS Intelligence layer
        self.bedrock_client = self._init_bedrock_client()
    
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
    
    async def resolve_error(self, error_data: Dict[str, Any], 
                          auto_execute: bool = None) -> Dict[str, Any]:
        """
    async def resolve_error(self, error_data: Dict[str, Any], 
                          auto_execute: bool = None) -> Dict[str, Any]:
        """
        Main method to resolve a Kubernetes error using RAG + AWS Intelligence
        
        Args:
            error_data: Error information from debug_results
            auto_execute: Whether to auto-execute safe commands
        
        Returns:
            Resolution result with steps, commands, and outcomes
        """
        auto_execute = auto_execute if auto_execute is not None else self.config.auto_execute_safe_commands
        
        resolution_start = datetime.utcnow()
        error_type = error_data.get('error_type', 'unknown')
        logger.info(f"Starting intelligent RAG resolution for error: {error_type}")
        
        try:
            # Step 1: AWS Intelligence Investigation (NEW)
            aws_investigation = await self._perform_aws_investigation(error_data)
            
            # Step 2: Retrieve similar resolutions from vector store
            similar_resolutions = await self._retrieve_similar_resolutions(error_data)
            
            # Step 3: Generate enhanced resolution plan using RAG + AWS Intelligence
            resolution_plan = await self._generate_intelligent_resolution_plan(
                error_data, similar_resolutions, aws_investigation
            )
            
            # Step 4: Execute diagnosis commands with AWS context
            diagnosis_results = await self._execute_intelligent_diagnosis(
                resolution_plan, aws_investigation
            )
            
            # Step 5: Update resolution plan based on diagnosis + AWS findings
            updated_plan = await self._update_plan_with_intelligence(
                resolution_plan, diagnosis_results, aws_investigation, error_data
            )
            
            # Step 6: Execute resolution steps with intelligent commands
            execution_results = await self._execute_intelligent_resolution(updated_plan, auto_execute)
            
            # Step 7: Validate resolution success
            validation_results = await self._validate_resolution(updated_plan, execution_results)
            
            # Step 8: Store enhanced resolution knowledge
            enhanced_resolution_data = {
                **error_data,
                "resolution_plan": updated_plan,
                "aws_investigation": aws_investigation,
                "diagnosis_results": diagnosis_results,
                "execution_results": execution_results,
                "validation_results": validation_results,
                "resolution_duration": (datetime.utcnow() - resolution_start).total_seconds(),
                "intelligence_used": True
            }
            
            resolution_id = await self._store_resolution_knowledge(enhanced_resolution_data)
            
            return {
                "resolution_id": resolution_id,
                "success": validation_results.get("success", False),
                "resolution_type": updated_plan.get("resolution_type", "intelligent"),
                "steps_executed": len(execution_results.get("commands", [])),
                "diagnosis_results": diagnosis_results,
                "execution_results": execution_results,
                "validation_results": validation_results,
                "aws_investigation": aws_investigation,
                "similar_resolutions_found": len(similar_resolutions),
                "auto_executed": auto_execute,
                "resolution_duration": (datetime.utcnow() - resolution_start).total_seconds(),
                "recommendations": updated_plan.get("recommendations", []),
                "manual_steps": updated_plan.get("manual_steps", []),
                "intelligence_confidence": aws_investigation.get("resolution_confidence", 0.0),
                "report_generated": not validation_results.get("success", False)
            }
            
        except Exception as e:
            logger.error(f"Error in intelligent RAG resolution: {e}")
            return {
                "resolution_id": None,
                "success": False,
                "error": str(e),
                "resolution_type": "failed",
                "resolution_duration": (datetime.utcnow() - resolution_start).total_seconds(),
                "intelligence_used": False
            }
    
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
                # For other error types, provide basic investigation
                return {
                    "error_type": error_type,
                    "findings": ["AWS intelligence investigation not available for this error type"],
                    "recommendations": ["Use standard troubleshooting approach"],
                    "suggested_commands": [],
                    "resolution_confidence": 0.5
                }
        except Exception as e:
            logger.error(f"AWS investigation failed: {e}")
            return {
                "error_type": error_type,
                "findings": [f"AWS investigation error: {str(e)}"],
                "recommendations": ["Proceed with standard resolution"],
                "suggested_commands": [],
                "resolution_confidence": 0.3
            }
    
    async def _generate_intelligent_resolution_plan(self, error_data: Dict[str, Any], 
                                                  similar_resolutions: List[Dict[str, Any]],
                                                  aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Generate enhanced resolution plan using RAG + AWS Intelligence"""
        try:
            # Build enhanced context
            rag_context = self._build_rag_context(similar_resolutions)
            aws_context = self._build_aws_context(aws_investigation)
            
            # Create intelligent prompt
            prompt = self._create_intelligent_resolution_prompt(error_data, rag_context, aws_context)
            
            # Generate plan using LLM with enhanced context
            llm_response = await self._call_llm(prompt)
            
            # Parse and structure the response
            resolution_plan = self._parse_resolution_response(llm_response, error_data)
            
            # Enhance with AWS intelligence findings
            resolution_plan = self._enhance_plan_with_aws_intelligence(resolution_plan, aws_investigation)
            
            logger.info(f"Generated intelligent resolution plan with confidence {aws_investigation.get('resolution_confidence', 0.0):.2f}")
            return resolution_plan
            
        except Exception as e:
            logger.error(f"Error generating intelligent resolution plan: {e}")
            # Fallback to AWS intelligence findings
            return self._create_aws_intelligence_plan(error_data, aws_investigation)
    
    def _build_aws_context(self, aws_investigation: Dict[str, Any]) -> str:
        """Build context from AWS intelligence investigation"""
        context_parts = [
            f"AWS Intelligence Investigation Results:",
            f"Confidence: {aws_investigation.get('resolution_confidence', 0.0):.2f}",
            ""
        ]
        
        # Add findings
        findings = aws_investigation.get('findings', [])
        if findings:
            context_parts.append("Key Findings:")
            for finding in findings[:5]:  # Top 5 findings
                context_parts.append(f"- {finding}")
            context_parts.append("")
        
        # Add recommendations  
        recommendations = aws_investigation.get('recommendations', [])
        if recommendations:
            context_parts.append("AWS Recommendations:")
            for rec in recommendations[:3]:  # Top 3 recommendations
                context_parts.append(f"- {rec}")
            context_parts.append("")
        
        return "\n".join(context_parts)
    
    def _create_intelligent_resolution_prompt(self, error_data: Dict[str, Any], 
                                            rag_context: str, aws_context: str) -> str:
        """Create enhanced prompt with RAG + AWS Intelligence context"""
        return f"""
        You are an expert Kubernetes troubleshooter with access to AWS intelligence and previous successful resolutions.
        
        Current Error:
        - Type: {error_data.get('error_type', 'unknown')}
        - Message: {error_data.get('error_message', '')[:300]}
        - Pod: {error_data.get('pod_name', 'unknown')}
        - Namespace: {error_data.get('namespace', 'default')}
        - Severity: {error_data.get('error_severity', 'MEDIUM')}
        
        AWS Intelligence Results:
        {aws_context}
        
        Similar Previous Resolutions:
        {rag_context}
        
        Based on the AWS investigation findings and similar resolutions, provide a comprehensive resolution plan in JSON format:
        {{
            "error_analysis": "Enhanced analysis incorporating AWS findings",
            "resolution_type": "intelligent|automatic|manual",
            "confidence": 0.9,
            "diagnosis_commands": ["kubectl commands based on AWS findings"],
            "fix_commands": ["intelligent fix commands"],
            "validation_commands": ["validation commands"],
            "safe_for_auto_execution": true/false,
            "estimated_risk": "low|medium|high",
            "recommendations": ["AWS-enhanced recommendations"],
            "manual_steps": ["manual steps if needed"],
            "aws_actions_required": ["AWS console/CLI actions if needed"],
            "prevention_tips": ["prevention based on AWS best practices"]
        }}
        
        Prioritize using the AWS investigation findings to make the resolution more intelligent and targeted.
        """
    
    def _enhance_plan_with_aws_intelligence(self, resolution_plan: Dict[str, Any], 
                                          aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance resolution plan with AWS intelligence findings"""
        
        # Use AWS suggested commands if available and better than generated ones
        aws_commands = aws_investigation.get('suggested_commands', [])
        if aws_commands and len(aws_commands) > len(resolution_plan.get('diagnosis_commands', [])):
            resolution_plan['diagnosis_commands'] = aws_commands
        
        # Add AWS-specific recommendations
        aws_recommendations = aws_investigation.get('recommendations', [])
        plan_recommendations = resolution_plan.get('recommendations', [])
        resolution_plan['recommendations'] = aws_recommendations + plan_recommendations
        
        # Add AWS findings to metadata
        resolution_plan['aws_findings'] = aws_investigation.get('findings', [])
        resolution_plan['aws_confidence'] = aws_investigation.get('resolution_confidence', 0.0)
        
        # Adjust confidence based on AWS investigation
        aws_confidence = aws_investigation.get('resolution_confidence', 0.5)
        plan_confidence = resolution_plan.get('confidence', 0.5)
        resolution_plan['confidence'] = (aws_confidence + plan_confidence) / 2
        
        # Mark as intelligent resolution
        resolution_plan['resolution_type'] = 'intelligent'
        resolution_plan['intelligence_enhanced'] = True
        
        return resolution_plan
    
    def _create_aws_intelligence_plan(self, error_data: Dict[str, Any], 
                                    aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Create fallback plan based purely on AWS intelligence"""
        return {
            "error_type": error_data.get("error_type", "unknown"),
            "namespace": error_data.get("namespace", "default"),
            "pod_name": error_data.get("pod_name", ""),
            "error_analysis": f"AWS Intelligence analysis: {', '.join(aws_investigation.get('findings', [])[:2])}",
            "resolution_type": "intelligent",
            "confidence": aws_investigation.get('resolution_confidence', 0.7),
            "diagnosis_commands": aws_investigation.get('suggested_commands', []),
            "fix_commands": aws_investigation.get('suggested_commands', []),
            "validation_commands": [f"kubectl get pod {error_data.get('pod_name', '')} -n {error_data.get('namespace', 'default')}"],
            "safe_for_auto_execution": aws_investigation.get('resolution_confidence', 0.0) > 0.8,
            "estimated_risk": "low" if aws_investigation.get('resolution_confidence', 0.0) > 0.8 else "medium",
            "recommendations": aws_investigation.get('recommendations', []),
            "manual_steps": aws_investigation.get('recommendations', []),
            "aws_findings": aws_investigation.get('findings', []),
            "prevention_tips": ["Monitor AWS resources related to this error type"],
            "intelligence_enhanced": True
        }
    
    async def _execute_intelligent_diagnosis(self, resolution_plan: Dict[str, Any], 
                                           aws_investigation: Dict[str, Any]) -> Dict[str, Any]:
        """Execute diagnosis with AWS intelligence context"""
        try:
            # Use AWS-suggested commands if available, otherwise use plan commands
            diagnosis_commands = resolution_plan.get("diagnosis_commands", [])
            
            if not diagnosis_commands:
                return {"commands": [], "results": {}, "success": True, "message": "No diagnosis commands needed"}
            
            # Create MCP session
            namespace = resolution_plan.get("namespace", "default")
            session_id = self.mcp_executor.create_session(namespace)
            
            # Execute diagnosis commands
            results = await self.mcp_executor.execute_diagnosis_commands(session_id, diagnosis_commands)
            
            # Process results with AWS context
            processed_results = {}
            all_successful = True
            
            for command, result in results.items():
                processed_results[command] = {
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                    "execution_time": result.execution_time,
                    "aws_context": "Executed with AWS intelligence guidance"
                }
                if not result.success:
                    all_successful = False
            
            # Close session
            self.mcp_executor.close_session(session_id)
            
            return {
                "commands": diagnosis_commands,
                "results": processed_results,
                "success": all_successful,
                "session_id": session_id,
                "aws_enhanced": True,
                "intelligence_confidence": aws_investigation.get('resolution_confidence', 0.0)
            }
            
        except Exception as e:
            logger.error(f"Error executing intelligent diagnosis: {e}")
            return {"commands": [], "results": {}, "success": False, "error": str(e)}
    
    async def _update_plan_with_intelligence(self, resolution_plan: Dict[str, Any], 
                                           diagnosis_results: Dict[str, Any],
                                           aws_investigation: Dict[str, Any],
                                           error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update resolution plan with diagnosis results and AWS intelligence"""
        try:
            # If diagnosis failed, rely more heavily on AWS intelligence
            if not diagnosis_results.get("success", False):
                logger.warning("Diagnosis failed, enhancing plan with AWS intelligence")
                
                # Use AWS recommendations as primary guidance
                aws_commands = aws_investigation.get('suggested_commands', [])
                if aws_commands:
                    resolution_plan['fix_commands'] = aws_commands
                    resolution_plan['safe_for_auto_execution'] = aws_investigation.get('resolution_confidence', 0.0) > 0.8
                
                return resolution_plan
            
            # Combine diagnosis results with AWS intelligence for enhanced update
            diagnosis_context = json.dumps(diagnosis_results.get("results", {}), indent=2)
            aws_findings = "\n".join(aws_investigation.get('findings', []))
            
            update_prompt = f"""
            Based on the following information, update the resolution plan intelligently:
            
            Original Error:
            - Type: {error_data.get('error_type')}
            - Message: {error_data.get('error_message', '')[:200]}
            - Pod: {error_data.get('pod_name')}
            - Namespace: {error_data.get('namespace')}
            
            AWS Intelligence Findings:
            {aws_findings}
            
            Diagnostic Results:
            {diagnosis_context}
            
            Original Plan:
            {json.dumps(resolution_plan, indent=2)}
            
            Provide an enhanced resolution plan that incorporates both diagnostic results and AWS intelligence.
            Focus on the most intelligent approach based on all available information.
            """
            
            llm_response = await self._call_llm(update_prompt)
            updated_plan = self._parse_resolution_response(llm_response, error_data)
            
            # Merge with AWS intelligence
            updated_plan["diagnosis_results"] = diagnosis_results
            updated_plan["aws_enhanced"] = True
            updated_plan["intelligence_confidence"] = aws_investigation.get('resolution_confidence', 0.0)
            
            logger.info("Updated resolution plan with AWS intelligence and diagnosis results")
            return updated_plan
            
        except Exception as e:
            logger.error(f"Error updating plan with intelligence: {e}")
            # Fallback to original plan enhanced with AWS intelligence
            resolution_plan["aws_enhanced"] = True
            resolution_plan["intelligence_confidence"] = aws_investigation.get('resolution_confidence', 0.0)
            return resolution_plan
    
    async def _execute_intelligent_resolution(self, resolution_plan: Dict[str, Any], 
                                            auto_execute: bool) -> Dict[str, Any]:
        """Execute resolution with intelligence-based decision making"""
        try:
            fix_commands = resolution_plan.get("fix_commands", [])
            if not fix_commands:
                return {
                    "commands": [],
                    "results": {},
                    "success": True,
                    "message": "No fix commands to execute",
                    "intelligence_enhanced": True
                }
            
            # Enhanced auto-execution logic based on AWS intelligence confidence
            aws_confidence = resolution_plan.get('intelligence_confidence', 0.0)
            plan_safe = resolution_plan.get("safe_for_auto_execution", False)
            
            # More intelligent decision making
            can_auto_execute = auto_execute and (plan_safe or aws_confidence > 0.8)
            
            if not can_auto_execute:
                return {
                    "commands": fix_commands,
                    "results": {},
                    "success": False,
                    "message": f"Resolution requires manual execution (AWS confidence: {aws_confidence:.2f})",
                    "manual_steps": fix_commands,
                    "reason": "Intelligence-based safety check" if aws_confidence < 0.8 else "Auto-execution disabled",
                    "intelligence_enhanced": True
                }
            
            # Create MCP session
            namespace = resolution_plan.get("namespace", "default")
            session_id = self.mcp_executor.create_session(namespace)
            
            # Execute fix commands with intelligence context
            results = await self.mcp_executor.execute_fix_commands(
                session_id, 
                fix_commands,
                confirm_dangerous=aws_confidence > 0.9  # Only confirm dangerous if very confident
            )
            
            # Process results
            processed_results = {}
            all_successful = True
            
            for command, result in results.items():
                processed_results[command] = {
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                    "execution_time": result.execution_time,
                    "safe": result.safe,
                    "intelligence_guided": True,
                    "aws_confidence": aws_confidence
                }
                if not result.success:
                    all_successful = False
            
            # Close session
            self.mcp_executor.close_session(session_id)
            
            return {
                "commands": fix_commands,
                "results": processed_results,
                "success": all_successful,
                "session_id": session_id,
                "auto_executed": True,
                "intelligence_enhanced": True,
                "aws_confidence": aws_confidence
            }
            
        except Exception as e:
            logger.error(f"Error executing intelligent resolution: {e}")
            return {
                "commands": resolution_plan.get("fix_commands", []),
                "results": {},
                "success": False,
                "error": str(e),
                "auto_executed": False,
                "intelligence_enhanced": True
            }
    
    async def _retrieve_similar_resolutions(self, error_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve similar resolutions from vector store"""
        try:
            error_description = self._create_error_description(error_data)
            error_type = error_data.get("error_type")
            
            similar_resolutions = self.vector_store.search_similar_resolutions(
                error_description=error_description,
                error_type=error_type,
                limit=self.config.rag.max_context_documents
            )
            
            logger.info(f"Found {len(similar_resolutions)} similar resolutions")
            return similar_resolutions
            
        except Exception as e:
            logger.error(f"Error retrieving similar resolutions: {e}")
            return []
    
    async def _generate_resolution_plan(self, error_data: Dict[str, Any], 
                                      similar_resolutions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate a resolution plan using RAG with LLM"""
        try:
            # Build context from similar resolutions
            context = self._build_rag_context(similar_resolutions)
            
            # Create prompt for resolution planning
            prompt = self._create_resolution_prompt(error_data, context)
            
            # Generate plan using LLM
            llm_response = await self._call_llm(prompt)
            
            # Parse and structure the response
            resolution_plan = self._parse_resolution_response(llm_response, error_data)
            
            logger.info(f"Generated resolution plan with {len(resolution_plan.get('steps', []))} steps")
            return resolution_plan
            
        except Exception as e:
            logger.error(f"Error generating resolution plan: {e}")
            # Fallback to template-based plan
            return self._create_template_plan(error_data)
    
    async def _execute_diagnosis(self, resolution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute diagnostic commands to gather information"""
        try:
            diagnosis_commands = resolution_plan.get("diagnosis_commands", [])
            if not diagnosis_commands:
                return {"commands": [], "results": {}, "success": True}
            
            # Create MCP session
            namespace = resolution_plan.get("namespace", "default")
            session_id = self.mcp_executor.create_session(namespace)
            
            # Execute diagnosis commands
            results = await self.mcp_executor.execute_diagnosis_commands(session_id, diagnosis_commands)
            
            # Process results
            processed_results = {}
            all_successful = True
            
            for command, result in results.items():
                processed_results[command] = {
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                    "execution_time": result.execution_time
                }
                if not result.success:
                    all_successful = False
            
            # Close session
            self.mcp_executor.close_session(session_id)
            
            return {
                "commands": diagnosis_commands,
                "results": processed_results,
                "success": all_successful,
                "session_id": session_id
            }
            
        except Exception as e:
            logger.error(f"Error executing diagnosis: {e}")
            return {"commands": [], "results": {}, "success": False, "error": str(e)}
    
    async def _update_plan_with_diagnosis(self, resolution_plan: Dict[str, Any], 
                                        diagnosis_results: Dict[str, Any],
                                        error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update resolution plan based on diagnosis results"""
        try:
            # If diagnosis failed, fall back to basic plan
            if not diagnosis_results.get("success", False):
                logger.warning("Diagnosis failed, using basic resolution plan")
                return resolution_plan
            
            # Create prompt to update plan with diagnosis info
            diagnosis_context = json.dumps(diagnosis_results.get("results", {}), indent=2)
            
            update_prompt = f"""
            Based on the following diagnostic information, update the resolution plan:
            
            Original Error:
            - Type: {error_data.get('error_type')}
            - Message: {error_data.get('error_message', '')[:200]}
            - Pod: {error_data.get('pod_name')}
            - Namespace: {error_data.get('namespace')}
            
            Diagnostic Results:
            {diagnosis_context}
            
            Original Plan:
            {json.dumps(resolution_plan, indent=2)}
            
            Please provide an updated resolution plan in JSON format with:
            1. Refined fix commands based on diagnosis
            2. Updated risk assessment
            3. Additional recommendations if needed
            4. Manual steps if automated resolution isnt sufficient
            
            Focus on the most likely root cause based on the diagnostic output.
            """
            
            llm_response = await self._call_llm(update_prompt)
            updated_plan = self._parse_resolution_response(llm_response, error_data)
            
            # Merge with original plan, keeping successful diagnosis results
            updated_plan["diagnosis_results"] = diagnosis_results
            
            logger.info("Updated resolution plan based on diagnosis results")
            return updated_plan
            
        except Exception as e:
            logger.error(f"Error updating plan with diagnosis: {e}")
            return resolution_plan
    
    async def _execute_resolution(self, resolution_plan: Dict[str, Any], 
                                auto_execute: bool) -> Dict[str, Any]:
        """Execute the resolution steps"""
        try:
            fix_commands = resolution_plan.get("fix_commands", [])
            if not fix_commands:
                return {
                    "commands": [],
                    "results": {},
                    "success": True,
                    "message": "No fix commands to execute"
                }
            
            # Check if auto-execution is enabled and safe
            can_auto_execute = auto_execute and resolution_plan.get("safe_for_auto_execution", False)
            
            if not can_auto_execute:
                return {
                    "commands": fix_commands,
                    "results": {},
                    "success": False,
                    "message": "Resolution requires manual execution",
                    "manual_steps": fix_commands,
                    "reason": "Commands not safe for auto-execution or auto-execution disabled"
                }
            
            # Create MCP session
            namespace = resolution_plan.get("namespace", "default")
            session_id = self.mcp_executor.create_session(namespace)
            
            # Execute fix commands
            results = await self.mcp_executor.execute_fix_commands(
                session_id, 
                fix_commands,
                confirm_dangerous=False  # Only execute safe commands
            )
            
            # Process results
            processed_results = {}
            all_successful = True
            
            for command, result in results.items():
                processed_results[command] = {
                    "success": result.success,
                    "output": result.output,
                    "error": result.error,
                    "execution_time": result.execution_time,
                    "safe": result.safe
                }
                if not result.success:
                    all_successful = False
            
            # Close session
            self.mcp_executor.close_session(session_id)
            
            return {
                "commands": fix_commands,
                "results": processed_results,
                "success": all_successful,
                "session_id": session_id,
                "auto_executed": True
            }
            
        except Exception as e:
            logger.error(f"Error executing resolution: {e}")
            return {
                "commands": resolution_plan.get("fix_commands", []),
                "results": {},
                "success": False,
                "error": str(e),
                "auto_executed": False
            }
    
    async def _validate_resolution(self, resolution_plan: Dict[str, Any], 
                                 execution_results: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if the resolution was successful"""
        try:
            # If commands weren't executed, resolution is not complete
            if not execution_results.get("auto_executed", False):
                return {
                    "success": False,
                    "validation_type": "manual_required",
                    "message": "Resolution requires manual intervention",
                    "next_steps": resolution_plan.get("manual_steps", [])
                }
            
            # If any command failed, resolution failed
            if not execution_results.get("success", False):
                return {
                    "success": False,
                    "validation_type": "execution_failed",
                    "message": "Fix commands failed to execute successfully",
                    "failed_commands": [
                        cmd for cmd, result in execution_results.get("results", {}).items()
                        if not result.get("success", False)
                    ]
                }
            
            # For auto-executed commands, validate by re-running diagnosis
            validation_commands = resolution_plan.get("validation_commands", [])
            if validation_commands:
                namespace = resolution_plan.get("namespace", "default")
                session_id = self.mcp_executor.create_session(namespace)
                
                validation_results = await self.mcp_executor.execute_diagnosis_commands(
                    session_id, validation_commands
                )
                
                self.mcp_executor.close_session(session_id)
                
                # Analyze validation results
                success = self._analyze_validation_results(validation_results, resolution_plan)
                
                return {
                    "success": success,
                    "validation_type": "automated",
                    "validation_results": validation_results,
                    "message": "Resolution validated successfully" if success else "Resolution validation failed"
                }
            
            # If no validation commands, assume success if execution succeeded
            return {
                "success": True,
                "validation_type": "execution_based",
                "message": "Resolution completed based on successful command execution"
            }
            
        except Exception as e:
            logger.error(f"Error validating resolution: {e}")
            return {
                "success": False,
                "validation_type": "validation_error",
                "message": f"Validation failed: {str(e)}"
            }
    
    def _analyze_validation_results(self, validation_results: Dict[str, Any], 
                                  resolution_plan: Dict[str, Any]) -> bool:
        """Analyze validation command results to determine success"""
        try:
            error_type = resolution_plan.get("error_type", "")
            
            for command, result in validation_results.items():
                if not result.success:
                    continue
                
                output = result.output.lower()
                
                # Error-type specific validation logic
                if error_type == "image_pull":
                    if "running" in output or "ready" in output:
                        return True
                elif error_type == "resource_limit":
                    if "running" in output and "oomkilled" not in output:
                        return True
                elif error_type == "network":
                    if "running" in output:
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error analyzing validation results: {e}")
            return False
    
    async def _store_resolution_knowledge(self, resolution_data: Dict[str, Any]) -> str:
        """Store the resolution knowledge in vector store"""
        try:
            resolution_id = self.vector_store.store_resolution(resolution_data)
            logger.info(f"Stored resolution knowledge: {resolution_id}")
            return resolution_id
        except Exception as e:
            logger.error(f"Error storing resolution knowledge: {e}")
            return ""
    
    async def _call_llm(self, prompt: str) -> str:
        """Call Bedrock LLM for generation with fallback"""
        try:
            response = self.bedrock_client.invoke_model(
                modelId=self.config.aws.bedrock_model_id,
                body=json.dumps({
                    "messages": [
                        {"role": "user", "content": [{"type": "text", "text": prompt}]}
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.3,
                    "top_p": 0.9
                }),
                contentType="application/json",
                accept="application/json",
            )
            
            response_body = json.loads(response["body"].read())
            return response_body.get("content", [{}])[0].get("text", "")
            
        except ClientError as e:
            if "AccessDeniedException" in str(e):
                logger.warning("Bedrock access denied, using template-based resolution")
                return self._generate_template_response(prompt)
            else:
                logger.error(f"Bedrock client error: {e}")
                return self._generate_template_response(prompt)
        except Exception as e:
            logger.error(f"Error calling LLM: {e}")
            return self._generate_template_response(prompt)
    
    def _generate_template_response(self, prompt: str) -> str:
        """Generate template-based response when LLM is unavailable"""
        # Extract error type from prompt
        error_type = "unknown"
        if "image_pull" in prompt.lower():
            error_type = "image_pull"
        elif "resource" in prompt.lower():
            error_type = "resource_limit"
        elif "network" in prompt.lower():
            error_type = "network"
        
        # Return structured JSON response based on error type
        template_responses = {
            "image_pull": {
                "error_analysis": "Image pull failure - likely due to incorrect image tag or registry access issues",
                "resolution_type": "manual",
                "confidence": 0.7,
                "diagnosis_commands": [
                    "kubectl describe pod {pod_name} -n {namespace}",
                    "kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name}",
                    "kubectl get pod {pod_name} -n {namespace} -o yaml"
                ],
                "fix_commands": [
                    "kubectl delete pod {pod_name} -n {namespace}",
                    "kubectl rollout restart deployment {deployment_name} -n {namespace}"
                ],
                "validation_commands": ["kubectl get pod {pod_name} -n {namespace}"],
                "safe_for_auto_execution": False,
                "estimated_risk": "low",
                "recommendations": [
                    "Verify the image exists in the registry",
                    "Check image tag spelling and case sensitivity", 
                    "Ensure proper registry authentication"
                ],
                "manual_steps": [
                    "Verify image exists: docker pull <image>",
                    "Update deployment with correct image tag",
                    "Monitor pod startup after correction"
                ]
            }
        }
        
        response = template_responses.get(error_type, {
            "error_analysis": "Error detected but specific analysis requires manual review",
            "resolution_type": "manual",
            "confidence": 0.5,
            "diagnosis_commands": ["kubectl describe pod {pod_name} -n {namespace}"],
            "fix_commands": [],
            "safe_for_auto_execution": False,
            "estimated_risk": "medium",
            "recommendations": ["Manual investigation required"],
            "manual_steps": ["Review error details and consult documentation"]
        })
        
        return json.dumps(response, indent=2)
    
    def _create_error_description(self, error_data: Dict[str, Any]) -> str:
        """Create a searchable error description"""
        components = []
        
        error_type = error_data.get("error_type", "")
        error_message = error_data.get("error_message", "")
        namespace = error_data.get("namespace", "")
        pod_name = error_data.get("pod_name", "")
        
        if error_type:
            components.append(f"Error type: {error_type}")
        if error_message:
            components.append(f"Error message: {error_message}")
        if namespace:
            components.append(f"Namespace: {namespace}")
        if pod_name:
            components.append(f"Pod: {pod_name}")
        
        return " ".join(components)
    
    def _build_rag_context(self, similar_resolutions: List[Dict[str, Any]]) -> str:
        """Build context from similar resolutions for RAG"""
        if not similar_resolutions:
            return "No similar resolutions found."
        
        context_parts = []
        for i, resolution in enumerate(similar_resolutions[:3], 1):  # Top 3
            metadata = resolution.get("metadata", {})
            context_parts.append(f"""
            Similar Resolution {i} (Similarity: {resolution.get('score', 0):.2f}):
            - Error Type: {metadata.get('error_type', 'unknown')}
            - Success: {metadata.get('success', False)}
            - Auto Resolved: {metadata.get('auto_resolved', False)}
            - Commands Executed: {metadata.get('commands_executed', 0)}
            - Context: {metadata.get('text_content', '')[:200]}...
            """)
        
        return "\n".join(context_parts)
    
    def _create_resolution_prompt(self, error_data: Dict[str, Any], context: str) -> str:
        """Create prompt for resolution planning"""
        return f"""
        You are an expert Kubernetes troubleshooter with access to previous successful resolutions.
        
        Current Error:
        - Type: {error_data.get('error_type', 'unknown')}
        - Message: {error_data.get('error_message', '')[:300]}
        - Pod: {error_data.get('pod_name', 'unknown')}
        - Namespace: {error_data.get('namespace', 'default')}
        - Severity: {error_data.get('error_severity', 'MEDIUM')}
        
        Similar Previous Resolutions:
        {context}
        
        Please provide a comprehensive resolution plan in JSON format with:
        {{
            "error_analysis": "Brief analysis of the root cause",
            "resolution_type": "automatic|manual|hybrid",
            "confidence": 0.8,
            "diagnosis_commands": ["kubectl command1", "kubectl command2"],
            "fix_commands": ["kubectl fix1", "kubectl fix2"],
            "validation_commands": ["kubectl validate1"],
            "safe_for_auto_execution": true/false,
            "estimated_risk": "low|medium|high",
            "recommendations": ["recommendation1", "recommendation2"],
            "manual_steps": ["manual step1", "manual step2"],
            "prevention_tips": ["tip1", "tip2"]
        }}
        
        Important: Only include safe kubectl commands (get, describe, logs, etc.) in auto-executable commands.
        For dangerous operations (delete, apply, patch), mark safe_for_auto_execution as false and include them in manual_steps.
        """
    
    def _parse_resolution_response(self, llm_response: str, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and validate LLM response into structured resolution plan"""
        try:
            # Try to extract JSON from response
            json_start = llm_response.find('{')
            json_end = llm_response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = llm_response[json_start:json_end]
                parsed_plan = json.loads(json_str)
            else:
                # If no JSON found, create basic plan
                parsed_plan = {}
            
            # Validate and fill in required fields
            resolution_plan = {
                "error_type": error_data.get("error_type", "unknown"),
                "namespace": error_data.get("namespace", "default"),
                "pod_name": error_data.get("pod_name", ""),
                "error_analysis": parsed_plan.get("error_analysis", "Analysis not available"),
                "resolution_type": parsed_plan.get("resolution_type", "manual"),
                "confidence": max(0.0, min(1.0, parsed_plan.get("confidence", 0.5))),
                "diagnosis_commands": parsed_plan.get("diagnosis_commands", []),
                "fix_commands": parsed_plan.get("fix_commands", []),
                "validation_commands": parsed_plan.get("validation_commands", []),
                "safe_for_auto_execution": parsed_plan.get("safe_for_auto_execution", False),
                "estimated_risk": parsed_plan.get("estimated_risk", "medium"),
                "recommendations": parsed_plan.get("recommendations", []),
                "manual_steps": parsed_plan.get("manual_steps", []),
                "prevention_tips": parsed_plan.get("prevention_tips", [])
            }
            
            # Format command templates with actual values
            variables = {
                "pod_name": error_data.get("pod_name", ""),
                "namespace": error_data.get("namespace", "default"),
                "deployment_name": error_data.get("pod_name", "").split('-')[0] if error_data.get("pod_name") else "",
                "container_name": "main",  # Default container name
                "node_name": error_data.get("metadata", {}).get("node_name", "")
            }
            
            # Format diagnosis commands
            resolution_plan["diagnosis_commands"] = [
                self.mcp_executor.format_command_template(cmd, variables)
                for cmd in resolution_plan["diagnosis_commands"]
            ]
            
            # Format fix commands
            resolution_plan["fix_commands"] = [
                self.mcp_executor.format_command_template(cmd, variables)
                for cmd in resolution_plan["fix_commands"]
            ]
            
            return resolution_plan
            
        except Exception as e:
            logger.error(f"Error parsing resolution response: {e}")
            return self._create_template_plan(error_data)
    
    def _create_template_plan(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a fallback resolution plan based on error type templates"""
        error_type = error_data.get("error_type", "unknown")
        template = RESOLUTION_TEMPLATES.get(error_type, RESOLUTION_TEMPLATES.get("default", {}))
        
        variables = {
            "pod_name": error_data.get("pod_name", ""),
            "namespace": error_data.get("namespace", "default"),
            "deployment_name": error_data.get("pod_name", "").split('-')[0] if error_data.get("pod_name") else "",
            "container_name": "main",
            "node_name": error_data.get("metadata", {}).get("node_name", "")
        }
        
        # Format template commands
        diagnosis_commands = [
            self.mcp_executor.format_command_template(cmd, variables)
            for cmd in template.get("diagnosis_commands", [])
        ]
        
        fix_commands = [
            self.mcp_executor.format_command_template(cmd, variables)
            for cmd in template.get("common_fixes", [])
        ]
        
        return {
            "error_type": error_type,
            "namespace": error_data.get("namespace", "default"),
            "pod_name": error_data.get("pod_name", ""),
            "error_analysis": f"Template-based analysis for {error_type} error",
            "resolution_type": "manual",
            "confidence": 0.6,
            "diagnosis_commands": diagnosis_commands,
            "fix_commands": fix_commands,
            "validation_commands": [f"kubectl get pod {variables['pod_name']} -n {variables['namespace']}"],
            "safe_for_auto_execution": False,  # Templates are conservative
            "estimated_risk": "medium",
            "recommendations": [f"Review {error_type} troubleshooting guide"],
            "manual_steps": fix_commands,
            "prevention_tips": ["Implement monitoring and alerting"]
        }
    
    async def generate_resolution_report(self, resolution_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive resolution report for manual intervention"""
        try:
            error_data = resolution_data
            diagnosis_results = resolution_data.get("diagnosis_results", {})
            
            # Generate report using LLM
            report_prompt = f"""
            Generate a comprehensive Kubernetes error resolution report based on the following information:
            
            Error Details:
            - Type: {error_data.get('error_type')}
            - Message: {error_data.get('error_message', '')}
            - Pod: {error_data.get('pod_name')}
            - Namespace: {error_data.get('namespace')}
            - Severity: {error_data.get('error_severity')}
            
            Diagnostic Information:
            {json.dumps(diagnosis_results, indent=2)}
            
            Please provide a detailed report in the following structure:
            
            # Kubernetes Error Resolution Report
            
            ## Executive Summary
            - Brief description of the issue
            - Impact assessment
            - Resolution status
            
            ## Error Analysis
            - Root cause analysis
            - Contributing factors
            - Timeline of events
            
            ## Diagnostic Results
            - Key findings from diagnostic commands
            - Resource status
            - System health indicators
            
            ## Resolution Steps
            - Immediate actions required
            - Step-by-step resolution guide
            - Verification procedures
            
            ## Prevention Recommendations
            - Long-term fixes
            - Monitoring improvements
            - Process enhancements
            
            ## Risk Assessment
            - Potential risks of resolution steps
            - Rollback procedures
            - Safety considerations
            
            Make the report actionable and include specific kubectl commands where appropriate.
            """
            
            report_content = await self._call_llm(report_prompt)
            
            # Structure the report
            report = {
                "report_id": f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                "generated_at": datetime.utcnow().isoformat(),
                "error_type": error_data.get("error_type"),
                "severity": error_data.get("error_severity", "MEDIUM"),
                "pod_name": error_data.get("pod_name"),
                "namespace": error_data.get("namespace"),
                "content": report_content,
                "status": "manual_intervention_required",
                "diagnosis_summary": self._summarize_diagnosis(diagnosis_results),
                "recommended_actions": self._extract_recommended_actions(report_content),
                "estimated_resolution_time": self._estimate_resolution_time(error_data.get("error_type"))
            }
            
            logger.info(f"Generated resolution report: {report['report_id']}")
            return report
            
        except Exception as e:
            logger.error(f"Error generating resolution report: {e}")
            return {
                "report_id": f"report_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                "generated_at": datetime.utcnow().isoformat(),
                "error": str(e),
                "status": "report_generation_failed"
            }
    
    def _summarize_diagnosis(self, diagnosis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize diagnosis results for the report"""
        if not diagnosis_results.get("results"):
            return {"summary": "No diagnosis performed", "key_findings": []}
        
        key_findings = []
        successful_commands = 0
        
        for command, result in diagnosis_results.get("results", {}).items():
            if result.get("success"):
                successful_commands += 1
                # Extract key information from common commands
                if "describe pod" in command.lower():
                    key_findings.append("Pod description obtained")
                elif "get events" in command.lower():
                    key_findings.append("Event history retrieved")
                elif "logs" in command.lower():
                    key_findings.append("Pod logs analyzed")
        
        return {
            "summary": f"{successful_commands}/{len(diagnosis_results.get('results', {}))} diagnostic commands successful",
            "key_findings": key_findings,
            "diagnosis_success": diagnosis_results.get("success", False)
        }
    
    def _extract_recommended_actions(self, report_content: str) -> List[str]:
        """Extract recommended actions from report content"""
        # Simple extraction of action items from the report
        actions = []
        lines = report_content.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('- ') and any(keyword in line.lower() for keyword in ['kubectl', 'run', 'execute', 'check']):
                actions.append(line[2:])  # Remove '- ' prefix
        
        return actions[:5]  # Return top 5 actions
    
    def _estimate_resolution_time(self, error_type: str) -> str:
        """Estimate resolution time based on error type"""
        time_estimates = {
            "image_pull": "5-15 minutes",
            "resource_limit": "10-30 minutes",
            "network": "15-45 minutes",
            "storage": "20-60 minutes",
            "node": "30-120 minutes",
            "container": "10-30 minutes",
            "auth": "5-20 minutes"
        }
        
        return time_estimates.get(error_type, "15-45 minutes")
    
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
                "commands_executed": metadata.get("commands_executed", 0)
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
    
    def get_resolution_stats(self) -> Dict[str, Any]:
        """Get resolution statistics"""
        try:
            return self.vector_store.get_resolution_stats()
        except Exception as e:
            logger.error(f"Error getting resolution stats: {e}")
            return {"error": str(e)}
    
    def close(self):
        """Close all connections"""
        try:
            self.vector_store.close()
            logger.info("RAG engine closed")
        except Exception as e:
            logger.error(f"Error closing RAG engine: {e}")