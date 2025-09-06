#!/usr/bin/env python3
"""
Conversational AI-MCP Engine that maintains continuous dialogue
Model ↔ MCP ↔ Kubernetes in real-time conversation until problem is resolved
"""

import asyncio
import json
import logging
import boto3
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .config import ResolverConfig, parse_bedrock_response

logger = logging.getLogger(__name__)

class ConversationState(Enum):
    INITIAL_ANALYSIS = "initial_analysis"
    INVESTIGATING = "investigating"
    DIAGNOSING = "diagnosing"
    FIXING = "fixing"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class ConversationStep:
    """Single step in AI-MCP conversation"""
    step_number: int
    ai_reasoning: str
    ai_command: str
    mcp_output: str
    mcp_success: bool
    mcp_execution_time: float
    next_action: str
    conversation_state: ConversationState

@dataclass
class ConversationSession:
    """Complete conversation session between AI and MCP"""
    session_id: str
    error_data: Dict[str, Any]
    conversation_steps: List[ConversationStep]
    current_state: ConversationState
    final_resolution: Dict[str, Any]
    total_duration: float
    success: bool

class ConversationalAI:
    """AI that maintains continuous conversation with MCP"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        
        # Initialize Bedrock client
        try:
            session = boto3.Session(profile_name=config.aws.profile)
            self.bedrock_client = session.client('bedrock-runtime', region_name=config.aws.region)
            self.ai_available = True
            logger.info("✅ Conversational AI initialized with Bedrock")
        except Exception as e:
            logger.warning(f"⚠️ Bedrock unavailable: {e}")
            self.ai_available = False
    
    async def start_conversation(self, error_data: Dict[str, Any], 
                               conversation_history: List[ConversationStep] = None) -> str:
        """Start or continue conversation with MCP about the error"""
        
        if not conversation_history:
            # Initial conversation start
            return await self._create_initial_analysis_prompt(error_data)
        else:
            # Continue existing conversation
            return await self._create_continuation_prompt(error_data, conversation_history)
    
    async def _create_initial_analysis_prompt(self, error_data: Dict[str, Any]) -> str:
        """Create initial analysis prompt to start conversation"""
        
        error_type = error_data.get('error_type', 'unknown')
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        error_message = error_data.get('error_message', '')
        
        prompt = f"""
You are an Expert Kubernetes Administrator having a LIVE CONVERSATION with a Kubernetes cluster through an MCP executor.

SITUATION:
- Error Type: {error_type}
- Pod: {pod_name}  
- Namespace: {namespace}
- Error Message: {error_message}
- Cluster: {self.config.aws.cluster_name}

You will have a BACK-AND-FORTH CONVERSATION where:
1. You suggest ONE kubectl command
2. MCP executes it and shows you the output
3. You analyze the output and suggest the NEXT command
4. This continues until the problem is SOLVED

START the conversation by suggesting your FIRST diagnostic command to understand the problem.

Respond in this EXACT format:
{{
    "reasoning": "Why I want to run this command and what I expect to learn",
    "command": "kubectl get pods -n {namespace}",
    "expected_outcome": "What this command should reveal",
    "next_steps": "What I plan to do based on the results",
    "conversation_state": "investigating"
}}

Remember: Suggest ONLY ONE command at a time. We'll have a conversation!
"""
        
        return prompt
    
    async def _create_continuation_prompt(self, error_data: Dict[str, Any], 
                                        conversation_history: List[ConversationStep]) -> str:
        """Create continuation prompt based on conversation history"""
        
        last_step = conversation_history[-1]
        error_type = error_data.get('error_type', 'unknown')
        pod_name = error_data.get('pod_name', 'unknown')
        namespace = error_data.get('namespace', 'default')
        
        # Build conversation history
        history_text = "\n".join([
            f"Step {step.step_number}: {step.ai_reasoning}\n"
            f"Command: {step.ai_command}\n"
            f"Output: {step.mcp_output[:300]}...\n"
            f"Success: {step.mcp_success}\n"
            for step in conversation_history[-3:]  # Last 3 steps for context
        ])
        
        prompt = f"""
You are continuing a LIVE CONVERSATION with a Kubernetes cluster to resolve a {error_type} error.

CONVERSATION HISTORY:
{history_text}

LAST COMMAND RESULT:
Command: {last_step.ai_command}
Success: {last_step.mcp_success}
Output: {last_step.mcp_output}

Based on this output, what's your NEXT action? Analyze what you learned and suggest the NEXT command.

Current State: {last_step.conversation_state.value}

Respond in this EXACT format:
{{
    "reasoning": "What I learned from the last command and why I want to run the next command",
    "command": "kubectl describe pod {pod_name} -n {namespace}",
    "expected_outcome": "What this next command should reveal",
    "next_steps": "My plan based on what I expect to find",
    "conversation_state": "diagnosing|fixing|validating|completed"
}}

If the problem is SOLVED, use conversation_state: "completed" and command: "RESOLUTION_COMPLETE"
If you need to apply a FIX, use conversation_state: "fixing"
If you're still investigating, use conversation_state: "investigating" or "diagnosing"
"""
        
        return prompt
    
    async def get_ai_response(self, prompt: str) -> Dict[str, Any]:
        """Get AI response and parse it"""
        
        if not self.ai_available:
            # Fallback response if AI not available
            return {
                "reasoning": "AI not available, using basic troubleshooting",
                "command": "kubectl get pods -n default",
                "expected_outcome": "Check pod status",
                "next_steps": "Proceed based on output",
                "conversation_state": "investigating"
            }
        
        try:
            # Call Bedrock
            response = await self._call_bedrock(prompt)
            
            # Parse JSON response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            else:
                raise ValueError("No JSON found in AI response")
                
        except Exception as e:
            logger.error(f"AI response error: {e}")
            # Fallback response
            return {
                "reasoning": f"AI error ({e}), using fallback",
                "command": "kubectl describe pod",
                "expected_outcome": "Basic pod information",
                "next_steps": "Manual analysis needed",
                "conversation_state": "investigating"
            }
    
    async def _call_bedrock(self, prompt: str) -> str:
        """Call Bedrock API with correct parameters"""
        
        model_id = self.config.aws.bedrock_model_id
        
        if "gpt" in model_id.lower():
            # OpenAI format - FIXED: Use max_completion_tokens instead of max_tokens
            request_body = {
                "model": model_id,
                "messages": [{"role": "user", "content": prompt}],
                "max_completion_tokens": 2000,  # FIXED: Correct parameter name
                "temperature": 0.3
            }
        elif "claude" in model_id.lower():
            # Anthropic Claude format
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
                "temperature": 0.3,
                "messages": [{"role": "user", "content": prompt}]
            }
        else:
            # Generic format
            request_body = {
                "inputText": prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 2000,
                    "temperature": 0.3
                }
            }
        
        response = self.bedrock_client.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )
        
        response_body = json.loads(response['body'].read())
        return parse_bedrock_response(response_body, model_id)


class ConversationalMCPExecutor:
    """MCP Executor that participates in conversations with AI"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        
        # Import your existing MCP executor
        from .mcp_executor import MCPExecutor
        self.mcp_executor = MCPExecutor(config, log_to_console=True, log_to_file=True)
        
        logger.info("🗣️ Conversational MCP Executor initialized")
    
    async def execute_command_with_conversation(self, session_id: str, 
                                              command: str, 
                                              step_number: int) -> ConversationStep:
        """Execute command and format response for AI conversation"""
        
        print(f"\n🤖 AI suggests: {command}")
        print("🔧 MCP executing...")
        
        start_time = datetime.now()
        
        try:
            # Execute the command
            result = await self.mcp_executor.execute_command(session_id, command)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Format output for AI
            if result.success:
                formatted_output = f"SUCCESS (exit_code: {result.exit_code}, time: {execution_time:.2f}s)\n{result.output}"
                print(f"✅ Command succeeded in {execution_time:.2f}s")
                if result.output:
                    print(f"📝 Output: {result.output[:200]}...")
            else:
                formatted_output = f"FAILED (exit_code: {result.exit_code}, time: {execution_time:.2f}s)\nSTDERR: {result.error}"
                print(f"❌ Command failed in {execution_time:.2f}s")
                print(f"⚠️ Error: {result.error[:200]}...")
            
            return ConversationStep(
                step_number=step_number,
                ai_reasoning="",  # Will be filled by AI
                ai_command=command,
                mcp_output=formatted_output,
                mcp_success=result.success,
                mcp_execution_time=execution_time,
                next_action="",  # Will be filled by AI
                conversation_state=ConversationState.INVESTIGATING
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            print(f"💥 Command execution error: {e}")
            
            return ConversationStep(
                step_number=step_number,
                ai_reasoning="",
                ai_command=command,
                mcp_output=f"EXECUTION_ERROR: {str(e)}",
                mcp_success=False,
                mcp_execution_time=execution_time,
                next_action="",
                conversation_state=ConversationState.FAILED
            )
    
    def create_session(self, namespace: str) -> str:
        """Create MCP session"""
        return self.mcp_executor.create_session(namespace)
    
    def close_session(self, session_id: str):
        """Close MCP session"""
        self.mcp_executor.close_session(session_id)


class ConversationalAIMCPEngine:
    """Main engine that orchestrates AI ↔ MCP conversation"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.ai = ConversationalAI(config)
        self.mcp = ConversationalMCPExecutor(config)
        
        # Import other components for compatibility
        from .vector_store import VectorStore
        self.vector_store = VectorStore(config)
        
        logger.info("🗣️ Conversational AI-MCP Engine initialized")
    
    async def start_conversation_resolution(self, error_data: Dict[str, Any], 
                                          max_conversation_steps: int = 10,
                                          auto_execute: bool = True) -> ConversationSession:
        """Start a conversational resolution session"""
        
        session_id = f"conv_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        namespace = error_data.get('namespace', 'default')
        
        print(f"\n🗣️ Starting AI ↔ MCP Conversation")
        print(f"   Session: {session_id}")
        print(f"   Error: {error_data.get('error_type')} in {namespace}")
        print(f"   Max Steps: {max_conversation_steps}")
        print("=" * 60)
        
        # Create MCP session
        mcp_session = self.mcp.create_session(namespace)
        
        conversation_steps = []
        current_state = ConversationState.INITIAL_ANALYSIS
        start_time = datetime.now()
        
        try:
            for step_num in range(1, max_conversation_steps + 1):
                print(f"\n💬 Conversation Step {step_num}")
                print("-" * 30)
                
                # AI generates next action
                print("🤖 AI is thinking...")
                
                if step_num == 1:
                    # Initial analysis
                    prompt = await self.ai.start_conversation(error_data)
                else:
                    # Continue conversation
                    prompt = await self.ai.start_conversation(error_data, conversation_steps)
                
                ai_response = await self.ai.get_ai_response(prompt)
                
                print(f"🧠 AI Reasoning: {ai_response.get('reasoning', 'No reasoning provided')}")
                
                # Check if AI thinks we're done
                command = ai_response.get('command', '')
                if command == "RESOLUTION_COMPLETE" or ai_response.get('conversation_state') == 'completed':
                    print("✅ AI believes the problem is resolved!")
                    current_state = ConversationState.COMPLETED
                    break
                
                # MCP executes the command
                if auto_execute and command:
                    conversation_step = await self.mcp.execute_command_with_conversation(
                        mcp_session, command, step_num
                    )
                    
                    # Fill in AI details
                    conversation_step.ai_reasoning = ai_response.get('reasoning', '')
                    conversation_step.next_action = ai_response.get('next_steps', '')
                    conversation_step.conversation_state = ConversationState(
                        ai_response.get('conversation_state', 'investigating')
                    )
                    
                    conversation_steps.append(conversation_step)
                    current_state = conversation_step.conversation_state
                    
                    # AI analyzes the output and continues
                    print(f"🎯 AI Next Steps: {ai_response.get('next_steps', 'Analyzing...')}")
                    
                    # Small delay for readability
                    await asyncio.sleep(1)
                    
                    # Check if we've reached a terminal state
                    if current_state in [ConversationState.COMPLETED, ConversationState.FAILED]:
                        break
                        
                else:
                    print(f"⚠️ Auto-execute disabled or no command provided")
                    break
            
            # Calculate final duration
            total_duration = (datetime.now() - start_time).total_seconds()
            
            # Determine final success
            final_success = (current_state == ConversationState.COMPLETED and 
                           len([s for s in conversation_steps if s.mcp_success]) > 0)
            
            # Create final resolution summary
            final_resolution = {
                "conversation_summary": self._create_conversation_summary(conversation_steps),
                "total_commands_executed": len(conversation_steps),
                "successful_commands": len([s for s in conversation_steps if s.mcp_success]),
                "final_state": current_state.value,
                "resolution_achieved": final_success
            }
            
            print(f"\n🏁 Conversation Completed")
            print(f"   Total Steps: {len(conversation_steps)}")
            print(f"   Duration: {total_duration:.2f}s")
            print(f"   Success: {'✅' if final_success else '❌'}")
            print(f"   Final State: {current_state.value}")
            
        except Exception as e:
            logger.error(f"Conversation error: {e}")
            total_duration = (datetime.now() - start_time).total_seconds()
            final_resolution = {"error": str(e)}
            final_success = False
            
        finally:
            # Cleanup MCP session
            self.mcp.close_session(mcp_session)
        
        # Create conversation session object
        session = ConversationSession(
            session_id=session_id,
            error_data=error_data,
            conversation_steps=conversation_steps,
            current_state=current_state,
            final_resolution=final_resolution,
            total_duration=total_duration,
            success=final_success
        )
        
        # Store conversation for learning
        await self._store_conversation(session)
        
        return session
    
    def _create_conversation_summary(self, steps: List[ConversationStep]) -> str:
        """Create a summary of the conversation"""
        
        summary_parts = []
        for step in steps:
            status = "✅" if step.mcp_success else "❌"
            summary_parts.append(
                f"Step {step.step_number}: {status} {step.ai_command} "
                f"({step.mcp_execution_time:.2f}s)"
            )
        
        return "\n".join(summary_parts)
    
    async def _store_conversation(self, session: ConversationSession):
        """Store conversation session for learning and analysis"""
        
        try:
            # Store in vector store for future learning
            conversation_data = {
                "session_id": session.session_id,
                "error_data": session.error_data,
                "conversation_steps": [
                    {
                        "step": step.step_number,
                        "reasoning": step.ai_reasoning,
                        "command": step.ai_command,
                        "success": step.mcp_success,
                        "execution_time": step.mcp_execution_time,
                        "state": step.conversation_state.value
                    }
                    for step in session.conversation_steps
                ],
                "final_resolution": session.final_resolution,
                "success": session.success,
                "duration": session.total_duration,
                "resolution_type": "conversational_ai_mcp"
            }
            
            resolution_id = self.vector_store.store_resolution(conversation_data)
            logger.info(f"Stored conversation session: {resolution_id}")
            
        except Exception as e:
            logger.error(f"Error storing conversation: {e}")
    
    # Compatibility methods for existing interface
    async def resolve_error_smartly(self, error_data: Dict[str, Any], 
                                  auto_execute: bool = True) -> Dict[str, Any]:
        """Main resolution method using conversational approach"""
        
        # Start conversation
        session = await self.start_conversation_resolution(error_data, auto_execute=auto_execute)
        
        # Convert to expected format
        return {
            "success": session.success,
            "resolution_id": session.session_id,
            "resolution_type": "conversational_ai_mcp",
            "conversation_steps": len(session.conversation_steps),
            "resolution_duration": session.total_duration,
            "auto_executed": auto_execute,
            "final_state": session.current_state.value,
            "resolution_summary": session.final_resolution.get("conversation_summary", ""),
            "actual_changes_made": [
                f"Step {s.step_number}: {s.ai_command}" 
                for s in session.conversation_steps if s.mcp_success
            ],
            "intelligence_confidence": 0.85 if session.success else 0.5,
            "cluster_name": self.config.aws.cluster_name,
            "namespace": error_data.get('namespace', 'default')
        }
    
    async def close(self):
        """Close all connections"""
        try:
            self.vector_store.close()
            logger.info("🗣️ Conversational AI-MCP engine closed")
        except Exception as e:
            logger.error(f"Error closing conversational engine: {e}")


# Alias for compatibility with your existing code
AISmartRAGEngine = ConversationalAIMCPEngine