#!/usr/bin/env python3
"""
MCP (Model Context Protocol) executor for running kubectl commands
in EKS cluster with safety controls and validation
FIXED: Command template formatting with proper error handling
"""

import asyncio
import json
import logging
import subprocess
import shlex
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

from .config import ResolverConfig, SAFE_KUBECTL_COMMANDS, DANGEROUS_KUBECTL_COMMANDS

logger = logging.getLogger(__name__)

@dataclass
class CommandResult:
    """Result of a command execution"""
    success: bool
    output: str
    error: str
    exit_code: int
    execution_time: float
    command: str
    safe: bool

@dataclass
class MCPSession:
    """MCP session information"""
    session_id: str
    cluster_context: str
    namespace: str
    start_time: datetime
    commands_executed: List[str]

class MCPExecutor:
    """MCP-based executor for safe kubectl command execution"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.active_sessions = {}
        self._init_kubectl_context()
    
    def _init_kubectl_context(self):
        """Initialize kubectl context for EKS cluster"""
        try:
            # Update kubeconfig for EKS cluster
            update_cmd = [
                "aws", "eks", "update-kubeconfig",
                "--region", self.config.aws.region,
                "--name", self.config.aws.cluster_name,
                "--profile", self.config.aws.profile
            ]
            
            result = subprocess.run(
                update_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully configured kubectl for cluster: {self.config.aws.cluster_name}")
            else:
                logger.error(f"Failed to configure kubectl: {result.stderr}")
                raise Exception(f"Kubectl configuration failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error initializing kubectl context: {e}")
            raise
    
    def create_session(self, namespace: str = "default") -> str:
        """Create a new MCP session for command execution"""
        session_id = f"mcp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{len(self.active_sessions)}"
        
        session = MCPSession(
            session_id=session_id,
            cluster_context=f"{self.config.aws.cluster_name}",
            namespace=namespace,
            start_time=datetime.utcnow(),
            commands_executed=[]
        )
        
        self.active_sessions[session_id] = session
        logger.info(f"Created MCP session: {session_id} for namespace: {namespace}")
        
        return session_id
    
    def validate_command(self, command: str, namespace: str = None) -> Tuple[bool, str, str]:
        """Validate kubectl command for safety and security"""
        try:
            parts = shlex.split(command)
        except ValueError as e:
            return False, "invalid", f"Invalid command syntax: {e}"
        
        if not parts or parts[0] != "kubectl":
            return False, "invalid", "Only kubectl commands are allowed"
        
        if len(parts) < 2:
            return False, "invalid", "Incomplete kubectl command"
        
        action = parts[1].lower()
        
        if action in DANGEROUS_KUBECTL_COMMANDS:
            return False, "dangerous", f"Command '{action}' is not allowed for safety reasons"
        
        if action in SAFE_KUBECTL_COMMANDS:
            safety_level = "safe"
        else:
            safety_level = "moderate"
        
        if namespace and namespace not in self.config.mcp.allowed_namespaces:
            return False, "restricted", f"Namespace '{namespace}' is not in allowed list"
        
        if action == "delete":
            return False, "dangerous", "Delete operations are not permitted"
        
        if action in ["apply", "create", "patch"]:
            if any(dangerous in command.lower() for dangerous in ["--force", "--grace-period=0"]):
                return False, "dangerous", "Forced operations are not permitted"
        
        return True, safety_level, "Command validated successfully"
    
    async def execute_command(self, session_id: str, command: str, 
                            timeout: int = None) -> CommandResult:
        """Execute a kubectl command safely with MCP session context"""
        timeout = timeout or self.config.mcp.timeout_seconds
        
        if session_id not in self.active_sessions:
            return CommandResult(
                success=False,
                output="",
                error="Invalid session ID",
                exit_code=-1,
                execution_time=0.0,
                command=command,
                safe=False
            )
        
        session = self.active_sessions[session_id]
        
        # Validate command
        is_safe, safety_level, reason = self.validate_command(command, session.namespace)
        
        if not is_safe:
            logger.warning(f"Command blocked: {command} - Reason: {reason}")
            return CommandResult(
                success=False,
                output="",
                error=f"Command blocked: {reason}",
                exit_code=-1,
                execution_time=0.0,
                command=command,
                safe=False
            )
        
        # Add namespace context if not specified
        if "-n " not in command and "--namespace" not in command and session.namespace != "default":
            command += f" -n {session.namespace}"
        
        start_time = datetime.utcnow()
        
        try:
            # Execute command with asyncio subprocess
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout
            )
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            # Decode output
            stdout_str = stdout.decode('utf-8') if stdout else ""
            stderr_str = stderr.decode('utf-8') if stderr else ""
            
            # Log command execution
            session.commands_executed.append(command)
            logger.info(f"Executed command in session {session_id}: {command[:100]}...")
            
            result = CommandResult(
                success=process.returncode == 0,
                output=stdout_str,
                error=stderr_str,
                exit_code=process.returncode,
                execution_time=execution_time,
                command=command,
                safe=safety_level in ["safe", "moderate"]
            )
            
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Command timeout: {command}")
            return CommandResult(
                success=False,
                output="",
                error=f"Command timed out after {timeout} seconds",
                exit_code=-1,
                execution_time=timeout,
                command=command,
                safe=is_safe
            )
            
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.error(f"Command execution error: {e}")
            return CommandResult(
                success=False,
                output="",
                error=str(e),
                exit_code=-1,
                execution_time=execution_time,
                command=command,
                safe=is_safe
            )
    
    def format_command_template(self, command_template: str, variables: Dict[str, str]) -> str:
        """FIXED: Format command template with provided variables and proper error handling"""
        try:
            # Handle both {variable} and {0}, {1} style formatting
            
            # First, try named formatting with variables dict
            try:
                return command_template.format(**variables)
            except KeyError as e:
                logger.warning(f"Missing variable in command template: {e}")
                # Continue to positional formatting attempt
            
            # If named formatting fails, try positional formatting
            # Extract values in order of common kubernetes variables
            common_vars = ["pod_name", "namespace", "deployment_name", "container_name", "node_name"]
            var_values = []
            
            for var in common_vars:
                if var in variables:
                    var_values.append(variables[var])
            
            # Try positional formatting if we have values
            if var_values:
                try:
                    return command_template.format(*var_values)
                except (IndexError, ValueError) as e:
                    logger.warning(f"Positional formatting failed: {e}")
            
            # If all formatting fails, do manual replacement for common patterns
            formatted_command = command_template
            
            # Replace common kubernetes placeholders
            replacements = {
                "{pod_name}": variables.get("pod_name", "UNKNOWN_POD"),
                "{namespace}": variables.get("namespace", "default"),
                "{deployment_name}": variables.get("deployment_name", "UNKNOWN_DEPLOYMENT"),
                "{container_name}": variables.get("container_name", "main"),
                "{node_name}": variables.get("node_name", "UNKNOWN_NODE")
            }
            
            for placeholder, value in replacements.items():
                formatted_command = formatted_command.replace(placeholder, value)
            
            # Handle numbered placeholders like {0}, {1}, etc.
            for i, value in enumerate(var_values):
                placeholder = "{" + str(i) + "}"
                formatted_command = formatted_command.replace(placeholder, value)
            
            return formatted_command
            
        except Exception as e:
            logger.error(f"Error formatting command template: {e}")
            logger.error(f"Template: {command_template}")
            logger.error(f"Variables: {variables}")
            
            # Return a safe fallback command
            pod_name = variables.get("pod_name", "UNKNOWN_POD")
            namespace = variables.get("namespace", "default")
            
            if "describe" in command_template.lower():
                return f"kubectl describe pod {pod_name} -n {namespace}"
            elif "get" in command_template.lower() and "pod" in command_template.lower():
                return f"kubectl get pod {pod_name} -n {namespace}"
            elif "logs" in command_template.lower():
                return f"kubectl logs {pod_name} -n {namespace}"
            else:
                # Generic fallback
                return f"kubectl get pod {pod_name} -n {namespace}"
    
    async def execute_diagnosis_commands(self, session_id: str, 
                                       commands: List[str]) -> Dict[str, CommandResult]:
        """Execute multiple diagnostic commands and return results"""
        results = {}
        
        for command in commands:
            try:
                result = await self.execute_command(session_id, command)
                results[command] = result
                
                # Small delay between commands to avoid overwhelming the cluster
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error executing diagnosis command {command}: {e}")
                results[command] = CommandResult(
                    success=False,
                    output="",
                    error=str(e),
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
        
        return results
    
    async def execute_fix_commands(self, session_id: str, commands: List[str],
                                 confirm_dangerous: bool = False) -> Dict[str, CommandResult]:
        """Execute fix commands with additional safety checks"""
        results = {}
        
        for command in commands:
            # Extra validation for fix commands
            is_safe, safety_level, reason = self.validate_command(command)
            
            if not is_safe:
                results[command] = CommandResult(
                    success=False,
                    output="",
                    error=f"Fix command blocked: {reason}",
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                continue
            
            # For moderate safety commands, require explicit confirmation
            if safety_level == "moderate" and not confirm_dangerous:
                results[command] = CommandResult(
                    success=False,
                    output="",
                    error="Moderate-risk command requires explicit confirmation",
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                continue
            
            # Execute the fix command
            try:
                result = await self.execute_command(session_id, command)
                results[command] = result
                
                # If a fix command fails, stop executing remaining commands
                if not result.success:
                    logger.warning(f"Fix command failed, stopping execution: {command}")
                    break
                
                # Delay between fix commands for safety
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error executing fix command {command}: {e}")
                results[command] = CommandResult(
                    success=False,
                    output="",
                    error=str(e),
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                break
        
        return results
    
    def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get information about an MCP session"""
        if session_id not in self.active_sessions:
            return None
        
        session = self.active_sessions[session_id]
        
        return {
            "session_id": session_id,
            "cluster_context": session.cluster_context,
            "namespace": session.namespace,
            "start_time": session.start_time.isoformat(),
            "commands_executed": len(session.commands_executed),
            "last_commands": session.commands_executed[-5:] if session.commands_executed else [],
            "duration_minutes": (datetime.utcnow() - session.start_time).total_seconds() / 60
        }
    
    def close_session(self, session_id: str) -> bool:
        """Close an MCP session"""
        if session_id in self.active_sessions:
            session = self.active_sessions.pop(session_id)
            logger.info(f"Closed MCP session: {session_id} (executed {len(session.commands_executed)} commands)")
            return True
        return False
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get basic cluster information"""
        try:
            # Get cluster info
            info_cmd = "kubectl cluster-info"
            process = subprocess.run(
                info_cmd.split(),
                capture_output=True,
                text=True,
                timeout=10
            )
            
            cluster_info = process.stdout if process.returncode == 0 else "Unable to get cluster info"
            
            # Get node count
            nodes_cmd = "kubectl get nodes --no-headers"
            process = subprocess.run(
                nodes_cmd.split(),
                capture_output=True,
                text=True,
                timeout=10
            )
            
            node_count = len(process.stdout.strip().split('\n')) if process.returncode == 0 and process.stdout.strip() else 0
            
            return {
                "cluster_name": self.config.aws.cluster_name,
                "region": self.config.aws.region,
                "cluster_info": cluster_info,
                "node_count": node_count,
                "allowed_namespaces": self.config.mcp.allowed_namespaces,
                "active_sessions": len(self.active_sessions)
            }
            
        except Exception as e:
            logger.error(f"Error getting cluster info: {e}")
            return {
                "cluster_name": self.config.aws.cluster_name,
                "region": self.config.aws.region,
                "error": str(e)
            }
    
    def cleanup_old_sessions(self, max_age_hours: int = 2) -> int:
        """Clean up old MCP sessions"""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        old_sessions = []
        
        for session_id, session in self.active_sessions.items():
            if session.start_time < cutoff_time:
                old_sessions.append(session_id)
        
        for session_id in old_sessions:
            self.close_session(session_id)
        
        if old_sessions:
            logger.info(f"Cleaned up {len(old_sessions)} old MCP sessions")
        
        return len(old_sessions)
    
    def get_safe_commands_for_error_type(self, error_type: str) -> List[str]:
        """Get recommended safe diagnosis commands for specific error types"""
        from .config import RESOLUTION_TEMPLATES
        
        template = RESOLUTION_TEMPLATES.get(error_type, {})
        diagnosis_commands = template.get("diagnosis_commands", [])
        
        # Filter to only safe commands
        safe_commands = []
        for cmd in diagnosis_commands:
            is_safe, safety_level, _ = self.validate_command(cmd)
            if is_safe and safety_level == "safe":
                safe_commands.append(cmd)
        
        return safe_commands