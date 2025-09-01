#!/usr/bin/env python3
"""
MCP (Model Context Protocol) executor for running kubectl commands
in EKS cluster with safety controls and validation
"""

import asyncio
import json
import logging
import subprocess
import shlex
import re
from datetime import datetime
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
        """
        Validate kubectl command for safety and security
        
        Returns:
            (is_safe, safety_level, reason)
        """
        # Parse command
        try:
            parts = shlex.split(command)
        except ValueError as e:
            return False, "invalid", f"Invalid command syntax: {e}"
        
        if not parts or parts[0] != "kubectl":
            return False, "invalid", "Only kubectl commands are allowed"
        
        if len(parts) < 2:
            return False, "invalid", "Incomplete kubectl command"
        
        action = parts[1].lower()
        
        # Check against dangerous commands
        if action in DANGEROUS_KUBECTL_COMMANDS:
            return False, "dangerous", f"Command '{action}' is not allowed for safety reasons"
        
        # Check against safe commands
        if action in SAFE_KUBECTL_COMMANDS:
            safety_level = "safe"
        else:
            safety_level = "moderate"
        
        # Validate namespace if specified
        if namespace and namespace not in self.config.mcp.allowed_namespaces:
            return False, "restricted", f"Namespace '{namespace}' is not in allowed list"
        
        # Additional validation for specific commands
        if action == "delete":
            # Never allow delete commands
            return False, "dangerous", "Delete operations are not permitted"
        
        if action in ["apply", "create", "patch"]:
            # Check for dangerous resource modifications
            if any(dangerous in command.lower() for dangerous in ["--force", "--grace-period=0"]):
                return False, "dangerous", "Forced operations are not permitted"
        
        return True, safety_level,