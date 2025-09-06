#!/usr/bin/env python3
"""
MCP (Model Context Protocol) Executor for RAG Resolver
Handles safe execution of kubectl commands with detailed logging of commands and outputs
"""

import asyncio
import logging
import os
import re
import json
import weakref
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

# Setup specialized loggers for command tracking
command_logger = logging.getLogger('mcp_commands')
output_logger = logging.getLogger('mcp_outputs')

class CommandSafetyLevel(Enum):
    SAFE = "safe"
    MODERATE = "moderate"
    DANGEROUS = "dangerous"

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
    command_type: str = "kubectl"

@dataclass
class MCPSession:
    """MCP session for managing command execution context"""
    session_id: str
    namespace: str
    created_at: datetime
    last_used: datetime
    commands_executed: List[str] = field(default_factory=list)
    is_active: bool = True

class ProcessManager:
    """Manager for tracking and cleaning up subprocess resources"""
    
    def __init__(self):
        self.active_processes: Set[asyncio.subprocess.Process] = set()
        self._cleanup_registered = False
    
    def register_process(self, process: asyncio.subprocess.Process):
        """Register a process for cleanup tracking"""
        self.active_processes.add(process)
        if not self._cleanup_registered:
            weakref.finalize(self, self._cleanup_all_processes, self.active_processes.copy())
            self._cleanup_registered = True
    
    def unregister_process(self, process: asyncio.subprocess.Process):
        """Unregister a process after it's done"""
        self.active_processes.discard(process)
    
    async def cleanup_process(self, process: asyncio.subprocess.Process):
        """Safely cleanup a single process"""
        try:
            if process.returncode is None:
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    try:
                        process.kill()
                        await asyncio.wait_for(process.wait(), timeout=2.0)
                    except:
                        pass
        except Exception as e:
            command_logger.debug(f"Error cleaning up process: {e}")
        finally:
            self.unregister_process(process)
    
    async def cleanup_all_processes(self):
        """Cleanup all tracked processes"""
        if not self.active_processes:
            return
        
        cleanup_tasks = []
        for process in self.active_processes.copy():
            cleanup_tasks.append(self.cleanup_process(process))
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
    
    @staticmethod
    def _cleanup_all_processes(processes_copy: Set):
        """Static cleanup method for finalize"""
        pass

class CommandLogger:
    """Specialized logger for command execution tracking"""
    
    def __init__(self, log_to_console: bool = True, log_to_file: bool = True):
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self._setup_loggers()
    
    def _setup_loggers(self):
        """Setup specialized loggers for commands and outputs"""
        # Create logs directory
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # Command logger - logs which commands are executed
        command_logger.setLevel(logging.INFO)
        
        # Output logger - logs command outputs
        output_logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for logger in [command_logger, output_logger]:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
        
        # File handlers
        if self.log_to_file:
            # Commands log file
            command_file_handler = logging.FileHandler('logs/mcp_commands.log')
            command_file_handler.setLevel(logging.INFO)
            command_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            command_file_handler.setFormatter(command_formatter)
            command_logger.addHandler(command_file_handler)
            
            # Outputs log file  
            output_file_handler = logging.FileHandler('logs/mcp_command_outputs.log')
            output_file_handler.setLevel(logging.INFO)
            output_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            output_file_handler.setFormatter(output_formatter)
            output_logger.addHandler(output_file_handler)
        
        # Console handlers
        if self.log_to_console:
            # Commands console handler
            command_console_handler = logging.StreamHandler()
            command_console_handler.setLevel(logging.INFO)
            command_console_formatter = logging.Formatter('🔧 COMMAND: %(message)s')
            command_console_handler.setFormatter(command_console_formatter)
            command_logger.addHandler(command_console_handler)
            
            # Outputs console handler
            output_console_handler = logging.StreamHandler()
            output_console_handler.setLevel(logging.INFO)
            output_console_formatter = logging.Formatter('📝 OUTPUT: %(message)s')
            output_console_handler.setFormatter(output_console_formatter)
            output_logger.addHandler(output_console_handler)
        
        # Prevent propagation to root logger
        command_logger.propagate = False
        output_logger.propagate = False
    
    def log_command_start(self, session_id: str, command: str, namespace: str):
        """Log when a command starts executing"""
        msg = f"[{session_id[:8]}] [{namespace}] EXECUTING: {command}"
        command_logger.info(msg)
        if self.log_to_console:
            print(f"🔧 EXECUTING COMMAND: {command}")
    
    def log_command_result(self, session_id: str, command: str, result: CommandResult):
        """Log the complete result of a command execution"""
        # Log command completion
        status = "✅ SUCCESS" if result.success else "❌ FAILED"
        summary = f"[{session_id[:8]}] {status} ({result.execution_time:.2f}s) exit_code={result.exit_code}: {command}"
        command_logger.info(summary)
        
        # Log outputs if they exist
        if result.output.strip():
            output_msg = f"[{session_id[:8]}] STDOUT:\n{result.output}"
            output_logger.info(output_msg)
            if self.log_to_console:
                print(f"📝 STDOUT:\n{result.output}")
        
        if result.error.strip():
            error_msg = f"[{session_id[:8]}] STDERR:\n{result.error}"
            output_logger.warning(error_msg)
            if self.log_to_console:
                print(f"⚠️ STDERR:\n{result.error}")
        
        # Console summary
        if self.log_to_console:
            print(f"{status} Command completed in {result.execution_time:.2f}s with exit code {result.exit_code}")
            print("-" * 80)
    
    def log_command_error(self, session_id: str, command: str, error: str):
        """Log command execution errors"""
        msg = f"[{session_id[:8]}] ERROR executing command: {command} - {error}"
        command_logger.error(msg)
        if self.log_to_console:
            print(f"❌ COMMAND ERROR: {error}")

class MCPExecutor:
    """MCP Executor that safely executes kubectl commands with detailed logging"""
    
    def __init__(self, config, log_to_console: bool = True, log_to_file: bool = True):
        self.config = config
        self.active_sessions: Dict[str, MCPSession] = {}
        self.command_history: List[CommandResult] = []
        self.process_manager = ProcessManager()
        self.command_logger = CommandLogger(log_to_console, log_to_file)
        
        # Safe kubectl commands that are read-only
        self.safe_commands = {
            "get", "describe", "logs", "top", "explain", "version", "cluster-info",
            "config", "api-resources", "api-versions", "whoami", "auth"
        }
        
        # Moderate risk commands that modify but are generally safe
        self.moderate_commands = {
            "apply", "create", "patch", "edit", "label", "annotate", "scale",
            "rollout", "expose", "port-forward", "exec", "cp"
        }
        
        # Dangerous commands that should be blocked or require confirmation
        self.dangerous_commands = {
            "delete", "replace", "drain", "cordon", "uncordon", "taint", "untaint"
        }
        
        # Blocked patterns for additional safety
        self.blocked_patterns = [
            r"rm\s+-rf",
            r"sudo",
            r"chmod\s+777",
            r"--force.*--grace-period=0",
            r"nuclear",
            r"destroy"
        ]
        
        print("🔧 MCP Executor initialized with command logging enabled")
        print(f"📁 Commands logged to: logs/mcp_commands.log")
        print(f"📁 Outputs logged to: logs/mcp_command_outputs.log")
    
    def create_session(self, namespace: str = "default") -> str:
        """Create a new MCP session for command execution"""
        import uuid
        session_id = str(uuid.uuid4())
        
        session = MCPSession(
            session_id=session_id,
            namespace=namespace,
            created_at=datetime.now(timezone.utc),
            last_used=datetime.now(timezone.utc)
        )
        
        self.active_sessions[session_id] = session
        print(f"🆕 Created MCP session {session_id[:8]} for namespace '{namespace}'")
        return session_id
    
    def close_session(self, session_id: str) -> bool:
        """Close and cleanup an MCP session"""
        if session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            session.is_active = False
            del self.active_sessions[session_id]
            print(f"🔒 Closed MCP session {session_id[:8]}")
            return True
        return False
    
    def validate_command(self, command: str, namespace: str = None) -> Tuple[bool, str, str]:
        """Validate if a command is safe to execute"""
        command_lower = command.lower().strip()
        
        # Check for blocked patterns
        for pattern in self.blocked_patterns:
            if re.search(pattern, command_lower):
                return False, "dangerous", f"Command contains blocked pattern: {pattern}"
        
        # Must be a kubectl command
        if not command_lower.startswith("kubectl"):
            return False, "dangerous", "Only kubectl commands are allowed"
        
        # Extract the action from kubectl command
        parts = command_lower.split()
        if len(parts) < 2:
            return False, "dangerous", "Invalid kubectl command format"
        
        action = parts[1]
        
        # Check safety level
        if action in self.safe_commands:
            safety_level = "safe"
        elif action in self.moderate_commands:
            safety_level = "moderate"
        elif action in self.dangerous_commands:
            safety_level = "dangerous"
        else:
            safety_level = "moderate"
        
        # Additional checks for dangerous operations
        if safety_level == "dangerous":
            return False, "dangerous", f"Command action '{action}' is not permitted"
        
        # Check for forced operations
        if action in ["apply", "create", "patch"]:
            if any(dangerous in command_lower for dangerous in ["--force", "--grace-period=0"]):
                return False, "dangerous", "Forced operations are not permitted"
        
        # Check for system namespace operations
        if namespace and namespace.startswith("kube-") and action in self.moderate_commands:
            return False, "dangerous", "Modifications to system namespaces not allowed"
        
        return True, safety_level, "Command validated successfully"
    
    async def execute_command(self, session_id: str, command: str, 
                            timeout: int = None) -> CommandResult:
        """Execute a kubectl command with detailed logging of command and output"""
        timeout = timeout or getattr(self.config, 'timeout_seconds', 300)
        
        if session_id not in self.active_sessions:
            error_result = CommandResult(
                success=False,
                output="",
                error="Invalid session ID",
                exit_code=-1,
                execution_time=0.0,
                command=command,
                safe=False
            )
            self.command_logger.log_command_error(session_id, command, "Invalid session ID")
            return error_result
        
        session = self.active_sessions[session_id]
        session.last_used = datetime.now(timezone.utc)
        
        # Validate command
        is_safe, safety_level, reason = self.validate_command(command, session.namespace)
        
        if not is_safe:
            error_result = CommandResult(
                success=False,
                output="",
                error=f"Command blocked: {reason}",
                exit_code=-1,
                execution_time=0.0,
                command=command,
                safe=False
            )
            self.command_logger.log_command_error(session_id, command, f"BLOCKED: {reason}")
            return error_result
        
        # Add namespace context if not specified
        if "-n " not in command and "--namespace" not in command and session.namespace != "default":
            command += f" -n {session.namespace}"
        
        # Log command start
        self.command_logger.log_command_start(session_id, command, session.namespace)
        
        start_time = datetime.now(timezone.utc)
        process = None
        
        try:
            # Create subprocess
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL
            )
            
            # Register process for cleanup
            self.process_manager.register_process(process)
            
            # Wait for command completion with timeout
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                await self.process_manager.cleanup_process(process)
                raise
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Decode output
            stdout_str = stdout.decode('utf-8') if stdout else ""
            stderr_str = stderr.decode('utf-8') if stderr else ""
            
            # Update session
            session.commands_executed.append(command)
            
            # Create result
            result = CommandResult(
                success=process.returncode == 0,
                output=stdout_str,
                error=stderr_str,
                exit_code=process.returncode,
                execution_time=execution_time,
                command=command,
                safe=safety_level in ["safe", "moderate"]
            )
            
            # Log the complete result
            self.command_logger.log_command_result(session_id, command, result)
            
            # Store in history
            self.command_history.append(result)
            
            return result
            
        except asyncio.TimeoutError:
            error_result = CommandResult(
                success=False,
                output="",
                error=f"Command timed out after {timeout} seconds",
                exit_code=-1,
                execution_time=timeout,
                command=command,
                safe=is_safe
            )
            self.command_logger.log_command_error(session_id, command, f"TIMEOUT after {timeout}s")
            return error_result
            
        except Exception as e:
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            error_result = CommandResult(
                success=False,
                output="",
                error=str(e),
                exit_code=-1,
                execution_time=execution_time,
                command=command,
                safe=is_safe
            )
            self.command_logger.log_command_error(session_id, command, str(e))
            return error_result
        
        finally:
            # Always cleanup process
            if process:
                try:
                    await self.process_manager.cleanup_process(process)
                except Exception as e:
                    command_logger.debug(f"Error in process cleanup: {e}")
    
    async def execute_diagnosis_commands(self, session_id: str, 
                                       commands: List[str]) -> Dict[str, CommandResult]:
        """Execute multiple diagnostic commands with detailed logging"""
        results = {}
        
        print(f"🔍 Starting diagnosis with {len(commands)} commands...")
        
        for i, command in enumerate(commands, 1):
            print(f"📋 Executing diagnostic command {i}/{len(commands)}")
            try:
                result = await self.execute_command(session_id, command)
                results[command] = result
                
                # Small delay between commands
                await asyncio.sleep(1)
                
            except Exception as e:
                error_result = CommandResult(
                    success=False,
                    output="",
                    error=str(e),
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                results[command] = error_result
                self.command_logger.log_command_error(session_id, command, str(e))
        
        successful_commands = sum(1 for r in results.values() if r.success)
        print(f"✅ Diagnosis completed: {successful_commands}/{len(commands)} commands successful")
        
        return results
    
    async def execute_fix_commands(self, session_id: str, commands: List[str],
                                 confirm_dangerous: bool = False) -> Dict[str, CommandResult]:
        """Execute fix commands with detailed logging"""
        results = {}
        
        print(f"🔧 Starting fix execution with {len(commands)} commands...")
        
        for i, command in enumerate(commands, 1):
            print(f"🛠️ Executing fix command {i}/{len(commands)}")
            
            # Extra validation for fix commands
            is_safe, safety_level, reason = self.validate_command(command)
            
            if not is_safe:
                error_result = CommandResult(
                    success=False,
                    output="",
                    error=f"Fix command blocked: {reason}",
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                results[command] = error_result
                print(f"❌ Fix command {i} blocked: {reason}")
                continue
            
            # For moderate safety commands, require explicit confirmation
            if safety_level == "moderate" and not confirm_dangerous:
                error_result = CommandResult(
                    success=False,
                    output="",
                    error="Moderate-risk command requires explicit confirmation",
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                results[command] = error_result
                print(f"⚠️ Fix command {i} requires confirmation (moderate risk)")
                continue
            
            # Execute the fix command
            try:
                result = await self.execute_command(session_id, command)
                results[command] = result
                
                # If a fix command fails, stop executing remaining commands
                if not result.success:
                    print(f"❌ Fix command {i} failed, stopping execution")
                    break
                
                # Delay between fix commands for safety
                await asyncio.sleep(2)
                
            except Exception as e:
                error_result = CommandResult(
                    success=False,
                    output="",
                    error=str(e),
                    exit_code=-1,
                    execution_time=0.0,
                    command=command,
                    safe=False
                )
                results[command] = error_result
                print(f"❌ Fix command {i} failed with exception: {e}")
                break
        
        successful_fixes = sum(1 for r in results.values() if r.success)
        print(f"✅ Fix execution completed: {successful_fixes}/{len(commands)} commands successful")
        
        return results
    
    def format_command_template(self, command_template: str, variables: Dict[str, str]) -> str:
        """Format command template with provided variables"""
        try:
            formatted_command = command_template
            
            # Replace named placeholders
            for var_name, var_value in variables.items():
                placeholder = "{" + var_name + "}"
                formatted_command = formatted_command.replace(placeholder, var_value)
            
            # Replace numbered placeholders
            var_values = list(variables.values())
            for i, value in enumerate(var_values):
                placeholder = "{" + str(i) + "}"
                formatted_command = formatted_command.replace(placeholder, value)
            
            return formatted_command
            
        except Exception as e:
            command_logger.error(f"Error formatting command template: {e}")
            
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
                return f"kubectl get pod {pod_name} -n {namespace}"
    
    def get_session_stats(self, session_id: str) -> Dict[str, any]:
        """Get statistics for a session"""
        if session_id not in self.active_sessions:
            return {"error": "Session not found"}
        
        session = self.active_sessions[session_id]
        return {
            "session_id": session_id,
            "namespace": session.namespace,
            "created_at": session.created_at.isoformat(),
            "last_used": session.last_used.isoformat(),
            "commands_executed": len(session.commands_executed),
            "is_active": session.is_active
        }
    
    def get_command_history(self, limit: int = 100) -> List[Dict[str, any]]:
        """Get recent command execution history"""
        recent_commands = self.command_history[-limit:]
        return [
            {
                "command": cmd.command,
                "success": cmd.success,
                "execution_time": cmd.execution_time,
                "exit_code": cmd.exit_code,
                "safe": cmd.safe,
                "output_length": len(cmd.output),
                "error_length": len(cmd.error)
            }
            for cmd in recent_commands
        ]
    
    def cleanup_inactive_sessions(self, max_age_hours: int = 24):
        """Cleanup sessions that haven't been used recently"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        inactive_sessions = [
            session_id for session_id, session in self.active_sessions.items()
            if session.last_used < cutoff_time
        ]
        
        for session_id in inactive_sessions:
            self.close_session(session_id)
        
        print(f"🧹 Cleaned up {len(inactive_sessions)} inactive sessions")
        return len(inactive_sessions)
    
    def get_executor_stats(self) -> Dict[str, any]:
        """Get overall executor statistics"""
        return {
            "active_sessions": len(self.active_sessions),
            "total_commands_executed": len(self.command_history),
            "successful_commands": sum(1 for cmd in self.command_history if cmd.success),
            "failed_commands": sum(1 for cmd in self.command_history if not cmd.success),
            "average_execution_time": sum(cmd.execution_time for cmd in self.command_history) / len(self.command_history) if self.command_history else 0
        }
    
    async def close(self):
        """Close all sessions and cleanup all processes"""
        try:
            session_ids = list(self.active_sessions.keys())
            for session_id in session_ids:
                self.close_session(session_id)
            
            await self.process_manager.cleanup_all_processes()
            
            print(f"🔒 MCP executor closed, cleaned up {len(session_ids)} sessions and all processes")
        
        except Exception as e:
            command_logger.error(f"Error during MCP executor cleanup: {e}")