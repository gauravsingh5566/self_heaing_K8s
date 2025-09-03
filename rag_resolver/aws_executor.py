#!/usr/bin/env python3
"""
Enhanced AWS Command Executor that can execute both AWS CLI and kubectl commands
to perform intelligent resolution of Kubernetes errors
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
    command_type: str  # 'kubectl', 'aws', 'docker'

class EnhancedAWSExecutor:
    """Enhanced executor that can run AWS CLI, kubectl, and Docker commands intelligently"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.active_sessions = {}
        self._init_aws_context()
        self._init_kubectl_context()
    
    def _init_aws_context(self):
        """Initialize AWS CLI context"""
        try:
            # Test AWS CLI access
            test_cmd = [
                "aws", "sts", "get-caller-identity",
                "--profile", self.config.aws.profile
            ]
            
            result = subprocess.run(
                test_cmd,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                caller_info = json.loads(result.stdout)
                logger.info(f"AWS CLI configured for account: {caller_info.get('Account', 'unknown')}")
            else:
                logger.warning(f"AWS CLI test failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error initializing AWS context: {e}")
    
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
    
    async def intelligent_image_pull_resolution(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Intelligently resolve image pull errors by checking ECR and updating deployments"""
        pod_name = error_data.get('pod_name', '')
        namespace = error_data.get('namespace', 'default')
        deployment_name = pod_name.split('-')[0] if '-' in pod_name else pod_name
        
        resolution_steps = []
        
        try:
            # Step 1: Get current image from the deployment
            logger.info(f"🔍 Step 1: Getting current image from deployment {deployment_name}")
            current_image_result = await self._execute_command(
                f"kubectl get deployment {deployment_name} -n {namespace} -o jsonpath='{{.spec.template.spec.containers[0].image}}'"
            )
            
            if not current_image_result.success:
                resolution_steps.append({
                    "step": "Get current image",
                    "success": False,
                    "error": current_image_result.error
                })
                return {"success": False, "steps": resolution_steps}
            
            current_image = current_image_result.output.strip().strip("'\"")
            logger.info(f"Current image: {current_image}")
            resolution_steps.append({
                "step": "Get current image",
                "success": True,
                "result": current_image
            })
            
            # Step 2: Parse the image to extract registry and repository
            image_parts = self._parse_image_url(current_image)
            if not image_parts:
                logger.error("Could not parse image URL")
                return {"success": False, "steps": resolution_steps, "error": "Could not parse image URL"}
            
            registry = image_parts['registry']
            repository = image_parts['repository'] 
            current_tag = image_parts['tag']
            
            logger.info(f"Image parts - Registry: {registry}, Repository: {repository}, Tag: {current_tag}")
            
            # Step 3: Check if this is an ECR image and get available tags
            if 'ecr' in registry.lower():
                logger.info("🏭 Step 3: Checking ECR for available tags...")
                available_tags = await self._get_ecr_tags(repository, is_public='public' in registry)
                
                if available_tags:
                    logger.info(f"Available ECR tags: {available_tags[:5]}...")  # Show first 5
                    latest_tag = available_tags[0]  # Most recent tag
                    
                    resolution_steps.append({
                        "step": "Check ECR tags",
                        "success": True,
                        "available_tags": available_tags[:10],
                        "latest_tag": latest_tag
                    })
                    
                    # Step 4: If current tag doesn't exist, use the latest one
                    if current_tag not in available_tags:
                        logger.info(f"🔄 Step 4: Current tag '{current_tag}' not found, updating to '{latest_tag}'")
                        new_image = f"{registry}/{repository}:{latest_tag}"
                        
                        # Update the deployment
                        update_result = await self._update_deployment_image(
                            deployment_name, namespace, new_image
                        )
                        
                        if update_result.success:
                            resolution_steps.append({
                                "step": "Update deployment image",
                                "success": True,
                                "old_image": current_image,
                                "new_image": new_image
                            })
                            
                            # Step 5: Delete the failing pod to trigger restart
                            logger.info(f"🗑️ Step 5: Deleting failing pod {pod_name}")
                            delete_result = await self._execute_command(
                                f"kubectl delete pod {pod_name} -n {namespace}"
                            )
                            
                            resolution_steps.append({
                                "step": "Delete failing pod",
                                "success": delete_result.success,
                                "result": delete_result.output if delete_result.success else delete_result.error
                            })
                            
                            # Step 6: Wait and check if new pod is running
                            logger.info("⏳ Step 6: Waiting for new pod to start...")
                            await asyncio.sleep(10)  # Wait 10 seconds
                            
                            new_pod_status = await self._check_deployment_status(deployment_name, namespace)
                            resolution_steps.append({
                                "step": "Check new pod status",
                                "success": new_pod_status['healthy'],
                                "result": new_pod_status
                            })
                            
                            return {
                                "success": new_pod_status['healthy'],
                                "steps": resolution_steps,
                                "resolution_summary": f"Updated image from {current_image} to {new_image}",
                                "new_image": new_image
                            }
                        else:
                            resolution_steps.append({
                                "step": "Update deployment image",
                                "success": False,
                                "error": update_result.error
                            })
                    else:
                        # Tag exists but still failing - might be permissions issue
                        logger.info("🔐 Step 4: Tag exists, checking ECR permissions...")
                        permissions_result = await self._check_ecr_permissions(repository, is_public='public' in registry)
                        
                        resolution_steps.append({
                            "step": "Check ECR permissions",
                            "success": permissions_result['has_access'],
                            "result": permissions_result
                        })
                        
                        if not permissions_result['has_access']:
                            # Fix permissions
                            fix_result = await self._fix_ecr_permissions(is_public='public' in registry)
                            resolution_steps.append({
                                "step": "Fix ECR permissions",
                                "success": fix_result['success'],
                                "result": fix_result
                            })
                else:
                    resolution_steps.append({
                        "step": "Check ECR tags",
                        "success": False,
                        "error": "No tags found in ECR repository"
                    })
            else:
                # Non-ECR registry (Docker Hub, etc.)
                logger.info("🐳 Step 3: Checking Docker Hub or other registry...")
                
                # For Docker Hub, try to use 'latest' tag if current tag fails
                if current_tag != 'latest':
                    logger.info("🔄 Trying 'latest' tag...")
                    new_image = f"{registry}/{repository}:latest"
                    
                    update_result = await self._update_deployment_image(
                        deployment_name, namespace, new_image
                    )
                    
                    resolution_steps.append({
                        "step": "Update to latest tag",
                        "success": update_result.success,
                        "old_image": current_image,
                        "new_image": new_image
                    })
            
            return {"success": True, "steps": resolution_steps}
            
        except Exception as e:
            logger.error(f"Error in intelligent image pull resolution: {e}")
            resolution_steps.append({
                "step": "Resolution failed",
                "success": False,
                "error": str(e)
            })
            return {"success": False, "steps": resolution_steps, "error": str(e)}
    
    async def _get_ecr_tags(self, repository: str, is_public: bool = False) -> List[str]:
        """Get available tags from ECR repository"""
        try:
            if is_public:
                # ECR Public
                cmd = f"aws ecr-public describe-images --repository-name {repository} --region us-east-1 --profile {self.config.aws.profile}"
            else:
                # ECR Private
                cmd = f"aws ecr describe-images --repository-name {repository} --region {self.config.aws.region} --profile {self.config.aws.profile}"
            
            result = await self._execute_command(cmd)
            
            if result.success:
                images_data = json.loads(result.output)
                tags = []
                
                for image in images_data.get('imageDetails', []):
                    image_tags = image.get('imageTags', [])
                    if image_tags:
                        tags.extend(image_tags)
                        
                # Sort tags by push date (most recent first)
                # For simplicity, sort alphabetically for now
                tags.sort(reverse=True)
                return tags
            
        except Exception as e:
            logger.error(f"Error getting ECR tags: {e}")
        
        return []
    
    async def _update_deployment_image(self, deployment_name: str, namespace: str, new_image: str) -> CommandResult:
        """Update deployment image using kubectl"""
        cmd = f"kubectl set image deployment/{deployment_name} {deployment_name}={new_image} -n {namespace}"
        return await self._execute_command(cmd)
    
    async def _check_deployment_status(self, deployment_name: str, namespace: str) -> Dict[str, Any]:
        """Check if deployment is healthy"""
        try:
            # Get deployment status
            cmd = f"kubectl get deployment {deployment_name} -n {namespace} -o json"
            result = await self._execute_command(cmd)
            
            if result.success:
                deployment_data = json.loads(result.output)
                status = deployment_data.get('status', {})
                
                ready_replicas = status.get('readyReplicas', 0)
                desired_replicas = status.get('replicas', 1)
                
                return {
                    "healthy": ready_replicas >= desired_replicas,
                    "ready_replicas": ready_replicas,
                    "desired_replicas": desired_replicas,
                    "status": status
                }
        except Exception as e:
            logger.error(f"Error checking deployment status: {e}")
        
        return {"healthy": False, "error": "Could not check status"}
    
    async def _check_ecr_permissions(self, repository: str, is_public: bool = False) -> Dict[str, Any]:
        """Check if we have ECR access permissions"""
        try:
            if is_public:
                cmd = f"aws ecr-public get-authorization-token --region us-east-1 --profile {self.config.aws.profile}"
            else:
                cmd = f"aws ecr get-authorization-token --region {self.config.aws.region} --profile {self.config.aws.profile}"
            
            result = await self._execute_command(cmd)
            
            return {
                "has_access": result.success,
                "error": result.error if not result.success else None
            }
            
        except Exception as e:
            return {"has_access": False, "error": str(e)}
    
    async def _fix_ecr_permissions(self, is_public: bool = False) -> Dict[str, Any]:
        """Attempt to fix ECR permissions by updating IAM policies"""
        try:
            # Get EKS worker node role
            cluster_info_cmd = f"aws eks describe-cluster --name {self.config.aws.cluster_name} --region {self.config.aws.region} --profile {self.config.aws.profile}"
            cluster_result = await self._execute_command(cluster_info_cmd)
            
            if not cluster_result.success:
                return {"success": False, "error": "Could not get cluster info"}
            
            cluster_data = json.loads(cluster_result.output)
            
            # Get node groups to find worker role
            nodegroups_cmd = f"aws eks list-nodegroups --cluster-name {self.config.aws.cluster_name} --region {self.config.aws.region} --profile {self.config.aws.profile}"
            nodegroups_result = await self._execute_command(nodegroups_cmd)
            
            if nodegroups_result.success:
                nodegroups_data = json.loads(nodegroups_result.output)
                
                for nodegroup_name in nodegroups_data.get('nodegroups', []):
                    # Get nodegroup details
                    ng_cmd = f"aws eks describe-nodegroup --cluster-name {self.config.aws.cluster_name} --nodegroup-name {nodegroup_name} --region {self.config.aws.region} --profile {self.config.aws.profile}"
                    ng_result = await self._execute_command(ng_cmd)
                    
                    if ng_result.success:
                        ng_data = json.loads(ng_result.output)
                        node_role = ng_data['nodegroup']['nodeRole']
                        role_name = node_role.split('/')[-1]
                        
                        # Check if ECR policy is attached
                        policies_cmd = f"aws iam list-attached-role-policies --role-name {role_name} --profile {self.config.aws.profile}"
                        policies_result = await self._execute_command(policies_cmd)
                        
                        if policies_result.success:
                            policies_data = json.loads(policies_result.output)
                            
                            # Check if ECR policy exists
                            ecr_policy_attached = any(
                                'ECR' in policy['PolicyName'] or 'ContainerRegistry' in policy['PolicyName']
                                for policy in policies_data.get('AttachedPolicies', [])
                            )
                            
                            if not ecr_policy_attached:
                                # Attach ECR policy
                                if is_public:
                                    policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
                                else:
                                    policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
                                
                                attach_cmd = f"aws iam attach-role-policy --role-name {role_name} --policy-arn {policy_arn} --profile {self.config.aws.profile}"
                                attach_result = await self._execute_command(attach_cmd)
                                
                                return {
                                    "success": attach_result.success,
                                    "action": f"Attached ECR policy to role {role_name}",
                                    "error": attach_result.error if not attach_result.success else None
                                }
            
            return {"success": False, "error": "Could not determine worker node role"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _parse_image_url(self, image_url: str) -> Optional[Dict[str, str]]:
        """Parse container image URL into components"""
        try:
            # Handle different formats:
            # docker.io/library/nginx:latest
            # public.ecr.aws/sample/node:v1
            # 123456789012.dkr.ecr.us-west-2.amazonaws.com/my-repo:latest
            
            if '/' not in image_url:
                # Just image name like "nginx:latest"
                parts = image_url.split(':')
                return {
                    'registry': 'docker.io',
                    'repository': f"library/{parts[0]}",
                    'tag': parts[1] if len(parts) > 1 else 'latest'
                }
            
            # Split on first slash to separate registry from repository/tag
            if image_url.count('/') == 1 and '.' not in image_url.split('/')[0]:
                # Format like "username/repo:tag" (Docker Hub)
                registry = 'docker.io'
                repo_tag = image_url
            else:
                # Format like "registry.com/repo:tag"
                parts = image_url.split('/', 1)
                registry = parts[0]
                repo_tag = parts[1]
            
            # Split repository and tag
            if ':' in repo_tag:
                repository, tag = repo_tag.rsplit(':', 1)
            else:
                repository = repo_tag
                tag = 'latest'
            
            return {
                'registry': registry,
                'repository': repository,
                'tag': tag
            }
            
        except Exception as e:
            logger.error(f"Error parsing image URL {image_url}: {e}")
            return None
    
    async def _execute_command(self, command: str, timeout: int = 30) -> CommandResult:
        """Execute any command (AWS, kubectl, etc.) with proper error handling"""
        start_time = datetime.utcnow()
        
        # Determine command type
        cmd_type = "unknown"
        if command.strip().startswith("kubectl"):
            cmd_type = "kubectl"
        elif command.strip().startswith("aws"):
            cmd_type = "aws"
        elif command.strip().startswith("docker"):
            cmd_type = "docker"
        
        try:
            # Execute command
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
            
            logger.info(f"Executed {cmd_type} command: {command[:100]}...")
            
            return CommandResult(
                success=process.returncode == 0,
                output=stdout_str,
                error=stderr_str,
                exit_code=process.returncode,
                execution_time=execution_time,
                command=command,
                safe=True,  # We control what gets executed
                command_type=cmd_type
            )
            
        except asyncio.TimeoutError:
            logger.error(f"Command timeout: {command}")
            return CommandResult(
                success=False,
                output="",
                error=f"Command timed out after {timeout} seconds",
                exit_code=-1,
                execution_time=timeout,
                command=command,
                safe=True,
                command_type=cmd_type
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
                safe=True,
                command_type=cmd_type
            )
    
    async def intelligent_resource_limit_resolution(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Intelligently resolve resource limit errors"""
        pod_name = error_data.get('pod_name', '')
        namespace = error_data.get('namespace', 'default')
        deployment_name = pod_name.split('-')[0] if '-' in pod_name else pod_name
        
        resolution_steps = []
        
        try:
            # Step 1: Check current resource limits
            logger.info("🔍 Step 1: Checking current resource limits...")
            limits_cmd = f"kubectl get deployment {deployment_name} -n {namespace} -o jsonpath='{{.spec.template.spec.containers[0].resources}}'"
            limits_result = await self._execute_command(limits_cmd)
            
            if limits_result.success:
                try:
                    current_resources = json.loads(limits_result.output)
                except:
                    current_resources = {}
                
                resolution_steps.append({
                    "step": "Check current resources",
                    "success": True,
                    "current_resources": current_resources
                })
                
                # Step 2: Increase resource limits
                logger.info("⬆️ Step 2: Increasing resource limits...")
                new_limits = {
                    "limits": {
                        "memory": "1Gi",
                        "cpu": "500m"
                    },
                    "requests": {
                        "memory": "512Mi", 
                        "cpu": "250m"
                    }
                }
                
                # Apply new resource limits
                patch_cmd = f"""kubectl patch deployment {deployment_name} -n {namespace} -p '{{"spec":{{"template":{{"spec":{{"containers":[{{"name":"{deployment_name}","resources":{json.dumps(new_limits)}}}]}}}}}}}}'"""
                patch_result = await self._execute_command(patch_cmd)
                
                resolution_steps.append({
                    "step": "Update resource limits",
                    "success": patch_result.success,
                    "new_limits": new_limits,
                    "error": patch_result.error if not patch_result.success else None
                })
                
                if patch_result.success:
                    # Step 3: Wait for rollout
                    logger.info("⏳ Step 3: Waiting for deployment rollout...")
                    rollout_cmd = f"kubectl rollout status deployment/{deployment_name} -n {namespace} --timeout=60s"
                    rollout_result = await self._execute_command(rollout_cmd)
                    
                    resolution_steps.append({
                        "step": "Wait for rollout",
                        "success": rollout_result.success,
                        "result": rollout_result.output
                    })
                    
                    return {
                        "success": rollout_result.success,
                        "steps": resolution_steps,
                        "resolution_summary": f"Increased resource limits for {deployment_name}"
                    }
            
        except Exception as e:
            resolution_steps.append({
                "step": "Resource resolution failed",
                "success": False,
                "error": str(e)
            })
        
        return {"success": False, "steps": resolution_steps}
    
    async def intelligent_network_resolution(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """Intelligently resolve network errors"""
        pod_name = error_data.get('pod_name', '')
        namespace = error_data.get('namespace', 'default')
        
        resolution_steps = []
        
        try:
            # Step 1: Check service discovery
            logger.info("🌐 Step 1: Checking service discovery...")
            services_cmd = f"kubectl get svc -n {namespace}"
            services_result = await self._execute_command(services_cmd)
            
            resolution_steps.append({
                "step": "Check services",
                "success": services_result.success,
                "services": services_result.output if services_result.success else services_result.error
            })
            
            # Step 2: Check network policies
            logger.info("🔒 Step 2: Checking network policies...")
            netpol_cmd = f"kubectl get networkpolicies -n {namespace}"
            netpol_result = await self._execute_command(netpol_cmd)
            
            resolution_steps.append({
                "step": "Check network policies",
                "success": netpol_result.success,
                "policies": netpol_result.output
            })
            
            # Step 3: Restart pod to reset network
            logger.info("🔄 Step 3: Restarting pod to reset network...")
            delete_cmd = f"kubectl delete pod {pod_name} -n {namespace}"
            delete_result = await self._execute_command(delete_cmd)
            
            resolution_steps.append({
                "step": "Restart pod",
                "success": delete_result.success,
                "result": delete_result.output
            })
            
            return {
                "success": delete_result.success,
                "steps": resolution_steps,
                "resolution_summary": "Restarted pod to reset network connectivity"
            }
            
        except Exception as e:
            resolution_steps.append({
                "step": "Network resolution failed",
                "success": False,
                "error": str(e)
            })
        
        return {"success": False, "steps": resolution_steps}