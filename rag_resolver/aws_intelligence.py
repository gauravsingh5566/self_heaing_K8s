#!/usr/bin/env python3
"""
AWS Intelligence module for intelligent investigation of AWS services
related to EKS errors. Provides deep insights before command execution.
"""

import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .config import ResolverConfig

logger = logging.getLogger(__name__)

class AWSIntelligence:
    """Intelligent AWS service investigator for EKS-related issues"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.session = boto3.Session(
            profile_name=config.aws.profile,
            region_name=config.aws.region
        )
        
        # Initialize AWS clients
        self._init_clients()
        
        # Cache for performance
        self._cache = {
            'cluster_info': None,
            'node_groups': None,
            'cache_timestamp': None
        }
        self._cache_ttl = 300  # 5 minutes
    
    def _init_clients(self):
        """Initialize AWS service clients"""
        try:
            self.eks_client = self.session.client('eks')
            self.ec2_client = self.session.client('ec2')
            self.ecr_client = self.session.client('ecr')
            self.ecr_public_client = self.session.client('ecr-public', region_name='us-east-1')
            self.elbv2_client = self.session.client('elbv2')
            self.iam_client = self.session.client('iam')
            self.cloudwatch_client = self.session.client('cloudwatch')
            self.logs_client = self.session.client('logs')
            
            logger.info("AWS Intelligence clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise
    
    async def investigate_image_pull_error(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Intelligently investigate image pull errors by checking ECR, network, and permissions
        """
        investigation = {
            "error_type": "image_pull",
            "findings": [],
            "recommendations": [],
            "suggested_commands": [],
            "resolution_confidence": 0.0
        }
        
        try:
            # Extract image information from error message
            image_info = self._extract_image_info(error_data.get('error_message', ''))
            pod_name = error_data.get('pod_name', '')
            namespace = error_data.get('namespace', 'default')
            
            if not image_info:
                investigation["findings"].append("Could not extract image information from error message")
                return investigation
            
            registry = image_info.get('registry', '')
            repository = image_info.get('repository', '')
            tag = image_info.get('tag', 'latest')
            
            # 1. Check ECR repository and image existence
            ecr_results = await self._investigate_ecr_image(registry, repository, tag)
            investigation["findings"].extend(ecr_results["findings"])
            investigation["recommendations"].extend(ecr_results["recommendations"])
            
            # 2. Check node network connectivity and permissions
            network_results = await self._investigate_network_access(pod_name, namespace, registry)
            investigation["findings"].extend(network_results["findings"])
            investigation["recommendations"].extend(network_results["recommendations"])
            
            # 3. Check IAM permissions for ECR access
            permissions_results = await self._investigate_ecr_permissions(namespace)
            investigation["findings"].extend(permissions_results["findings"])
            investigation["recommendations"].extend(permissions_results["recommendations"])
            
            # 4. Generate intelligent resolution commands
            investigation["suggested_commands"] = self._generate_intelligent_commands(
                image_info, ecr_results, network_results, pod_name, namespace
            )
            
            # Calculate confidence based on findings
            investigation["resolution_confidence"] = self._calculate_confidence(investigation)
            
            logger.info(f"Image pull investigation completed with {investigation['resolution_confidence']:.2f} confidence")
            
        except Exception as e:
            logger.error(f"Error during image pull investigation: {e}")
            investigation["findings"].append(f"Investigation error: {str(e)}")
        
        return investigation
    
    async def investigate_resource_limit_error(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Investigate resource limit errors by checking cluster capacity, node resources, and limits
        """
        investigation = {
            "error_type": "resource_limit",
            "findings": [],
            "recommendations": [],
            "suggested_commands": [],
            "resolution_confidence": 0.0
        }
        
        try:
            pod_name = error_data.get('pod_name', '')
            namespace = error_data.get('namespace', 'default')
            
            # 1. Check cluster capacity
            capacity_results = await self._investigate_cluster_capacity()
            investigation["findings"].extend(capacity_results["findings"])
            investigation["recommendations"].extend(capacity_results["recommendations"])
            
            # 2. Check node group scaling settings
            scaling_results = await self._investigate_node_group_scaling()
            investigation["findings"].extend(scaling_results["findings"])
            investigation["recommendations"].extend(scaling_results["recommendations"])
            
            # 3. Check resource quotas and limits
            quota_results = await self._investigate_resource_quotas(namespace)
            investigation["findings"].extend(quota_results["findings"])
            investigation["recommendations"].extend(quota_results["recommendations"])
            
            # 4. Generate scaling commands
            investigation["suggested_commands"] = self._generate_resource_commands(
                capacity_results, scaling_results, pod_name, namespace
            )
            
            investigation["resolution_confidence"] = self._calculate_confidence(investigation)
            
        except Exception as e:
            logger.error(f"Error during resource limit investigation: {e}")
            investigation["findings"].append(f"Investigation error: {str(e)}")
        
        return investigation
    
    async def investigate_network_error(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Investigate network errors by checking security groups, NACLs, and service connectivity
        """
        investigation = {
            "error_type": "network",
            "findings": [],
            "recommendations": [],
            "suggested_commands": [],
            "resolution_confidence": 0.0
        }
        
        try:
            pod_name = error_data.get('pod_name', '')
            namespace = error_data.get('namespace', 'default')
            
            # 1. Check security groups
            sg_results = await self._investigate_security_groups()
            investigation["findings"].extend(sg_results["findings"])
            investigation["recommendations"].extend(sg_results["recommendations"])
            
            # 2. Check service discovery
            service_results = await self._investigate_service_connectivity(namespace)
            investigation["findings"].extend(service_results["findings"])
            investigation["recommendations"].extend(service_results["recommendations"])
            
            # 3. Check load balancer health
            lb_results = await self._investigate_load_balancer_health()
            investigation["findings"].extend(lb_results["findings"])
            investigation["recommendations"].extend(lb_results["recommendations"])
            
            investigation["suggested_commands"] = self._generate_network_commands(
                sg_results, service_results, pod_name, namespace
            )
            
            investigation["resolution_confidence"] = self._calculate_confidence(investigation)
            
        except Exception as e:
            logger.error(f"Error during network investigation: {e}")
            investigation["findings"].append(f"Investigation error: {str(e)}")
        
        return investigation
    
    async def investigate_node_error(self, error_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Investigate node errors by checking EC2 health, EKS node group status, and system resources
        """
        investigation = {
            "error_type": "node",
            "findings": [],
            "recommendations": [],
            "suggested_commands": [],
            "resolution_confidence": 0.0
        }
        
        try:
            # 1. Check node group health
            node_health = await self._investigate_node_group_health()
            investigation["findings"].extend(node_health["findings"])
            investigation["recommendations"].extend(node_health["recommendations"])
            
            # 2. Check EC2 instances
            ec2_health = await self._investigate_ec2_instances()
            investigation["findings"].extend(ec2_health["findings"])
            investigation["recommendations"].extend(ec2_health["recommendations"])
            
            # 3. Check CloudWatch metrics
            metrics_results = await self._investigate_node_metrics()
            investigation["findings"].extend(metrics_results["findings"])
            investigation["recommendations"].extend(metrics_results["recommendations"])
            
            investigation["suggested_commands"] = self._generate_node_commands(
                node_health, ec2_health, error_data.get('metadata', {}).get('node_name', '')
            )
            
            investigation["resolution_confidence"] = self._calculate_confidence(investigation)
            
        except Exception as e:
            logger.error(f"Error during node investigation: {e}")
            investigation["findings"].append(f"Investigation error: {str(e)}")
        
        return investigation
    
    async def _investigate_ecr_image(self, registry: str, repository: str, tag: str) -> Dict[str, Any]:
        """Check if image exists in ECR and get available tags"""
        results = {"findings": [], "recommendations": []}
        
        try:
            if 'public.ecr.aws' in registry:
                # Public ECR
                try:
                    response = self.ecr_public_client.describe_images(
                        repositoryName=repository,
                        imageIds=[{'imageTag': tag}]
                    )
                    
                    if response['imageDetails']:
                        results["findings"].append(f"✅ Image {registry}/{repository}:{tag} exists in public ECR")
                        
                        # Get image details
                        image_detail = response['imageDetails'][0]
                        image_size = image_detail.get('imageSizeInBytes', 0)
                        push_date = image_detail.get('imagePushedAt', datetime.now())
                        
                        results["findings"].append(f"Image size: {image_size / (1024*1024):.1f} MB, pushed: {push_date}")
                    else:
                        results["findings"].append(f"❌ Image tag '{tag}' not found in {repository}")
                        
                        # Get available tags
                        all_images = self.ecr_public_client.describe_images(repositoryName=repository)
                        available_tags = []
                        for img in all_images.get('imageDetails', []):
                            available_tags.extend(img.get('imageTags', []))
                        
                        if available_tags:
                            results["findings"].append(f"Available tags: {', '.join(available_tags[:10])}")
                            results["recommendations"].append(f"Use an existing tag like: {available_tags[0]}")
                        else:
                            results["recommendations"].append("Repository exists but has no tagged images")
                            
                except ClientError as e:
                    if e.response['Error']['Code'] == 'RepositoryNotFoundException':
                        results["findings"].append(f"❌ Repository '{repository}' not found in public ECR")
                        results["recommendations"].append("Verify repository name and ensure it exists")
                    else:
                        results["findings"].append(f"ECR access error: {e}")
                        
            else:
                # Private ECR
                try:
                    response = self.ecr_client.describe_images(
                        repositoryName=repository,
                        imageIds=[{'imageTag': tag}]
                    )
                    
                    if response['imageDetails']:
                        results["findings"].append(f"✅ Image {registry}/{repository}:{tag} exists in private ECR")
                    else:
                        results["findings"].append(f"❌ Image tag '{tag}' not found")
                        
                except ClientError as e:
                    results["findings"].append(f"Private ECR error: {e}")
                    
        except Exception as e:
            results["findings"].append(f"ECR investigation error: {e}")
        
        return results
    
    async def _investigate_network_access(self, pod_name: str, namespace: str, registry: str) -> Dict[str, Any]:
        """Check network connectivity to registry"""
        results = {"findings": [], "recommendations": []}
        
        try:
            # Check if cluster has internet access
            cluster_info = await self._get_cluster_info()
            
            if cluster_info and 'resourcesVpcConfig' in cluster_info:
                vpc_config = cluster_info['resourcesVpcConfig']
                subnet_ids = vpc_config.get('subnetIds', [])
                
                # Check if subnets are public or private
                if subnet_ids:
                    subnets = self.ec2_client.describe_subnets(SubnetIds=subnet_ids[:5])  # Check first 5
                    
                    public_subnets = 0
                    private_subnets = 0
                    
                    for subnet in subnets['Subnets']:
                        if subnet.get('MapPublicIpOnLaunch', False):
                            public_subnets += 1
                        else:
                            private_subnets += 1
                    
                    if private_subnets > 0 and public_subnets == 0:
                        results["findings"].append("⚠️ Cluster uses private subnets - need NAT Gateway for internet access")
                        results["recommendations"].append("Verify NAT Gateway exists and routes are configured")
                    elif public_subnets > 0:
                        results["findings"].append("✅ Cluster has public subnets available")
                        
        except Exception as e:
            results["findings"].append(f"Network investigation error: {e}")
        
        return results
    
    async def _investigate_ecr_permissions(self, namespace: str) -> Dict[str, Any]:
        """Check IAM permissions for ECR access"""
        results = {"findings": [], "recommendations": []}
        
        try:
            cluster_info = await self._get_cluster_info()
            
            if cluster_info and 'roleArn' in cluster_info:
                role_arn = cluster_info['roleArn']
                role_name = role_arn.split('/')[-1]
                
                # Check if role has ECR permissions
                try:
                    policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
                    
                    ecr_policy_found = False
                    for policy in policies['AttachedPolicies']:
                        if 'ECR' in policy['PolicyName'] or 'ContainerRegistry' in policy['PolicyName']:
                            ecr_policy_found = True
                            results["findings"].append(f"✅ ECR policy found: {policy['PolicyName']}")
                    
                    if not ecr_policy_found:
                        results["findings"].append("⚠️ No explicit ECR policies found on cluster role")
                        results["recommendations"].append("Verify ECR access permissions for worker nodes")
                        
                except Exception as e:
                    results["findings"].append(f"IAM permission check error: {e}")
                    
        except Exception as e:
            results["findings"].append(f"Permission investigation error: {e}")
        
        return results
    
    async def _investigate_cluster_capacity(self) -> Dict[str, Any]:
        """Check cluster resource capacity"""
        results = {"findings": [], "recommendations": []}
        
        try:
            node_groups = await self._get_node_groups()
            
            total_capacity = {"cpu": 0, "memory": 0, "nodes": 0}
            
            for ng in node_groups:
                ng_name = ng['nodegroupName']
                instance_types = ng.get('instanceTypes', ['unknown'])
                desired_capacity = ng.get('scalingConfig', {}).get('desiredSize', 0)
                
                total_capacity["nodes"] += desired_capacity
                
                # Estimate capacity based on instance types
                for instance_type in instance_types:
                    # This is a simplified estimation - in reality you'd query EC2 for exact specs
                    cpu_estimate, memory_estimate = self._estimate_instance_resources(instance_type)
                    total_capacity["cpu"] += cpu_estimate * desired_capacity
                    total_capacity["memory"] += memory_estimate * desired_capacity
                
                results["findings"].append(f"Node group {ng_name}: {desired_capacity} {instance_types[0]} nodes")
            
            results["findings"].append(f"Total estimated capacity: {total_capacity['nodes']} nodes, ~{total_capacity['cpu']} vCPUs")
            
            if total_capacity["nodes"] == 0:
                results["recommendations"].append("⚠️ No active nodes found - check node group scaling")
            
        except Exception as e:
            results["findings"].append(f"Capacity investigation error: {e}")
        
        return results
    
    def _extract_image_info(self, error_message: str) -> Optional[Dict[str, str]]:
        """Extract image registry, repository, and tag from error message"""
        # Pattern to match various image formats
        patterns = [
            r'(?:image|pull)\s+["\']?([^"\s]+/[^"\s]+):([^"\s\)]+)',  # registry/repo:tag
            r'(?:image|pull)\s+["\']?([^"\s/:]+):([^"\s\)]+)',        # repo:tag
            r'Failed to pull image ["\']?([^"]+)["\']?'               # general format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, error_message, re.IGNORECASE)
            if match:
                full_image = match.group(1)
                tag = match.group(2) if len(match.groups()) > 1 else 'latest'
                
                # Split registry and repository
                if '/' in full_image:
                    parts = full_image.split('/')
                    if '.' in parts[0]:  # Has registry
                        registry = parts[0]
                        repository = '/'.join(parts[1:])
                    else:  # No registry, assume Docker Hub
                        registry = 'docker.io'
                        repository = full_image
                else:
                    registry = 'docker.io'
                    repository = full_image
                
                return {
                    'registry': registry,
                    'repository': repository,
                    'tag': tag,
                    'full_image': f"{registry}/{repository}:{tag}"
                }
        
        return None
    
    async def _get_cluster_info(self) -> Optional[Dict[str, Any]]:
        """Get cached cluster information"""
        now = datetime.now()
        
        if (self._cache['cluster_info'] and 
            self._cache['cache_timestamp'] and
            (now - self._cache['cache_timestamp']).total_seconds() < self._cache_ttl):
            return self._cache['cluster_info']
        
        try:
            response = self.eks_client.describe_cluster(name=self.config.aws.cluster_name)
            cluster_info = response['cluster']
            
            self._cache['cluster_info'] = cluster_info
            self._cache['cache_timestamp'] = now
            
            return cluster_info
            
        except Exception as e:
            logger.error(f"Error getting cluster info: {e}")
            return None
    
    async def _get_node_groups(self) -> List[Dict[str, Any]]:
        """Get node group information"""
        if (self._cache['node_groups'] and 
            self._cache['cache_timestamp'] and
            (datetime.now() - self._cache['cache_timestamp']).total_seconds() < self._cache_ttl):
            return self._cache['node_groups']
        
        try:
            # List node groups
            response = self.eks_client.list_nodegroups(clusterName=self.config.aws.cluster_name)
            
            node_groups = []
            for ng_name in response.get('nodegroups', []):
                ng_detail = self.eks_client.describe_nodegroup(
                    clusterName=self.config.aws.cluster_name,
                    nodegroupName=ng_name
                )
                node_groups.append(ng_detail['nodegroup'])
            
            self._cache['node_groups'] = node_groups
            return node_groups
            
        except Exception as e:
            logger.error(f"Error getting node groups: {e}")
            return []
    
    def _estimate_instance_resources(self, instance_type: str) -> Tuple[int, int]:
        """Estimate vCPU and memory for instance type (simplified)"""
        # This is a basic estimation - in production, query EC2 API for exact specs
        instance_specs = {
            't3.micro': (2, 1),
            't3.small': (2, 2),
            't3.medium': (2, 4),
            't3.large': (2, 8),
            'm5.large': (2, 8),
            'm5.xlarge': (4, 16),
            'm5.2xlarge': (8, 32),
            'c5.large': (2, 4),
            'c5.xlarge': (4, 8),
        }
        
        return instance_specs.get(instance_type, (2, 4))  # Default fallback
    
    def _generate_intelligent_commands(self, image_info: Dict[str, str], ecr_results: Dict[str, Any],
                                     network_results: Dict[str, Any], pod_name: str, namespace: str) -> List[str]:
        """Generate intelligent kubectl commands based on investigation results"""
        commands = []
        
        # Always start with diagnosis
        commands.append(f"kubectl describe pod {pod_name} -n {namespace}")
        commands.append(f"kubectl get events -n {namespace} --field-selector involvedObject.name={pod_name}")
        
        # If image doesn't exist, suggest updating to available tag
        for finding in ecr_results["findings"]:
            if "Available tags:" in finding:
                # Extract first available tag and suggest using it
                tags_part = finding.split("Available tags: ")[1]
                first_tag = tags_part.split(",")[0].strip()
                
                deployment_name = pod_name.rsplit('-', 2)[0]  # Extract deployment name
                commands.append(f"kubectl set image deployment/{deployment_name} {deployment_name}={image_info['registry']}/{image_info['repository']}:{first_tag} -n {namespace}")
        
        # Force pod restart to pull new image
        commands.append(f"kubectl delete pod {pod_name} -n {namespace}")
        
        return commands
    
    def _generate_resource_commands(self, capacity_results: Dict[str, Any], scaling_results: Dict[str, Any],
                                  pod_name: str, namespace: str) -> List[str]:
        """Generate commands for resource limit issues"""
        commands = []
        
        commands.append(f"kubectl describe pod {pod_name} -n {namespace}")
        commands.append(f"kubectl top nodes")
        commands.append(f"kubectl get nodes -o wide")
        commands.append(f"kubectl describe nodes")
        
        return commands
    
    def _generate_network_commands(self, sg_results: Dict[str, Any], service_results: Dict[str, Any],
                                 pod_name: str, namespace: str) -> List[str]:
        """Generate commands for network issues"""
        commands = []
        
        commands.append(f"kubectl describe pod {pod_name} -n {namespace}")
        commands.append(f"kubectl get svc -n {namespace}")
        commands.append(f"kubectl get endpoints -n {namespace}")
        commands.append(f"kubectl get networkpolicies -n {namespace}")
        
        return commands
    
    def _generate_node_commands(self, node_health: Dict[str, Any], ec2_health: Dict[str, Any],
                               node_name: str) -> List[str]:
        """Generate commands for node issues"""
        commands = []
        
        if node_name:
            commands.append(f"kubectl describe node {node_name}")
        commands.append("kubectl get nodes -o wide")
        commands.append("kubectl top nodes")
        
        return commands
    
    def _calculate_confidence(self, investigation: Dict[str, Any]) -> float:
        """Calculate confidence score based on investigation completeness"""
        findings_count = len(investigation["findings"])
        recommendations_count = len(investigation["recommendations"])
        commands_count = len(investigation["suggested_commands"])
        
        # Basic scoring
        score = min(1.0, (findings_count * 0.3 + recommendations_count * 0.4 + commands_count * 0.3) / 5)
        
        # Bonus for specific positive findings
        for finding in investigation["findings"]:
            if "✅" in finding:
                score += 0.1
            elif "❌" in finding:
                score += 0.05  # Still valuable negative information
        
        return min(1.0, score)
    
    # Placeholder methods for other investigations - implement based on needs
    async def _investigate_node_group_scaling(self) -> Dict[str, Any]:
        return {"findings": ["Node group scaling check not implemented"], "recommendations": []}
    
    async def _investigate_resource_quotas(self, namespace: str) -> Dict[str, Any]:
        return {"findings": ["Resource quota check not implemented"], "recommendations": []}
    
    async def _investigate_security_groups(self) -> Dict[str, Any]:
        return {"findings": ["Security group check not implemented"], "recommendations": []}
    
    async def _investigate_service_connectivity(self, namespace: str) -> Dict[str, Any]:
        return {"findings": ["Service connectivity check not implemented"], "recommendations": []}
    
    async def _investigate_load_balancer_health(self) -> Dict[str, Any]:
        return {"findings": ["Load balancer health check not implemented"], "recommendations": []}
    
    async def _investigate_node_group_health(self) -> Dict[str, Any]:
        return {"findings": ["Node group health check not implemented"], "recommendations": []}
    
    async def _investigate_ec2_instances(self) -> Dict[str, Any]:
        return {"findings": ["EC2 instance check not implemented"], "recommendations": []}
    
    async def _investigate_node_metrics(self) -> Dict[str, Any]:
        return {"findings": ["Node metrics check not implemented"], "recommendations": []}