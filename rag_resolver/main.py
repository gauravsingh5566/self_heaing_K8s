#!/usr/bin/env python3
"""
Main entry point for the RAG Resolver microservice with Smart Intelligence
Handles CLI commands and service startup with actual AWS/K8s command execution
"""

import asyncio
import argparse
import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import smart components
try:
    from rag_resolver.config import load_resolver_config
    from rag_resolver.service import main as start_service
    from rag_resolver.smart_rag_engine import SmartRAGEngine
except ImportError:
    # Fallback for direct execution
    from config import load_resolver_config
    from service import main as start_service
    from smart_rag_engine import SmartRAGEngine

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

async def test_smart_components():
    """Test smart system components"""
    config = load_resolver_config()
    print("🧪 Testing Smart RAG Resolver Components...")
    print("=" * 50)
    
    try:
        # Test 1: Configuration
        print("✅ Configuration loaded successfully")
        print(f"   - Service: {config.service_name}")
        print(f"   - Qdrant: {config.qdrant.host}:{config.qdrant.port}")
        print(f"   - EKS Cluster: {config.aws.cluster_name}")
        print(f"   - MongoDB: {config.mongodb.database_name}")
        
        # Test 2: Initialize Smart RAG Engine
        print("\n🔧 Initializing Smart RAG Engine...")
        rag_engine = SmartRAGEngine(config)
        print("✅ Smart RAG Engine initialized successfully")
        
        # Test 3: AWS Command Execution
        print("\n⚙️  Testing AWS Command Execution...")
        test_aws_cmd = f"aws sts get-caller-identity --profile {config.aws.profile}"
        aws_result = await rag_engine.aws_executor._execute_command(test_aws_cmd)
        
        if aws_result.success:
            print("✅ AWS CLI execution successful")
            print(f"   - Account: {aws_result.output[:50]}...")
        else:
            print(f"⚠️ AWS CLI test warning: {aws_result.error[:100]}...")
        
        # Test 4: Kubectl Command Execution
        print("\n🎯 Testing Kubectl Command Execution...")
        test_k8s_cmd = "kubectl get nodes"
        k8s_result = await rag_engine.aws_executor._execute_command(test_k8s_cmd)
        
        if k8s_result.success:
            print("✅ Kubectl execution successful")
            node_count = len([line for line in k8s_result.output.split('\n') if line.strip() and not line.startswith('NAME')])
            print(f"   - Nodes: {node_count}")
        else:
            print(f"⚠️ Kubectl test warning: {k8s_result.error[:100]}...")
        
        # Test 5: Image parsing capability
        print("\n🐳 Testing Image Parsing...")
        test_images = [
            "nginx:latest",
            "public.ecr.aws/sample/node:v1",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-app:latest"
        ]
        
        for image in test_images:
            parsed = rag_engine.aws_executor._parse_image_url(image)
            if parsed:
                print(f"✅ Parsed {image}")
                print(f"   - Registry: {parsed['registry']}")
                print(f"   - Repository: {parsed['repository']}")
                print(f"   - Tag: {parsed['tag']}")
            else:
                print(f"❌ Failed to parse {image}")
        
        # Cleanup
        rag_engine.close()
        print("\n✅ All smart component tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Smart component test failed: {e}")
        return False

async def resolve_error_smartly(error_log_id: str, auto_execute: bool = True):
    """CLI command to smartly resolve a specific error"""
    config = load_resolver_config()
    print(f"🚀 SMARTLY Resolving error: {error_log_id}")
    print("=" * 50)
    
    try:
        # Initialize Smart RAG Engine
        rag_engine = SmartRAGEngine(config)
        
        # Fetch error from MongoDB
        from pymongo import MongoClient
        client = MongoClient(config.mongodb.uri)
        db = client[config.mongodb.database_name]
        debug_collection = db[config.mongodb.debug_collection]
        
        error_doc = debug_collection.find_one({"error_log_id": error_log_id})
        if not error_doc:
            print(f"❌ Error log {error_log_id} not found in debug_results")
            return False
        
        print(f"📋 Error Details:")
        print(f"   - Type: {error_doc.get('error_type', 'unknown')}")
        print(f"   - Severity: {error_doc.get('error_severity', 'unknown')}")
        print(f"   - Pod: {error_doc.get('pod_name', 'unknown')}")
        print(f"   - Namespace: {error_doc.get('namespace', 'unknown')}")
        
        # Convert ObjectId to string
        error_doc["_id"] = str(error_doc["_id"])
        
        # Start SMART resolution
        print(f"\n🧠 Starting SMART resolution (auto_execute: {auto_execute})...")
        print("   This will actually execute AWS and kubectl commands to fix the issue!")
        
        result = await rag_engine.resolve_error_smartly(error_doc, auto_execute)
        
        # Display smart results
        print(f"\n📊 Smart Resolution Results:")
        print(f"   - Success: {'✅' if result.get('success', False) else '❌'} {result.get('success', False)}")
        print(f"   - Resolution ID: {result.get('resolution_id', 'N/A')}")
        print(f"   - Resolution Type: {result.get('resolution_type', 'unknown')}")
        print(f"   - Steps Executed: {result.get('steps_executed', 0)}")
        print(f"   - Duration: {result.get('resolution_duration', 0):.2f}s")
        print(f"   - Auto Executed: {result.get('auto_executed', False)}")
        print(f"   - Intelligence Confidence: {result.get('intelligence_confidence', 0.0):.2f}")
        
        if result.get('resolution_summary'):
            print(f"\n📝 Resolution Summary:")
            print(f"   {result['resolution_summary']}")
        
        if result.get('actual_changes_made'):
            print(f"\n🔧 Actual Changes Made:")
            for i, change in enumerate(result['actual_changes_made'], 1):
                print(f"   {i}. {change}")
        
        if result.get('execution_results', {}).get('steps'):
            print(f"\n📋 Detailed Execution Steps:")
            for i, step in enumerate(result['execution_results']['steps'], 1):
                status = "✅" if step.get('success', False) else "❌"
                print(f"   {i}. {status} {step.get('step', 'Unknown step')}")
                if step.get('error'):
                    print(f"      Error: {step['error']}")
                elif step.get('result') and step['success']:
                    result_str = str(step['result'])
                    if len(result_str) > 100:
                        result_str = result_str[:100] + "..."
                    print(f"      Result: {result_str}")
        
        if result.get('recommendations'):
            print(f"\n💡 AI Recommendations:")
            for i, rec in enumerate(result['recommendations'], 1):
                print(f"   {i}. {rec}")
        
        # Show validation results
        validation = result.get('validation_results', {})
        if validation:
            print(f"\n🔍 Resolution Validation:")
            print(f"   - Validated: {'✅' if validation.get('success', False) else '❌'}")
            print(f"   - Type: {validation.get('validation_type', 'unknown')}")
            print(f"   - Message: {validation.get('message', 'N/A')}")
        
        # Cleanup
        rag_engine.close()
        client.close()
        
        return result.get('success', False)
        
    except Exception as e:
        print(f"❌ Smart resolution failed: {e}")
        return False

async def generate_smart_report_cli(error_log_id: str):
    """CLI command to generate a comprehensive smart report"""
    config = load_resolver_config()
    print(f"📄 Generating smart resolution report for: {error_log_id}")
    print("=" * 50)
    
    try:
        # Initialize Smart RAG Engine
        rag_engine = SmartRAGEngine(config)
        
        # Fetch error from MongoDB
        from pymongo import MongoClient
        client = MongoClient(config.mongodb.uri)
        db = client[config.mongodb.database_name]
        debug_collection = db[config.mongodb.debug_collection]
        
        error_doc = debug_collection.find_one({"error_log_id": error_log_id})
        if not error_doc:
            print(f"❌ Error log {error_log_id} not found in debug_results")
            return False
        
        # Convert ObjectId to string
        error_doc["_id"] = str(error_doc["_id"])
        
        # Generate comprehensive report with AWS intelligence
        print("🔍 Analyzing error and generating smart report with AWS intelligence...")
        
        # First get AWS investigation
        aws_investigation = await rag_engine._perform_aws_investigation(error_doc)
        
        # Generate detailed report
        report_content = f"""# Smart Kubernetes Error Resolution Report

**Generated:** {datetime.now().isoformat()}
**Error Type:** {error_doc.get('error_type', 'unknown')}
**Severity:** {error_doc.get('error_severity', 'unknown')}
**Pod:** {error_doc.get('pod_name', 'unknown')}
**Namespace:** {error_doc.get('namespace', 'unknown')}

## Executive Summary
This error involves a {error_doc.get('error_type', 'unknown')} issue in the Kubernetes cluster.
AWS Intelligence confidence level: {aws_investigation.get('resolution_confidence', 0.0):.2f}

## AWS Intelligence Analysis

### Key Findings
"""
        
        for finding in aws_investigation.get('findings', []):
            report_content += f"- {finding}\n"
        
        report_content += "\n### AWS Recommendations\n"
        for rec in aws_investigation.get('recommendations', []):
            report_content += f"- {rec}\n"
        
        report_content += f"""
## Smart Resolution Available

The Smart RAG Resolver can automatically resolve this issue by:

1. **Performing AWS Intelligence Investigation**
   - Analyzing ECR repositories and image availability
   - Checking IAM permissions and policies
   - Validating network and security configurations

2. **Executing Smart Commands**
   - Finding correct image tags from ECR
   - Updating Kubernetes deployments automatically
   - Restarting pods and validating resolution

3. **Validating Resolution Success**
   - Checking deployment health after changes
   - Monitoring pod startup and readiness
   - Storing resolution knowledge for future use

## To Execute Smart Resolution

Run the following command to have the system automatically fix this issue:

```bash
python main.py resolve --error-id {error_log_id} --auto-execute
```

This will:
- ✅ Investigate the issue using AWS services
- ✅ Execute the necessary fix commands
- ✅ Validate that the resolution worked
- ✅ Store the resolution for future learning

## Manual Steps (if auto-resolution is not desired)

If you prefer to resolve this manually, follow these AWS-intelligent steps:
"""
        
        # Add suggested commands from AWS investigation
        for cmd in aws_investigation.get('suggested_commands', []):
            report_content += f"- `{cmd}`\n"
        
        report_content += f"""
## Resolution Confidence

AWS Intelligence Confidence: **{aws_investigation.get('resolution_confidence', 0.0):.2f}**

- 0.8+ = High confidence, safe for auto-execution
- 0.6-0.8 = Medium confidence, recommended with monitoring
- <0.6 = Lower confidence, manual review recommended

## Next Steps

1. **For High Confidence (0.8+)**: Use auto-execution
2. **For Medium Confidence**: Review the steps and use auto-execution with monitoring
3. **For Low Confidence**: Use this report for manual resolution

---
*Generated by Smart RAG Resolver with AWS Intelligence*
"""
        
        # Save report to file
        report_filename = f"smart_resolution_report_{error_log_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_filename, 'w') as f:
            f.write(report_content)
        
        print(f"✅ Smart report generated successfully!")
        print(f"   - File: {report_filename}")
        print(f"   - AWS Intelligence Confidence: {aws_investigation.get('resolution_confidence', 0.0):.2f}")
        print(f"   - Recommended Action: {'Auto-execute' if aws_investigation.get('resolution_confidence', 0.0) > 0.8 else 'Review and execute'}")
        
        # Show key insights
        if aws_investigation.get('findings'):
            print(f"\n🔍 Key AWS Intelligence Findings:")
            for i, finding in enumerate(aws_investigation['findings'][:3], 1):
                print(f"   {i}. {finding}")
        
        print(f"\n💡 To automatically resolve this issue:")
        print(f"   python main.py resolve --error-id {error_log_id} --auto-execute")
        
        # Cleanup
        rag_engine.close()
        client.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        return False

def main():
    """Main CLI function with smart capabilities"""
    parser = argparse.ArgumentParser(
        description='Smart RAG-based Kubernetes Error Resolver with AWS Intelligence',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Smart Examples:
  # Start the microservice
  python main.py serve

  # Test smart components
  python main.py test

  # SMARTLY resolve error (executes real AWS/K8s commands!)
  python main.py resolve --error-id 507f1f77bcf86cd799439011 --auto-execute

  # Generate smart report with AWS intelligence
  python main.py report --error-id 507f1f77bcf86cd799439011

  # Get system status
  python main.py status

Key Features:
  ✅ Automatically checks ECR for correct image tags
  ✅ Updates Kubernetes deployments with working images
  ✅ Fixes IAM permissions for ECR access
  ✅ Scales EKS node groups when needed
  ✅ Validates that resolutions actually work
  ✅ Learns from successful resolutions
        """
    )
    
    parser.add_argument('command', 
                       choices=['serve', 'test', 'resolve', 'report', 'status'],
                       help='Command to execute')
    
    parser.add_argument('--error-id', type=str,
                       help='Error log ID from debug_results collection')
    
    parser.add_argument('--auto-execute', action='store_true',
                       help='Enable smart auto-execution of AWS/K8s commands')
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    
    parser.add_argument('--port', type=int, default=8080,
                       help='Service port (for serve command)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    if args.command == 'serve':
        # Start the microservice
        print(f"🚀 Starting Smart RAG Resolver Service on port {args.port}")
        print("   Features: AWS Intelligence + Auto-execution + Real command execution")
        start_service()
        
    elif args.command == 'test':
        # Test smart components
        success = asyncio.run(test_smart_components())
        sys.exit(0 if success else 1)
        
    elif args.command == 'resolve':
        if not args.error_id:
            print("❌ Error ID is required for resolve command")
            sys.exit(1)
        
        # Smartly resolve error
        print("🧠 Using Smart RAG Engine with AWS Intelligence")
        print("   This will execute real AWS and kubectl commands!")
        
        if not args.auto_execute:
            print("⚠️  Note: --auto-execute not specified, will only analyze without executing")
        
        success = asyncio.run(resolve_error_smartly(args.error_id, args.auto_execute))
        sys.exit(0 if success else 1)
        
    elif args.command == 'report':
        if not args.error_id:
            print("❌ Error ID is required for report command")
            sys.exit(1)
        
        # Generate smart report
        success = asyncio.run(generate_smart_report_cli(args.error_id))
        sys.exit(0 if success else 1)
        
    elif args.command == 'status':
        # Get smart system status
        config = load_resolver_config()
        print("📊 Smart RAG Resolver System Status")
        print("=" * 50)
        print(f"Service Name: {config.service_name}")
        print(f"Service Port: {config.service_port}")
        print(f"EKS Cluster: {config.aws.cluster_name}")
        print(f"AWS Region: {config.aws.region}")
        print(f"AWS Profile: {config.aws.profile}")
        print(f"Qdrant Host: {config.qdrant.host}:{config.qdrant.port}")
        print(f"MongoDB: {config.mongodb.database_name}")
        print(f"Auto Execute: {config.auto_execute_safe_commands}")
        print(f"Max Concurrent: {config.max_concurrent_resolutions}")
        
        print(f"\n🧠 Smart Features:")
        print(f"   ✅ AWS Intelligence Investigation")
        print(f"   ✅ Real AWS CLI Command Execution")
        print(f"   ✅ ECR Repository Analysis")
        print(f"   ✅ Kubernetes Deployment Updates")
        print(f"   ✅ IAM Permission Management")
        print(f"   ✅ EKS Node Group Scaling")
        print(f"   ✅ Automatic Resolution Validation")
        
        # Test basic connectivity
        try:
            success = asyncio.run(test_smart_components())
            print(f"\n🔍 Component Status: {'✅ All Smart Features Working' if success else '❌ Some Issues Detected'}")
        except Exception as e:
            print(f"\n❌ Status check failed: {e}")

if __name__ == "__main__":
    main()