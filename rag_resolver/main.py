#!/usr/bin/env python3
"""
Main entry point for the RAG Resolver microservice
Handles CLI commands and service startup
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

# Now import with absolute imports
try:
    from rag_resolver.config import load_resolver_config
    from rag_resolver.service import main as start_service
    from rag_resolver.rag_engine import RAGEngine
except ImportError:
    # Fallback for direct execution
    from config import load_resolver_config
    from service import main as start_service
    from rag_engine import RAGEngine

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

async def test_components():
    """Test all system components"""
    config = load_resolver_config()
    print("🧪 Testing RAG Resolver Components...")
    print("=" * 50)
    
    try:
        # Test 1: Configuration
        print("✅ Configuration loaded successfully")
        print(f"   - Service: {config.service_name}")
        print(f"   - Qdrant: {config.qdrant.host}:{config.qdrant.port}")
        print(f"   - EKS Cluster: {config.aws.cluster_name}")
        print(f"   - MongoDB: {config.mongodb.database_name}")
        
        # Test 2: Initialize RAG Engine
        print("\n🔧 Initializing RAG Engine...")
        rag_engine = RAGEngine(config)
        print("✅ RAG Engine initialized successfully")
        
        # Test 3: Vector Store
        print("\n📊 Testing Vector Store...")
        stats = rag_engine.get_resolution_stats()
        if "error" not in stats:
            print("✅ Vector store connection successful")
            print(f"   - Total resolutions: {stats.get('total_resolutions', 0)}")
            print(f"   - Success rate: {stats.get('success_rate', 0):.1f}%")
        else:
            print(f"❌ Vector store error: {stats['error']}")
        
        # Test 4: MCP Executor
        print("\n⚙️  Testing MCP Executor...")
        cluster_info = rag_engine.mcp_executor.get_cluster_info()
        if "error" not in cluster_info:
            print("✅ MCP Executor connection successful")
            print(f"   - Cluster: {cluster_info.get('cluster_name')}")
            print(f"   - Nodes: {cluster_info.get('node_count', 0)}")
        else:
            print(f"❌ MCP Executor error: {cluster_info['error']}")
        
        # Test 5: Sample Resolution
        print("\n🎯 Testing Sample Resolution...")
        sample_error = {
            "error_type": "image_pull",
            "error_message": "Failed to pull image nginx:invalid-tag",
            "pod_name": "test-pod",
            "namespace": "default",
            "error_severity": "HIGH"
        }
        
        # Test resolution planning (without execution)
        similar_resolutions = await rag_engine._retrieve_similar_resolutions(sample_error)
        print(f"✅ Found {len(similar_resolutions)} similar resolutions for test error")
        
        # Cleanup
        rag_engine.close()
        print("\n✅ All component tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Component test failed: {e}")
        return False

async def resolve_error_cli(error_log_id: str, auto_execute: bool = False):
    """CLI command to resolve a specific error"""
    config = load_resolver_config()
    print(f"🔧 Resolving error: {error_log_id}")
    print("=" * 50)
    
    try:
        # Initialize RAG Engine
        rag_engine = RAGEngine(config)
        
        # Fetch error from MongoDB (simulate for CLI)
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
        
        # Start resolution
        print(f"\n🚀 Starting resolution (auto_execute: {auto_execute})...")
        result = await rag_engine.resolve_error(error_doc, auto_execute)
        
        # Display results
        print(f"\n📊 Resolution Results:")
        print(f"   - Success: {result.get('success', False)}")
        print(f"   - Resolution ID: {result.get('resolution_id', 'N/A')}")
        print(f"   - Steps Executed: {result.get('steps_executed', 0)}")
        print(f"   - Duration: {result.get('resolution_duration', 0):.2f}s")
        print(f"   - Auto Executed: {result.get('auto_executed', False)}")
        
        if result.get('recommendations'):
            print(f"\n💡 Recommendations:")
            for i, rec in enumerate(result['recommendations'], 1):
                print(f"   {i}. {rec}")
        
        if result.get('manual_steps'):
            print(f"\n🔧 Manual Steps Required:")
            for i, step in enumerate(result['manual_steps'], 1):
                print(f"   {i}. {step}")
        
        # Cleanup
        rag_engine.close()
        client.close()
        
        return result.get('success', False)
        
    except Exception as e:
        print(f"❌ Resolution failed: {e}")
        return False

async def generate_report_cli(error_log_id: str):
    """CLI command to generate a resolution report"""
    config = load_resolver_config()
    print(f"📄 Generating resolution report for: {error_log_id}")
    print("=" * 50)
    
    try:
        # Initialize RAG Engine
        rag_engine = RAGEngine(config)
        
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
        
        # Generate report
        print("🔍 Analyzing error and generating comprehensive report...")
        report = await rag_engine.generate_resolution_report(error_doc)
        
        # Save report to file
        report_filename = f"resolution_report_{error_log_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_filename, 'w') as f:
            f.write(f"# Resolution Report\n")
            f.write(f"**Generated:** {report.get('generated_at')}\n")
            f.write(f"**Report ID:** {report.get('report_id')}\n")
            f.write(f"**Error Type:** {report.get('error_type')}\n")
            f.write(f"**Severity:** {report.get('severity')}\n\n")
            f.write(report.get('content', 'No content available'))
        
        print(f"✅ Report generated successfully!")
        print(f"   - Report ID: {report.get('report_id')}")
        print(f"   - File: {report_filename}")
        print(f"   - Status: {report.get('status')}")
        
        if report.get('recommended_actions'):
            print(f"\n🎯 Key Recommended Actions:")
            for i, action in enumerate(report['recommended_actions'], 1):
                print(f"   {i}. {action}")
        
        # Cleanup
        rag_engine.close()
        client.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        return False

def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description='RAG-based Kubernetes Error Resolver',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start the microservice
  python -m rag_resolver.main serve

  # Test all components
  python -m rag_resolver.main test

  # Resolve a specific error
  python -m rag_resolver.main resolve --error-id 507f1f77bcf86cd799439011

  # Resolve with auto-execution
  python -m rag_resolver.main resolve --error-id 507f1f77bcf86cd799439011 --auto-execute

  # Generate resolution report
  python -m rag_resolver.main report --error-id 507f1f77bcf86cd799439011

  # Get system status
  python -m rag_resolver.main status
        """
    )
    
    parser.add_argument('command', 
                       choices=['serve', 'test', 'resolve', 'report', 'status'],
                       help='Command to execute')
    
    parser.add_argument('--error-id', type=str,
                       help='Error log ID from debug_results collection')
    
    parser.add_argument('--auto-execute', action='store_true',
                       help='Enable auto-execution of safe commands')
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    
    parser.add_argument('--port', type=int, default=8080,
                       help='Service port (for serve command)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    if args.command == 'serve':
        # Start the microservice
        print(f"🚀 Starting RAG Resolver Service on port {args.port}")
        start_service()
        
    elif args.command == 'test':
        # Test components
        success = asyncio.run(test_components())
        sys.exit(0 if success else 1)
        
    elif args.command == 'resolve':
        if not args.error_id:
            print("❌ Error ID is required for resolve command")
            sys.exit(1)
        
        # Resolve error
        success = asyncio.run(resolve_error_cli(args.error_id, args.auto_execute))
        sys.exit(0 if success else 1)
        
    elif args.command == 'report':
        if not args.error_id:
            print("❌ Error ID is required for report command")
            sys.exit(1)
        
        # Generate report
        success = asyncio.run(generate_report_cli(args.error_id))
        sys.exit(0 if success else 1)
        
    elif args.command == 'status':
        # Get system status
        config = load_resolver_config()
        print("📊 RAG Resolver System Status")
        print("=" * 40)
        print(f"Service Name: {config.service_name}")
        print(f"Service Port: {config.service_port}")
        print(f"EKS Cluster: {config.aws.cluster_name}")
        print(f"AWS Region: {config.aws.region}")
        print(f"Qdrant Host: {config.qdrant.host}:{config.qdrant.port}")
        print(f"MongoDB: {config.mongodb.database_name}")
        print(f"Auto Execute: {config.auto_execute_safe_commands}")
        print(f"Max Concurrent: {config.max_concurrent_resolutions}")
        
        # Test basic connectivity
        try:
            success = asyncio.run(test_components())
            print(f"\n🔍 Component Status: {'✅ All Healthy' if success else '❌ Issues Detected'}")
        except Exception as e:
            print(f"\n❌ Status check failed: {e}")

if __name__ == "__main__":
    main()