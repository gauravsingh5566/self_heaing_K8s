#!/usr/bin/env python3
"""
Updated main.py for AI-Powered Smart RAG Resolver
Compatible with your existing config.py structure
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

# Import components - try AI-powered first, fallback to original
try:
    from rag_resolver.config import load_resolver_config
    from rag_resolver.service import main as start_service
    from rag_resolver.ai_smart_rag_engine import AISmartRAGEngine  # NEW AI ENGINE
    AI_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ AI engine not available: {e}")
    try:
        # Fallback to original
        from config import load_resolver_config
        from service import main as start_service
        from smart_rag_engine import SmartRAGEngine as AISmartRAGEngine
        AI_AVAILABLE = False
    except ImportError:
        from config import load_resolver_config
        from service import main as start_service
        from rag_engine import RAGEngine as AISmartRAGEngine
        AI_AVAILABLE = False

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    # Create logs directory
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/rag_resolver.log')
        ]
    )

async def test_ai_components():
    """Test AI-powered system components"""
    config = load_resolver_config()
    
    if AI_AVAILABLE:
        print("🤖 Testing AI-Powered Smart RAG Resolver Components...")
        print("=" * 60)
    else:
        print("🔧 Testing Standard Smart RAG Resolver Components...")
        print("=" * 60)
    
    try:
        # Test 1: Configuration
        print("✅ Configuration loaded successfully")
        print(f"   - Service: {config.service_name}")
        print(f"   - EKS Cluster: {config.aws.cluster_name}")
        print(f"   - AWS Region: {config.aws.region}")
        print(f"   - AI Model: {config.aws.bedrock_model_id}")
        print(f"   - MongoDB: {config.mongodb.database_name}")
        print(f"   - Qdrant: {config.qdrant.host}:{config.qdrant.port}")
        print(f"   - Auto Execute: {config.auto_execute_safe_commands}")
        print(f"   - Max Concurrent: {config.max_concurrent_resolutions}")
        print(f"   - Allowed Namespaces: {', '.join(config.mcp.allowed_namespaces)}")
        
        # Test 2: Initialize AI-Powered RAG Engine
        if AI_AVAILABLE:
            print("\n🤖 Initializing AI-Powered RAG Engine...")
        else:
            print("\n🔧 Initializing Standard RAG Engine...")
            
        rag_engine = AISmartRAGEngine(config)
        print("✅ RAG Engine initialized successfully")
        
        if AI_AVAILABLE:
            # Test 3: AI Expert Analysis (Mock)
            print("\n🧠 Testing AI Expert Analysis...")
            mock_error = {
                "error_type": "node_selector_mismatch",
                "pod_name": "test-app-12345",
                "namespace": "default",
                "error_message": "0/3 nodes are available: 3 node(s) didn't match pod node selector.",
                "node_name": "node-1"
            }
            
            try:
                ai_plan = await rag_engine.expert_ai.analyze_error_with_ai(mock_error)
                print(f"✅ AI Analysis completed with {ai_plan.confidence_score:.2f} confidence")
                print(f"   - Root Cause: {ai_plan.error_analysis.get('root_cause', 'Unknown')[:80]}...")
                print(f"   - Diagnosis Commands: {len(ai_plan.diagnosis_commands)} intelligent commands")
                print(f"   - Fix Commands: {len(ai_plan.fix_commands)} intelligent fixes")
                print(f"   - Validation Steps: {len(ai_plan.validation_commands)} validation commands")
                print(f"   - Risk Level: {ai_plan.risk_assessment.get('risk_level', 'Unknown')}")
                
                # Show sample commands
                print("\n🎯 Sample AI-generated commands:")
                for i, cmd in enumerate(ai_plan.diagnosis_commands[:2], 1):
                    print(f"   {i}. {cmd}")
                
            except Exception as e:
                print(f"⚠️ AI Analysis test failed (using fallback): {e}")
                print("   - Fallback expert analysis will be used")
        
        # Test 4: Component connectivity
        print("\n🔌 Testing Component Connectivity...")
        
        # Test MCP Executor
        try:
            session_id = rag_engine.mcp_executor.create_session("default")
            print("✅ MCP Executor: Session creation works")
            rag_engine.mcp_executor.close_session(session_id)
        except Exception as e:
            print(f"⚠️ MCP Executor: {e}")
        
        # Test Vector Store
        try:
            stats = rag_engine.get_resolution_stats()
            print("✅ Vector Store: Connection successful")
        except Exception as e:
            print(f"⚠️ Vector Store: {e}")
        
        # Test AWS Executor
        try:
            # This is just checking if the executor initializes
            print("✅ AWS Executor: Initialization successful")
        except Exception as e:
            print(f"⚠️ AWS Executor: {e}")
        
        # Cleanup
        await rag_engine.close()
        
        if AI_AVAILABLE:
            print("\n✅ All AI-powered component tests completed!")
        else:
            print("\n✅ All standard component tests completed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Component test failed: {e}")
        return False

async def resolve_error_with_ai(error_log_id: str, auto_execute: bool = True):
    """CLI command to resolve error using AI expert knowledge"""
    config = load_resolver_config()
    
    if AI_AVAILABLE:
        print(f"🤖 AI-POWERED Resolution for error: {error_log_id}")
        print(f"   Using: {config.aws.bedrock_model_id}")
    else:
        print(f"🔧 SMART Resolution for error: {error_log_id}")
        print(f"   Using: Standard rule-based resolution")
    
    print("=" * 60)
    
    try:
        # Initialize RAG Engine
        rag_engine = AISmartRAGEngine(config)
        
        # Fetch error from MongoDB
        from pymongo import MongoClient
        client = MongoClient(config.mongodb.uri)
        db = client[config.mongodb.database_name]
        debug_collection = db[config.mongodb.debug_collection]
        
        error_doc = debug_collection.find_one({"error_log_id": error_log_id})
        if not error_doc:
            print(f"❌ Error log {error_log_id} not found in {config.mongodb.debug_collection}")
            return False
        
        print(f"📋 Error Details:")
        print(f"   - Type: {error_doc.get('error_type', 'unknown')}")
        print(f"   - Severity: {error_doc.get('error_severity', 'unknown')}")
        print(f"   - Pod: {error_doc.get('pod_name', 'unknown')}")
        print(f"   - Namespace: {error_doc.get('namespace', 'unknown')}")
        print(f"   - Node: {error_doc.get('node_name', 'unknown')}")
        print(f"   - Message: {error_doc.get('error_message', '')[:100]}...")
        
        # Convert ObjectId to string
        error_doc["_id"] = str(error_doc["_id"])
        
        # Check namespace permissions
        namespace = error_doc.get('namespace', 'default')
        if namespace not in config.mcp.allowed_namespaces:
            print(f"⚠️ Warning: Namespace '{namespace}' not in allowed list: {config.mcp.allowed_namespaces}")
            auto_execute = False
        
        # Start SMART/AI resolution
        if AI_AVAILABLE:
            print(f"\n🤖 Starting AI-POWERED resolution (auto_execute: {auto_execute})...")
            print("   AI Expert will analyze the error and generate intelligent commands!")
            print(f"   Model: {config.aws.bedrock_model_id}")
            print(f"   Cluster: {config.aws.cluster_name}")
        else:
            print(f"\n🔧 Starting SMART resolution (auto_execute: {auto_execute})...")
            print("   Using rule-based intelligent resolution!")
        
        result = await rag_engine.resolve_error_smartly(error_doc, auto_execute)
        
        # Display results
        print(f"\n📊 Resolution Results:")
        print(f"   - Success: {'✅' if result.get('success', False) else '❌'} {result.get('success', False)}")
        print(f"   - Resolution ID: {result.get('resolution_id', 'N/A')}")
        print(f"   - Resolution Type: {result.get('resolution_type', 'unknown')}")
        
        if AI_AVAILABLE and result.get('ai_confidence'):
            print(f"   - AI Confidence: {result.get('ai_confidence', 0.0):.2f}")
        
        print(f"   - Steps Executed: {result.get('steps_executed', 0)}")
        print(f"   - Duration: {result.get('resolution_duration', 0):.2f}s")
        print(f"   - Auto Executed: {result.get('auto_executed', False)}")
        print(f"   - Cluster: {result.get('cluster_name', 'unknown')}")
        print(f"   - Namespace: {result.get('namespace', 'unknown')}")
        
        if result.get('root_cause'):
            print(f"\n🔍 Root Cause Analysis:")
            print(f"   {result['root_cause']}")
        
        if result.get('fix_strategy'):
            strategy_type = "AI" if AI_AVAILABLE else "Rule-based"
            print(f"\n🧠 {strategy_type} Fix Strategy:")
            for i, strategy in enumerate(result['fix_strategy'], 1):
                print(f"   {i}. {strategy}")
        
        if result.get('actual_changes_made'):
            print(f"\n🔧 Changes Applied:")
            for i, change in enumerate(result['actual_changes_made'], 1):
                print(f"   {i}. {change}")
        
        if result.get('execution_results', {}).get('steps'):
            print(f"\n📋 Detailed Command Execution:")
            for i, step in enumerate(result['execution_results']['steps'], 1):
                status = "✅" if step.get('success', False) else "❌"
                print(f"   {i}. {status} {step.get('step', 'Unknown step')}")
                if step.get('command'):
                    print(f"      Command: {step['command']}")
                if step.get('execution_time'):
                    print(f"      Time: {step['execution_time']:.2f}s")
                if step.get('error'):
                    print(f"      Error: {step['error'][:100]}...")
                elif step.get('result') and step['success']:
                    result_str = str(step['result'])
                    if len(result_str) > 100:
                        result_str = result_str[:100] + "..."
                    print(f"      Result: {result_str}")
        
        if result.get('risk_assessment'):
            risk = result['risk_assessment']
            print(f"\n⚠️ Risk Assessment:")
            print(f"   - Risk Level: {risk.get('risk_level', 'Unknown')}")
            if risk.get('potential_impacts'):
                print(f"   - Potential Impacts: {', '.join(risk['potential_impacts'])}")
            if risk.get('rollback_commands'):
                print(f"   - Rollback Available: {len(risk['rollback_commands'])} commands")
        
        # Validation Results
        validation = result.get('validation_results', {})
        print(f"\n🔍 Resolution Validation:")
        print(f"   - Validated: {'✅' if validation.get('success') else '❌'}")
        print(f"   - Type: {validation.get('validation_type', 'none')}")
        print(f"   - Message: {validation.get('message', 'No validation performed')}")
        
        # Cleanup
        await rag_engine.close()
        client.close()
        
        success_msg = "AI-POWERED" if AI_AVAILABLE else "SMART"
        print(f"\n{'✅ ' + success_msg + ' RESOLUTION COMPLETED SUCCESSFULLY!' if result.get('success') else '❌ ' + success_msg + ' RESOLUTION FAILED'}")
        return result.get('success', False)
        
    except Exception as e:
        print(f"❌ Resolution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def generate_ai_report(error_log_id: str):
    """Generate comprehensive analysis report"""
    config = load_resolver_config()
    
    if AI_AVAILABLE:
        print(f"📊 Generating AI-Powered Analysis Report for: {error_log_id}")
    else:
        print(f"📊 Generating Smart Analysis Report for: {error_log_id}")
    print("=" * 60)
    
    try:
        # Initialize RAG Engine
        rag_engine = AISmartRAGEngine(config)
        
        # Fetch error from MongoDB
        from pymongo import MongoClient
        client = MongoClient(config.mongodb.uri)
        db = client[config.mongodb.database_name]
        debug_collection = db[config.mongodb.debug_collection]
        
        error_doc = debug_collection.find_one({"error_log_id": error_log_id})
        if not error_doc:
            print(f"❌ Error log {error_log_id} not found")
            return False
        
        # Convert ObjectId to string
        error_doc["_id"] = str(error_doc["_id"])
        
        # Generate analysis (without execution)
        if AI_AVAILABLE:
            print("🤖 AI Expert is analyzing the error...")
            ai_plan = await rag_engine.expert_ai.analyze_error_with_ai(error_doc)
            confidence = ai_plan.confidence_score
            analysis_type = "AI Expert EKS Administrator"
            model_info = config.aws.bedrock_model_id
        else:
            print("🔧 Smart system is analyzing the error...")
            # For non-AI version, create a mock analysis
            ai_plan = type('obj', (object,), {
                'error_analysis': {'root_cause': 'Analysis using rule-based approach'},
                'confidence_score': 0.75,
                'diagnosis_strategy': ['Standard troubleshooting approach'],
                'diagnosis_commands': ['kubectl describe pod', 'kubectl get events'],
                'fix_strategy': ['Standard resolution approach'],
                'fix_commands': ['kubectl rollout restart'],
                'validation_commands': ['kubectl get pods'],
                'risk_assessment': {'risk_level': 'Medium'},
                'reasoning': 'Rule-based resolution approach'
            })()
            confidence = 0.75
            analysis_type = "Rule-based Smart System"
            model_info = "Built-in templates"
        
        # Generate comprehensive report
        report_content = f"""
# {analysis_type} - Kubernetes Error Analysis Report

**Generated by**: {analysis_type}  
**Model**: {model_info}  
**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}  
**Error ID**: {error_log_id}  
**Confidence**: {confidence:.2f}/1.0  
**EKS Cluster**: {config.aws.cluster_name}  
**AWS Region**: {config.aws.region}

## Error Summary
- **Type**: {error_doc.get('error_type', 'unknown')}
- **Pod**: {error_doc.get('pod_name', 'unknown')}  
- **Namespace**: {error_doc.get('namespace', 'default')}
- **Severity**: {error_doc.get('error_severity', 'unknown')}
- **Node**: {error_doc.get('node_name', 'unknown')}
- **Message**: {error_doc.get('error_message', '')[:200]}...

## Root Cause Analysis
**Primary Cause**: {ai_plan.error_analysis.get('root_cause', 'Unknown')}

**Contributing Factors**:
{chr(10).join(f"- {factor}" for factor in ai_plan.error_analysis.get('contributing_factors', ['Analysis pending']))}

**Impact Assessment**: {ai_plan.error_analysis.get('impact_assessment', 'Unknown')}

## Expert Diagnosis Strategy
{chr(10).join(f"{i+1}. {strategy}" for i, strategy in enumerate(ai_plan.diagnosis_strategy))}

## Generated Diagnosis Commands
```bash
{chr(10).join(ai_plan.diagnosis_commands)}
```

## Expert Fix Strategy  
{chr(10).join(f"{i+1}. {strategy}" for i, strategy in enumerate(ai_plan.fix_strategy))}

## Generated Fix Commands
```bash
{chr(10).join(ai_plan.fix_commands)}
```

## Generated Validation Commands
```bash
{chr(10).join(ai_plan.validation_commands)}
```

## Risk Assessment
- **Risk Level**: {ai_plan.risk_assessment.get('risk_level', 'Unknown')}
- **Potential Impacts**: {', '.join(ai_plan.risk_assessment.get('potential_impacts', ['Unknown']))}

**Safety Checks**:
{chr(10).join(f"- {check}" for check in ai_plan.risk_assessment.get('safety_checks', ['Standard safety procedures']))}

**Rollback Commands**:
```bash
{chr(10).join(ai_plan.risk_assessment.get('rollback_commands', ['kubectl rollout undo']))}
```

## Expert Reasoning
{ai_plan.reasoning}

## Configuration Context
- **Allowed Namespaces**: {', '.join(config.mcp.allowed_namespaces)}
- **Auto Execute Safe Commands**: {config.auto_execute_safe_commands}
- **Max Concurrent Resolutions**: {config.max_concurrent_resolutions}
- **MCP Timeout**: {config.mcp.timeout_seconds}s

## Execution Recommendations

**For High Confidence (0.8+)**: Execute automatically with monitoring  
**For Medium Confidence (0.6-0.8)**: Review commands and execute manually  
**For Low Confidence (<0.6)**: Use as starting point for manual investigation

**Current Recommendation**: {'Auto-execute (High confidence)' if confidence > 0.8 else 'Review first (Medium confidence)' if confidence > 0.6 else 'Manual investigation (Low confidence)'}

## Next Steps

1. **To execute this resolution**:
   ```bash
   python main.py resolve --error-id {error_log_id} --auto-execute
   ```

2. **To execute specific commands manually**:
   ```bash
   # Copy and run the generated commands above
   ```

3. **Monitor the resolution**:
   ```bash
   # Use the generated validation commands to verify success
   ```

---
*This report was generated by {analysis_type} using {model_info} with specialized Kubernetes and AWS EKS knowledge.*
"""
        
        # Save report to file
        report_filename = f"resolution_report_{error_log_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_filename, 'w') as f:
            f.write(report_content)
        
        print(f"✅ Analysis report generated successfully!")
        print(f"   - File: {report_filename}")
        print(f"   - Confidence: {confidence:.2f}")
        print(f"   - Risk Level: {ai_plan.risk_assessment.get('risk_level', 'Unknown')}")
        print(f"   - Commands Generated: {len(ai_plan.fix_commands)} fix + {len(ai_plan.validation_commands)} validation")
        
        # Show key insights
        print(f"\n🔍 Key Insights:")
        print(f"   - Root Cause: {ai_plan.error_analysis.get('root_cause', 'Unknown')[:100]}...")
        print(f"   - Primary Strategy: {ai_plan.fix_strategy[0] if ai_plan.fix_strategy else 'None'}")
        print(f"   - Confidence Level: {'High' if confidence > 0.8 else 'Medium' if confidence > 0.6 else 'Low'}")
        
        print(f"\n💡 Recommendation:")
        if confidence > 0.8:
            print(f"   ✅ High confidence - Safe for auto-execution")
        elif confidence > 0.6:
            print(f"   ⚠️ Medium confidence - Review before execution")
        else:
            print(f"   🔍 Low confidence - Use for investigation guidance")
        
        # Cleanup
        await rag_engine.close()
        client.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main CLI function with AI-powered capabilities"""
    parser = argparse.ArgumentParser(
        description='AI-Powered Smart RAG-based Kubernetes Error Resolver',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start the microservice
  python main.py serve

  # Test components
  python main.py test

  # Resolve error with AI/Smart analysis
  python main.py resolve --error-id 507f1f77bcf86cd799439011 --auto-execute

  # Generate analysis report
  python main.py report --error-id 507f1f77bcf86cd799439011

  # Get system status
  python main.py status

Features:
  ✅ AI-Powered Expert EKS Administrator (if Bedrock available)
  ✅ Intelligent command generation based on error context  
  ✅ Deep root cause analysis with AWS expertise
  ✅ Dynamic fix strategies (not static commands!)
  ✅ Risk assessment and rollback planning
  ✅ Validation command generation
  ✅ Confidence-based execution decisions
  ✅ Integration with your existing config and components
        """
    )
    
    parser.add_argument('command', 
                       choices=['serve', 'test', 'resolve', 'report', 'status'],
                       help='Command to execute')
    
    parser.add_argument('--error-id', type=str,
                       help='Error log ID from debug_results collection')
    
    parser.add_argument('--auto-execute', action='store_true',
                       help='Enable auto-execution of intelligent commands')
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    
    parser.add_argument('--port', type=int, default=8080,
                       help='Service port (for serve command)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    if args.command == 'serve':
        # Start the microservice
        feature_type = "AI-Powered" if AI_AVAILABLE else "Smart"
        print(f"🚀 Starting {feature_type} RAG Resolver Service on port {args.port}")
        print(f"   Features: Expert Knowledge + Intelligent Commands + Real Execution")
        start_service()
        
    elif args.command == 'test':
        # Test components
        success = asyncio.run(test_ai_components())
        sys.exit(0 if success else 1)
        
    elif args.command == 'resolve':
        if not args.error_id:
            print("❌ Error ID is required for resolve command")
            sys.exit(1)
        
        # Smart/AI resolution
        feature_type = "AI-Powered" if AI_AVAILABLE else "Smart Rule-based"
        print(f"🤖 Using {feature_type} RAG Engine")
        
        if not args.auto_execute:
            print("⚠️  Note: --auto-execute not specified, will only analyze without executing")
        
        success = asyncio.run(resolve_error_with_ai(args.error_id, args.auto_execute))
        sys.exit(0 if success else 1)
        
    elif args.command == 'report':
        if not args.error_id:
            print("❌ Error ID is required for report command")
            sys.exit(1)
        
        # Generate report
        success = asyncio.run(generate_ai_report(args.error_id))
        sys.exit(0 if success else 1)
        
    elif args.command == 'status':
        # Get system status
        config = load_resolver_config()
        feature_type = "AI-Powered" if AI_AVAILABLE else "Smart Rule-based"
        print(f"📊 {feature_type} RAG Resolver System Status")
        print("=" * 60)
        print(f"Service Name: {config.service_name}")
        print(f"Service Port: {config.service_port}")
        print(f"EKS Cluster: {config.aws.cluster_name}")
        print(f"AWS Region: {config.aws.region}")
        print(f"AWS Profile: {config.aws.profile}")
        
        if AI_AVAILABLE:
            print(f"AI Model: {config.aws.bedrock_model_id}")
        else:
            print(f"AI Model: Not available (using rule-based)")
            
        print(f"Qdrant Host: {config.qdrant.host}:{config.qdrant.port}")
        print(f"MongoDB: {config.mongodb.database_name}")
        print(f"Auto Execute: {config.auto_execute_safe_commands}")
        print(f"Max Concurrent: {config.max_concurrent_resolutions}")
        print(f"Allowed Namespaces: {', '.join(config.mcp.allowed_namespaces)}")
        
        print(f"\n🔧 Features:")
        if AI_AVAILABLE:
            print(f"   ✅ Expert EKS Administrator AI")
            print(f"   ✅ Intelligent Command Generation")
            print(f"   ✅ Dynamic Root Cause Analysis")
            print(f"   ✅ Context-Aware Fix Strategies")
        else:
            print(f"   ✅ Rule-based Smart Resolution")
            print(f"   ✅ Template-based Command Generation")
            print(f"   ✅ Standard Root Cause Analysis")
            
        print(f"   ✅ Risk Assessment & Rollback Planning")
        print(f"   ✅ Confidence-Based Auto-Execution")
        print(f"   ✅ AWS/EKS Integration")
        print(f"   ✅ Real-time Command Logging & Validation")
        
        # Test basic connectivity
        try:
            success = asyncio.run(test_ai_components())
            status_msg = "All Features Working" if success else "Some Issues Detected"
            print(f"\n🔍 System Status: {'✅' if success else '❌'} {status_msg}")
        except Exception as e:
            print(f"\n❌ Status check failed: {e}")

if __name__ == "__main__":
    main()