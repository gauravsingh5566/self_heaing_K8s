#!/usr/bin/env python3
"""
Main entry point for AI analyzer
Provides CLI interface and main execution logic
"""

import json
import logging
import sys
import argparse
from datetime import datetime
from typing import Dict, Any

from .manager import KubernetesErrorAnalysisManager
from .config import load_analyzer_config

# Setup logging
def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file logging if possible
    try:
        import os
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        file_handler = logging.FileHandler('logs/ai_analyzer.log')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
    except Exception:
        pass  # Continue without file logging if it fails

logger = logging.getLogger(__name__)

def print_results(results: Dict[str, Any], title: str):
    """Pretty print results with formatting"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")
    
    if isinstance(results, dict):
        # Print summary information first
        if "processed" in results:
            print(f"📊 Processed: {results.get('processed', 0)}")
            print(f"❌ Failed: {results.get('failed', 0)}")
            if results.get('duration_seconds'):
                print(f"⏱️  Duration: {results['duration_seconds']}s")
            if results.get('success_rate'):
                print(f"✅ Success Rate: {results['success_rate']:.1f}%")
            print(f"💬 Message: {results.get('message', 'N/A')}")
        
        # Print other relevant information
        if "error_type" in results:
            print(f"🔍 Error Type: {results['error_type']}")
        
        if "namespace" in results:
            print(f"📁 Namespace: {results['namespace']}")
        
        if "health_status" in results:
            status_emoji = {"HEALTHY": "✅", "DEGRADED": "⚠️", "WARNING": "🔶", "CRITICAL": "🚨"}.get(results["health_status"], "❓")
            print(f"{status_emoji} Health Status: {results['health_status']}")
        
        # Print recommendations if available
        if "recommendations" in results and results["recommendations"]:
            print(f"\n📋 Recommendations:")
            for i, rec in enumerate(results["recommendations"][:5], 1):
                print(f"   {i}. {rec}")
        
        # Print summary statistics if available
        if "summary" in results:
            summary = results["summary"]
            print(f"\n📈 Summary Statistics:")
            print(f"   Total Errors: {summary.get('total_errors', 0)}")
            print(f"   Total Analyzed: {summary.get('total_analyzed', 0)}")
            print(f"   Coverage: {summary.get('coverage_percentage', 0):.1f}%")
            print(f"   Resolution Rate: {summary.get('resolution_rate', 0):.1f}%")
    
    # Print JSON for detailed analysis (truncated)
    print(f"\n📄 Detailed Results:")
    results_copy = dict(results)
    if "results" in results_copy and len(results_copy["results"]) > 3:
        results_copy["results"] = results_copy["results"][:3]
        results_copy["results_truncated"] = f"... and {len(results['results']) - 3} more"
    
    print(json.dumps(results_copy, indent=2, default=str))

def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description='Kubernetes Error Analysis with AI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze all unprocessed errors
  python -m ai_analyzer.main --mode all

  # Analyze specific error type  
  python -m ai_analyzer.main --mode specific --error-type image_pull

  # Generate comprehensive report
  python -m ai_analyzer.main --mode report

  # Analyze critical errors only
  python -m ai_analyzer.main --mode critical --hours 2

  # Check system health
  python -m ai_analyzer.main --mode health

  # Test system components
  python -m ai_analyzer.main --mode test
        """
    )
    
    parser.add_argument('--mode', 
                       choices=['all', 'specific', 'critical', 'namespace', 'report', 'health', 'test', 'cleanup'],
                       default='all',
                       help='Analysis mode (default: all)')
    
    parser.add_argument('--hours', type=int, default=1,
                       help='Hours back to analyze (default: 1)')
    
    parser.add_argument('--batch-size', type=int, default=20,
                       help='Batch size for processing (default: 20)')
    
    parser.add_argument('--error-type', type=str,
                       help='Specific error type to analyze (for specific mode)')
    
    parser.add_argument('--namespace', type=str,
                       help='Namespace to analyze (for namespace mode)')
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level (default: INFO)')
    
    parser.add_argument('--json-output', action='store_true',
                       help='Output results in JSON format only')
    
    parser.add_argument('--cleanup-days', type=int, default=30,
                       help='Days old for cleanup (default: 30)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    try:
        logger.info(f"Starting AI analyzer in {args.mode} mode")
        
        # Initialize manager
        config = load_analyzer_config()
        manager = KubernetesErrorAnalysisManager(config)
        
        # Execute based on mode
        if args.mode == 'all':
            results = manager.analyze_all_unprocessed_errors(
                hours_back=args.hours,
                batch_size=args.batch_size
            )
            title = f"All Unprocessed Errors Analysis (Last {args.hours}h)"
            
        elif args.mode == 'specific':
            if not args.error_type:
                logger.error("Error type must be specified for specific mode (use --error-type)")
                return 1
            
            results = manager.analyze_specific_error_type(
                args.error_type,
                hours_back=args.hours
            )
            title = f"{args.error_type.upper()} Error Analysis (Last {args.hours}h)"
            
        elif args.mode == 'critical':
            results = manager.analyze_critical_errors_only(hours_back=args.hours)
            title = f"Critical Errors Analysis (Last {args.hours}h)"
            
        elif args.mode == 'namespace':
            if not args.namespace:
                logger.error("Namespace must be specified for namespace mode (use --namespace)")
                return 1
            
            results = manager.analyze_namespace_errors(
                args.namespace,
                hours_back=args.hours
            )
            title = f"Namespace '{args.namespace}' Analysis (Last {args.hours}h)"
            
        elif args.mode == 'report':
            results = manager.get_comprehensive_report()
            title = "Comprehensive Error Analysis Report"
            
        elif args.mode == 'health':
            results = manager.get_system_health_status()
            title = "System Health Status"
            
        elif args.mode == 'test':
            results = manager.test_system_components()
            title = "System Component Tests"
            
        elif args.mode == 'cleanup':
            results = manager.cleanup_old_data(days_old=args.cleanup_days)
            title = f"Data Cleanup (>{args.cleanup_days} days old)"
        
        # Output results
        if args.json_output:
            print(json.dumps(results, indent=2, default=str))
        else:
            print_results(results, title)
        
        # Determine exit code based on results
        exit_code = 0
        if isinstance(results, dict):
            if "failed" in results and results["failed"] > 0:
                exit_code = 1
            elif "status" in results and results["status"] == "FAILED":
                exit_code = 1
            elif "overall_status" in results and results["overall_status"] == "FAILED":
                exit_code = 1
            elif "health_status" in results and results["health_status"] == "CRITICAL":
                logger.warning("System is in CRITICAL state")
                # Don't exit with error for critical health, just warn
        
        if exit_code == 0:
            logger.info("AI analyzer completed successfully")
        else:
            logger.warning("AI analyzer completed with issues")
        
        # Cleanup
        manager.close()
        return exit_code
        
    except Exception as e:
        logger.error(f"AI analyzer failed: {e}")
        if args.log_level == 'DEBUG':
            import traceback
            traceback.print_exc()
        return 1

def run_specific_error_analysis():
    """Function to analyze all specific error types - can be called separately"""
    try:
        manager = KubernetesErrorAnalysisManager()
        
        # Define error types to analyze
        error_types = ["image_pull", "resource_limit", "network", "container", "node", "storage", "auth", "application"]
        
        all_results = {}
        
        for error_type in error_types:
            try:
                print(f"\n🔍 Analyzing {error_type.upper()} errors...")
                results = manager.analyze_specific_error_type(error_type, hours_back=48)
                all_results[error_type] = results
                
                if results['processed'] > 0:
                    print(f"✅ {error_type}: {results['processed']} processed")
                else:
                    print(f"ℹ️  {error_type}: No errors found")
                    
            except Exception as e:
                logger.error(f"Failed to analyze {error_type} errors: {e}")
                all_results[error_type] = {"error": str(e), "processed": 0}
        
        # Print summary
        print(f"\n{'='*60}")
        print(" SPECIFIC ERROR TYPE ANALYSIS SUMMARY")
        print(f"{'='*60}")
        
        total_processed = sum(r.get('processed', 0) for r in all_results.values())
        successful_types = len([r for r in all_results.values() if r.get('processed', 0) > 0])
        
        print(f"📊 Total Processed: {total_processed}")
        print(f"🎯 Active Error Types: {successful_types}/{len(error_types)}")
        
        for error_type, results in all_results.items():
            if results.get('processed', 0) > 0:
                print(f"   • {error_type}: {results['processed']} errors")
        
        manager.close()
        return 0
        
    except Exception as e:
        logger.error(f"Specific error analysis failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)