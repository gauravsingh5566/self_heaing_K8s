#!/usr/bin/env python3
"""
Updated script to run the multi-file AI analyzer for Kubernetes errors
Can be run as a cron job or scheduled task
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import from the new multi-file structure
from ai_analyzer import KubernetesErrorAnalysisManager, load_analyzer_config

# Setup logging
def setup_logging():
    """Setup logging with both file and console output"""
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/ai_analyzer_scheduler.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    
    parser = argparse.ArgumentParser(description='Run Kubernetes Error Analysis (Multi-file Version)')
    parser.add_argument('--mode', 
                       choices=['all', 'specific', 'critical', 'namespace', 'report', 'health'],
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
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        logger.info(f"Starting AI analyzer in {args.mode} mode (multi-file version)")
        
        # Load configuration
        config = load_analyzer_config()
        logger.info(f"Loaded configuration: {config.mongodb.database_name}/{config.mongodb.logs_collection}")
        
        # Initialize manager
        manager = KubernetesErrorAnalysisManager(config)
        
        # Execute based on mode
        if args.mode == 'all':
            logger.info(f"Analyzing all unprocessed errors from last {args.hours} hours")
            results = manager.analyze_all_unprocessed_errors(
                hours_back=args.hours, 
                batch_size=args.batch_size
            )
            logger.info(f"✅ Processed {results['processed']} errors")
            
            if results['processed'] > 0:
                logger.info("🔍 Example processed errors:")
                for i, result in enumerate(results.get('results', [])[:3], 1):
                    error_type = result.get('error_type', 'unknown')
                    namespace = result.get('namespace', 'unknown')
                    pod_name = result.get('pod_name', 'unknown')
                    logger.info(f"   {i}. {error_type} in {namespace}/{pod_name}")
            else:
                logger.info("ℹ️  No new errors to process")
        
        elif args.mode == 'specific':
            if not args.error_type:
                logger.error("❌ Error type must be specified for specific mode")
                return 1
            
            logger.info(f"Analyzing {args.error_type} errors from last {args.hours} hours")
            results = manager.analyze_specific_error_type(
                args.error_type, 
                hours_back=args.hours
            )
            logger.info(f"✅ Processed {results['processed']} {args.error_type} errors")
        
        elif args.mode == 'critical':
            logger.info(f"Analyzing critical errors from last {args.hours} hours")
            results = manager.analyze_critical_errors_only(hours_back=args.hours)
            logger.info(f"✅ Processed {results['processed']} critical errors")
            
            if results['processed'] > 0:
                logger.warning(f"🚨 {results['processed']} critical errors were analyzed - review immediately!")
        
        elif args.mode == 'namespace':
            if not args.namespace:
                logger.error("❌ Namespace must be specified for namespace mode")
                return 1
            
            logger.info(f"Analyzing errors in namespace '{args.namespace}' from last {args.hours} hours")
            results = manager.analyze_namespace_errors(
                args.namespace, 
                hours_back=args.hours
            )
            logger.info(f"✅ Processed {results['processed']} errors in {args.namespace}")
        
        elif args.mode == 'report':
            logger.info("Generating comprehensive error analysis report")
            results = manager.get_comprehensive_report()
            
            summary = results.get('summary', {})
            logger.info("📊 Comprehensive Report Summary:")
            logger.info(f"   Total Errors: {summary.get('total_errors', 0)}")
            logger.info(f"   Total Analyzed: {summary.get('total_analyzed', 0)} ({summary.get('coverage_percentage', 0):.1f}% coverage)")
            logger.info(f"   Resolution Rate: {summary.get('resolution_rate', 0):.1f}%")
            
            # Log recommendations
            recommendations = results.get('recommendations', [])
            if recommendations:
                logger.info("💡 Top Recommendations:")
                for i, rec in enumerate(recommendations[:3], 1):
                    logger.info(f"   {i}. {rec}")
        
        elif args.mode == 'health':
            logger.info("Checking system health status")
            results = manager.get_system_health_status()
            
            health_status = results.get('health_status', 'UNKNOWN')
            status_emoji = {"HEALTHY": "✅", "DEGRADED": "⚠️", "WARNING": "🔶", "CRITICAL": "🚨"}.get(health_status, "❓")
            
            logger.info(f"{status_emoji} System Health: {health_status}")
            logger.info(f"   Recent Errors: {results.get('total_recent_errors', 0)}")
            logger.info(f"   Critical Errors: {results.get('critical_errors_count', 0)}")
            logger.info(f"   Pending High Priority: {results.get('pending_high_priority', 0)}")
            
            # Log health recommendations
            health_recs = results.get('system_recommendations', [])
            if health_recs:
                logger.info("🏥 Health Recommendations:")
                for rec in health_recs:
                    logger.info(f"   • {rec}")
        
        # Log execution summary
        if 'duration_seconds' in results:
            logger.info(f"⏱️  Execution completed in {results['duration_seconds']}s")
        
        # Close manager
        manager.close()
        logger.info("🔒 AI analyzer completed successfully")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ AI analyzer failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

def run_all_error_types():
    """Function to analyze all error types in sequence"""
    logger = setup_logging()
    
    try:
        logger.info("🚀 Starting comprehensive error type analysis")
        
        manager = KubernetesErrorAnalysisManager()
        
        # Define error types to analyze
        error_types = ["image_pull", "resource_limit", "network", "container", "node", "storage", "auth", "application"]
        
        total_processed = 0
        successful_types = 0
        
        for error_type in error_types:
            try:
                logger.info(f"🔍 Analyzing {error_type} errors...")
                results = manager.analyze_specific_error_type(error_type, hours_back=48)
                
                processed = results.get('processed', 0)
                total_processed += processed
                
                if processed > 0:
                    successful_types += 1
                    logger.info(f"✅ {error_type}: {processed} errors processed")
                else:
                    logger.info(f"ℹ️  {error_type}: No errors found")
                    
            except Exception as e:
                logger.error(f"❌ Failed to analyze {error_type} errors: {e}")
        
        # Final summary
        logger.info("📊 Comprehensive Analysis Summary:")
        logger.info(f"   Total Processed: {total_processed} errors")
        logger.info(f"   Active Error Types: {successful_types}/{len(error_types)}")
        logger.info(f"   Analysis Coverage: {(successful_types/len(error_types)*100):.1f}%")
        
        manager.close()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Comprehensive analysis failed: {e}")
        return 1

def run_health_check():
    """Function to run a quick health check"""
    logger = setup_logging()
    
    try:
        logger.info("🏥 Running system health check")
        
        manager = KubernetesErrorAnalysisManager()
        
        # Test system components
        test_results = manager.test_system_components()
        overall_status = test_results.get('overall_status', 'UNKNOWN')
        
        logger.info(f"🔧 Component Test Results: {overall_status}")
        
        for component, result in test_results.get('components', {}).items():
            status_emoji = "✅" if result['status'] == 'OK' else "❌"
            logger.info(f"   {status_emoji} {component}: {result['message']}")
        
        # Get health status
        health_results = manager.get_system_health_status()
        health_status = health_results.get('health_status', 'UNKNOWN')
        status_emoji = {"HEALTHY": "✅", "DEGRADED": "⚠️", "WARNING": "🔶", "CRITICAL": "🚨"}.get(health_status, "❓")
        
        logger.info(f"{status_emoji} Overall Health: {health_status}")
        
        manager.close()
        
        # Return appropriate exit code
        if overall_status == 'FAILED' or health_status == 'CRITICAL':
            return 1
        return 0
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return 1

if __name__ == "__main__":
    import sys
    
    # Check for special commands
    if len(sys.argv) > 1:
        if sys.argv[1] == "all-types":
            # Run comprehensive error type analysis
            exit_code = run_all_error_types()
            sys.exit(exit_code)
        elif sys.argv[1] == "health-check":
            # Run health check only
            exit_code = run_health_check()
            sys.exit(exit_code)
    
    # Run normal CLI
    exit_code = main()
    sys.exit(exit_code)#!/usr/bin/env python3
"""
Script to run the AI analyzer for Kubernetes errors
Can be run as a cron job or scheduled task
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from ai_analyzer.app import KubernetesErrorAnalysisManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ai_analyzer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Run Kubernetes Error Analysis')
    parser.add_argument('--mode', choices=['all', 'specific', 'report'], default='all',
                       help='Analysis mode: all errors, specific types, or report only')
    parser.add_argument('--hours', type=int, default=1,
                       help='Hours back to analyze (default: 1)')
    parser.add_argument('--batch-size', type=int, default=20,
                       help='Batch size for processing (default: 20)')
    parser.add_argument('--error-type', type=str,
                       help='Specific error type to analyze (for specific mode)')
    
    args = parser.parse_args()
    
    try:
        logger.info(f"Starting AI analyzer in {args.mode} mode...")
        manager = KubernetesErrorAnalysisManager()
        
        if args.mode == 'all':
            # Analyze all unprocessed errors
            results = manager.analyze_all_unprocessed_errors(
                hours_back=args.hours, 
                batch_size=args.batch_size
            )
            logger.info(f"Processed {results['processed']} errors")
            
            if results['processed'] > 0:
                logger.info("✅ AI analysis completed successfully")
                
                # Log some examples of processed errors
                for i, result in enumerate(results.get('results', [])[:3]):
                    logger.info(f"Example {i+1}: {result['error_type']} error in {result['namespace']}/{result['pod_name']}")
            else:
                logger.info("No new errors to process")
        
        elif args.mode == 'specific':
            if not args.error_type:
                logger.error("Error type must be specified for specific mode")
                return 1
                
            results = manager.analyze_specific_error_type(
                args.error_type, 
                hours_back=args.hours
            )
            logger.info(f"Processed {results['processed']} {args.error_type} errors")
        
        elif args.mode == 'report':
            # Generate comprehensive report
            report = manager.get_comprehensive_report()
            logger.info("Comprehensive Error Analysis Report")
            logger.info("=" * 40)
            
            coverage = report.get('coverage', {})
            logger.info(f"Total errors: {coverage.get('total_errors', 0)}")
            logger.info(f"Analyzed: {coverage.get('total_analyzed', 0)} ({coverage.get('coverage_percentage', 0):.1f}%)")
            
            # Log top error types
            error_stats = report.get('error_statistics', [])[:5]
            if error_stats:
                logger.info("Top error types:")
                for stat in error_stats:
                    error_info = stat['_id']
                    logger.info(f"  {error_info.get('error_type', 'unknown')} ({error_info.get('error_severity', 'unknown')}): {stat['count']}")
            
            # Log recommendations
            recommendations = report.get('recommendations', [])
            if recommendations:
                logger.info("Recommendations:")
                for i, rec in enumerate(recommendations, 1):
                    logger.info(f"  {i}. {rec}")
        
        logger.info("AI analyzer completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"AI analyzer failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)