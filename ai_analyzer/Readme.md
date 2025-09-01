# Multi-file AI Analyzer for Kubernetes Errors

A comprehensive, modular AI-powered error analysis system for Kubernetes with clean architecture and enhanced maintainability.

## 🏗️ Architecture Overview

```
ai_analyzer/
├── __init__.py          # Package initialization and exports
├── config.py           # Configuration management and AI prompts
├── fetcher.py          # Error log fetching from MongoDB
├── analyzer.py         # AI analysis using AWS Bedrock  
├── storage.py          # Debug results storage management
├── manager.py          # Main orchestration and workflows
└── main.py             # CLI interface and entry point
```

## ✨ Key Improvements Over Single-file Version

### **Modular Architecture**
- **Separation of Concerns**: Each module has a specific responsibility
- **Easy Testing**: Individual components can be tested in isolation
- **Better Maintainability**: Changes to one component don't affect others
- **Code Reusability**: Components can be used independently

### **Enhanced Features**
- **8+ Error Type Support**: Comprehensive error categorization
- **Specialized AI Prompts**: Tailored responses for each error type
- **Batch Processing**: Efficient handling of multiple errors
- **Priority-based Analysis**: Critical errors processed first
- **Health Monitoring**: System health and performance tracking
- **Comprehensive Reporting**: Detailed analytics and trends

## 🚀 Usage Examples

### **Basic Analysis**
```bash
# Analyze all unprocessed errors
python run_ai_analyzer.py --mode all --hours 2

# Analyze specific error type
python run_ai_analyzer.py --mode specific --error-type image_pull --hours 24

# Analyze critical errors only
python run_ai_analyzer.py --mode critical --hours 1
```

### **Advanced Analysis**
```bash
# Namespace-specific analysis
python run_ai_analyzer.py --mode namespace --namespace production --hours 12

# Comprehensive report generation
python run_ai_analyzer.py --mode report

# System health check
python run_ai_analyzer.py --mode health
```

### **Special Commands**
```bash
# Analyze all error types in sequence
python run_ai_analyzer.py all-types

# Quick health check only
python run_ai_analyzer.py health-check
```

### **Direct Module Usage**
```python
# Use as Python module
from ai_analyzer import KubernetesErrorAnalysisManager

manager = KubernetesErrorAnalysisManager()
results = manager.analyze_all_unprocessed_errors(hours_back=1)
print(f"Processed {results['processed']} errors")
```

## 📋 Detailed Module Functions

### **config.py**
```python
from ai_analyzer.config import load_analyzer_config, ERROR_PROMPTS

# Load configuration
config = load_analyzer_config()
print(f"Model: {config.aws.model_id}")

# Access error prompts
image_pull_prompt = ERROR_PROMPTS['image_pull']
```

### **fetcher.py**
```python
from ai_analyzer.fetcher import ErrorLogFetcher

fetcher = ErrorLogFetcher(config)

# Get unprocessed errors
errors = fetcher.fetch_unprocessed_errors(hours_back=2, limit=50)

# Get error statistics
stats = fetcher.get_error_statistics(hours_back=24)

# Get error trends
trends = fetcher.get_error_trends(hours_back=48)
```

### **analyzer.py**
```python
from ai_analyzer.analyzer import AIErrorAnalyzer

analyzer = AIErrorAnalyzer(config)

# Analyze single error
solution = analyzer.analyze_error(log_document)

# Batch analyze errors
results = analyzer.batch_analyze_errors(error_list)

# Get analysis statistics
stats = analyzer.get_analysis_stats()
```

### **storage.py**
```python
from ai_analyzer.storage import DebugResultsManager

storage = DebugResultsManager(config)

# Store analysis results
record = storage.store_debug_result(log_doc, ai_solution)

# Get debug summary
summary = storage.get_debug_summary(hours_back=24)

# Update result status
storage.update_result_status("log_id", "resolved", "Fixed by user")
```

### **manager.py**
```python
from ai_analyzer.manager import KubernetesErrorAnalysisManager

manager = KubernetesErrorAnalysisManager()

# Different analysis modes
results = manager.analyze_all_unprocessed_errors()
results = manager.analyze_specific_error_type("network")
results = manager.analyze_critical_errors_only()

# System monitoring
health = manager.get_system_health_status()
report = manager.get_comprehensive_report()

# Component testing
test_results = manager.test_system_components()
```

## 🔧 Configuration

### **Environment Variables**
```bash
# MongoDB Settings
export MONGO_URI="mongodb://admin:password@host:port/"
export DB_NAME="k8s_logs"
export COLLECTION_NAME="logs"
export DEBUG_COLLECTION="debug_results"

# AWS Bedrock Settings  
export AWS_REGION="us-west-2"
export AWS_PROFILE="finchat"
export MODEL_ID="openai.gpt-oss-20b-1:0"

# Analysis Settings
export ANALYSIS_BATCH_SIZE="20"
export MAX_TOKENS="1000"
export TEMPERATURE="0.3"
export HOURS_BACK_DEFAULT="1"
```

### **Programmatic Configuration**
```python
from ai_analyzer.config import AIAnalyzerConfig, MongoDBConfig, AWSConfig, AnalysisConfig

config = AIAnalyzerConfig(
    mongodb=MongoDBConfig(
        uri="mongodb://localhost:27017",
        database_name="k8s_logs",
        logs_collection="logs",
        debug_collection="debug_results"
    ),
    aws=AWSConfig(
        region="us-west-2",
        profile="default", 
        model_id="openai.gpt-oss-20b-1:0"
    ),
    analysis=AnalysisConfig(
        batch_size=25,
        max_tokens=1200,
        temperature=0.2,
        top_p=0.9,
        hours_back_default=2,
        critical_error_types=["node", "resource_limit", "storage"]
    )
)

manager = KubernetesErrorAnalysisManager(config)
```

## 📊 Output Examples

### **Analysis Results**
```json
{
  "processed": 15,
  "failed": 1,
  "success_rate": 93.8,
  "duration_seconds": 45.2,
  "message": "Analysis completed successfully",
  "results": [
    {
      "error_type": "image_pull",
      "namespace": "production",
      "pod_name": "api-server-xyz",
      "priority": 2,
      "status": "pending"
    }
  ]
}
```

### **Comprehensive Report**
```json
{
  "summary": {
    "total_errors": 127,
    "total_analyzed": 95,
    "coverage_percentage": 74.8,
    "resolution_rate": 23.4
  },
  "recommendations": [
    "🔴 High image pull errors (23 total, 15 critical). Consider implementing image registry caching.",
    "🟡 Network issues detected (8). Check service discovery and connectivity.",
    "✅ System appears stable with well-managed error patterns."
  ]
}
```

### **Health Status**
```json
{
  "health_status": "HEALTHY",
  "total_recent_errors": 12,
  "critical_errors_count": 2,
  "pending_high_priority": 3,
  "system_recommendations": [
    "✅ System is operating normally",
    "Continue regular monitoring and maintenance"
  ]
}
```

## 🔄 Automation & Scheduling

### **Cron Job Examples**
```bash
# Every 15 minutes - analyze new errors
*/15 * * * * /usr/bin/python3 /path/to/run_ai_analyzer.py --mode all --hours 1

# Every hour - analyze critical errors
0 * * * * /usr/bin/python3 /path/to/run_ai_analyzer.py --mode critical --hours 2

# Daily at 9 AM - comprehensive report
0 9 * * * /usr/bin/python3 /path/to/run_ai_analyzer.py --mode report > /var/log/k8s-analysis-daily.log

# Weekly cleanup - remove old data
0 2 * * 0 /usr/bin/python3 -c "from ai_analyzer import KubernetesErrorAnalysisManager; m=KubernetesErrorAnalysisManager(); m.cleanup_old_data(days_old=30)"
```

### **Kubernetes CronJob**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: k8s-error-analyzer
spec:
  schedule: "*/15 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: analyzer
            image: your-registry/k8s-analyzer:latest
            command: ["python3", "run_ai_analyzer.py", "--mode", "all", "--hours", "1"]
            env:
            - name: MONGO_URI
              valueFrom:
                secretKeyRef:
                  name: mongodb-secret
                  key: uri
            - name: AWS_PROFILE
              value: "finchat"
          restartPolicy: OnFailure
```

## 🧪 Testing & Debugging

### **Component Testing**
```python
# Test individual components
from ai_analyzer import KubernetesErrorAnalysisManager

manager = KubernetesErrorAnalysisManager()

# Test all components
test_results = manager.test_system_components()
print("MongoDB:", test_results["components"]["log_fetcher"]["status"])
print("Bedrock:", test_results["components"]["ai_analyzer"]["status"])

# Test specific component
from ai_analyzer.analyzer import AIErrorAnalyzer
analyzer = AIErrorAnalyzer(config)
connection_ok = analyzer.test_connection()
```

### **Debug Mode**
```bash
# Enable verbose logging
python run_ai_analyzer.py --mode all --verbose

# Check logs
tail -f logs/ai_analyzer_scheduler.log
tail -f logs/ai_analyzer.log
```

### **Manual Testing**
```python
# Test error detection
from ai_analyzer.fetcher import ErrorLogFetcher

fetcher = ErrorLogFetcher(config)
count = fetcher.get_unprocessed_count(hours_back=1)
print(f"Unprocessed errors: {count}")

# Test AI analysis
from ai_analyzer.analyzer import AIErrorAnalyzer
analyzer = AIErrorAnalyzer(config)

sample_log = {
    "error_type": "image_pull",
    "namespace": "test",
    "resource_name": "test-pod",
    "message": "Failed to pull image nginx:invalid",
    "metadata": {"node_name": "test-node"}
}

solution = analyzer.analyze_error(sample_log)
print("AI Solution:", solution[:200] + "...")
```

## 🚨 Monitoring & Alerts

### **Health Monitoring Script**
```python
#!/usr/bin/env python3
# health_monitor.py

from ai_analyzer import KubernetesErrorAnalysisManager
import sys

def main():
    manager = KubernetesErrorAnalysisManager()
    health = manager.get_system_health_status()
    
    status = health["health_status"]
    critical_count = health["critical_errors_count"]
    
    if status == "CRITICAL":
        print(f"🚨 ALERT: System CRITICAL - {critical_count} critical errors")
        sys.exit(2)
    elif status == "WARNING":
        print(f"⚠️  WARNING: System degraded - {critical_count} critical errors")
        sys.exit(1)
    else:
        print(f"✅ System healthy - {critical_count} critical errors")
        sys.exit(0)

if __name__ == "__main__":
    main()
```

### **Alertmanager Integration**
```bash
# Add to monitoring script
python health_monitor.py
if [ $? -eq 2 ]; then
  curl -X POST http://alertmanager:9093/api/v1/alerts \
    -d '[{"labels":{"alertname":"K8sAnalysisSystemCritical","severity":"critical"}}]'
fi
```

## 📚 Migration from Single-file

### **Old Usage → New Usage**
```python
# OLD (single file)
from ai_analyzer.app import KubernetesErrorAnalysisManager
manager = KubernetesErrorAnalysisManager()

# NEW (multi-file)
from ai_analyzer import KubernetesErrorAnalysisManager
manager = KubernetesErrorAnalysisManager()
```

### **Function Changes**
```python
# All public APIs remain the same
results = manager.analyze_all_unprocessed_errors()  # ✅ Same
results = manager.get_comprehensive_report()        # ✅ Same

# New enhanced features
health = manager.get_system_health_status()         # 🆕 New
tests = manager.test_system_components()            # 🆕 New
```

## 🔧 Troubleshooting

### **Common Issues**

**1. Module Import Errors**
```bash
# Make sure you're in the right directory
cd /path/to/kubernetes-log-tracker
python run_ai_analyzer.py --mode all
```

**2. Configuration Issues**
```python
# Test configuration loading
from ai_analyzer.config import load_analyzer_config
config = load_analyzer_config()
print(f"MongoDB URI: {config.mongodb.uri}")
print(f"AWS Region: {config.aws.region}")
```

**3. Component Failures**
```python
# Test each component
from ai_analyzer import KubernetesErrorAnalysisManager
manager = KubernetesErrorAnalysisManager()
tests = manager.test_system_components()

for component, result in tests["components"].items():
    if result["status"] != "OK":
        print(f"❌ {component}: {result['message']}")
```

### **Performance Optimization**

**1. Batch Size Tuning**
```bash
# For high-volume environments
python run_ai_analyzer.py --batch-size 50 --mode all

# For resource-constrained environments  
python run_ai_analyzer.py --batch-size 10 --mode all
```

**2. Memory Management**
```python
# Use context managers for automatic cleanup
from ai_analyzer import KubernetesErrorAnalysisManager

with KubernetesErrorAnalysisManager() as manager:
    results = manager.analyze_all_unprocessed_errors()
    # Automatic cleanup when done
```

## 📦 Dependencies

Update your `requirements.txt`:
```txt
# Existing dependencies
kubernetes==28.1.0
pymongo==4.6.0
boto3==1.34.0
botocore==1.34.0
python-dateutil==2.8.2

# Additional for multi-file structure
dataclasses-json>=0.6.0  # For enhanced configuration
```

This multi-file architecture provides better organization, maintainability, and extensibility while preserving all existing functionality and adding powerful new features!