#!/usr/bin/env python3
"""
Main microservice for RAG-based Kubernetes error resolution
Provides REST API and background processing capabilities
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient
import uvicorn

from .config import load_resolver_config, ResolverConfig
from .rag_engine import RAGEngine
from .vector_store import VectorStore
from .mcp_executor import MCPExecutor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
config: ResolverConfig = None
rag_engine: RAGEngine = None
mongodb_client: MongoClient = None

# Pydantic models for API
class ErrorResolutionRequest(BaseModel):
    error_log_id: str = Field(..., description="ID of the error log from debug_results")
    auto_execute: bool = Field(default=False, description="Whether to auto-execute safe commands")
    priority: str = Field(default="normal", description="Priority: low, normal, high, critical")
    requester: str = Field(default="system", description="Who requested the resolution")

class ResolutionStatusResponse(BaseModel):
    resolution_id: str
    status: str
    success: bool
    error_type: str
    namespace: str
    pod_name: str
    steps_executed: int
    auto_executed: bool
    created_at: str
    duration_seconds: float

class ResolutionFeedback(BaseModel):
    resolution_id: str
    success: bool
    feedback: Optional[str] = None
    resolved_by: str = Field(default="user")

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    components: Dict[str, str]
    active_resolutions: int
    total_resolutions: int

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    # Startup
    global config, rag_engine, mongodb_client
    
    logger.info("Starting RAG Resolver Service...")
    
    # Load configuration
    config = load_resolver_config()
    
    # Initialize components
    rag_engine = RAGEngine(config)
    mongodb_client = MongoClient(config.mongodb.uri)
    
    # Start background tasks
    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(periodic_data_sync())
    
    logger.info(f"RAG Resolver Service started on port {config.service_port}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down RAG Resolver Service...")
    if rag_engine:
        rag_engine.close()
    if mongodb_client:
        mongodb_client.close()

# Create FastAPI app
app = FastAPI(
    title="Kubernetes RAG Resolver",
    description="AI-powered Kubernetes error resolution using RAG and MCP",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency to get current config
def get_config() -> ResolverConfig:
    return config

def get_rag_engine() -> RAGEngine:
    return rag_engine

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Kubernetes RAG Resolver",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        # Test components
        components = {}
        
        # Test MongoDB
        try:
            mongodb_client.admin.command('ping')
            components["mongodb"] = "healthy"
        except Exception:
            components["mongodb"] = "unhealthy"
        
        # Test Qdrant
        try:
            stats = rag_engine.vector_store.get_resolution_stats()
            components["qdrant"] = "healthy" if "error" not in stats else "unhealthy"
        except Exception:
            components["qdrant"] = "unhealthy"
        
        # Test EKS connectivity
        try:
            cluster_info = rag_engine.mcp_executor.get_cluster_info()
            components["eks"] = "healthy" if "error" not in cluster_info else "unhealthy"
        except Exception:
            components["eks"] = "unhealthy"
        
        # Get resolution stats
        try:
            resolution_stats = rag_engine.get_resolution_stats()
            total_resolutions = resolution_stats.get("total_resolutions", 0)
        except Exception:
            total_resolutions = 0
        
        overall_status = "healthy" if all(status == "healthy" for status in components.values()) else "degraded"
        
        return HealthResponse(
            status=overall_status,
            timestamp=datetime.utcnow().isoformat(),
            components=components,
            active_resolutions=len(rag_engine.mcp_executor.active_sessions),
            total_resolutions=total_resolutions
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.post("/resolve")
async def resolve_error(
    request: ErrorResolutionRequest,
    background_tasks: BackgroundTasks,
    rag_engine: RAGEngine = Depends(get_rag_engine)
):
    """Resolve a Kubernetes error using RAG"""
    try:
        logger.info(f"Received resolution request for error: {request.error_log_id}")
        
        # Fetch error data from MongoDB
        error_data = await fetch_error_data(request.error_log_id)
        if not error_data:
            raise HTTPException(status_code=404, detail="Error log not found")
        
        # Add request metadata
        error_data["resolution_request"] = {
            "auto_execute": request.auto_execute,
            "priority": request.priority,
            "requester": request.requester,
            "requested_at": datetime.utcnow().isoformat()
        }
        
        # Start resolution in background for non-critical requests
        if request.priority != "critical":
            background_tasks.add_task(
                process_resolution_async,
                error_data,
                request.auto_execute
            )
            
            return {
                "message": "Resolution started in background",
                "error_log_id": request.error_log_id,
                "priority": request.priority,
                "estimated_completion": (datetime.utcnow() + timedelta(minutes=5)).isoformat()
            }
        else:
            # Process critical requests immediately
            result = await rag_engine.resolve_error(error_data, request.auto_execute)
            return {
                "resolution_result": result,
                "processed_immediately": True,
                "priority": request.priority
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in resolve_error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/resolution/{resolution_id}")
async def get_resolution_status(
    resolution_id: str,
    rag_engine: RAGEngine = Depends(get_rag_engine)
):
    """Get the status of a resolution"""
    try:
        status = await rag_engine.get_resolution_status(resolution_id)
        
        if "error" in status:
            raise HTTPException(status_code=404, detail=status["error"])
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting resolution status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/resolution/{resolution_id}/feedback")
async def update_resolution_feedback(
    resolution_id: str,
    feedback: ResolutionFeedback,
    rag_engine: RAGEngine = Depends(get_rag_engine)
):
    """Update resolution feedback for learning"""
    try:
        success = await rag_engine.update_resolution_feedback(
            resolution_id,
            feedback.success,
            feedback.feedback
        )
        
        if not success:
            raise HTTPException(status_code=404, detail="Resolution not found")
        
        return {
            "message": "Feedback updated successfully",
            "resolution_id": resolution_id,
            "success": feedback.success
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/resolutions/stats")
async def get_resolution_statistics(rag_engine: RAGEngine = Depends(get_rag_engine)):
    """Get resolution statistics"""
    try:
        stats = rag_engine.get_resolution_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error getting resolution statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/errors/unresolved")
async def get_unresolved_errors(
    limit: int = 50,
    error_type: Optional[str] = None,
    severity: Optional[str] = None
):
    """Get unresolved errors from debug_results"""
    try:
        # Build query
        query = {"status": "pending"}
        
        if error_type:
            query["error_type"] = error_type
        if severity:
            query["error_severity"] = severity
        
        # Fetch from MongoDB
        db = mongodb_client[config.mongodb.database_name]
        debug_collection = db[config.mongodb.debug_collection]
        
        cursor = debug_collection.find(query).sort("priority", 1).limit(limit)
        
        errors = []
        for doc in cursor:
            errors.append({
                "id": str(doc["_id"]),
                "error_log_id": doc.get("error_log_id"),
                "error_type": doc.get("error_type"),
                "severity": doc.get("error_severity"),
                "namespace": doc.get("namespace"),
                "pod_name": doc.get("pod_name"),
                "created_at": doc.get("created_at").isoformat() if doc.get("created_at") else None,
                "priority": doc.get("priority", 3)
            })
        
        return {
            "unresolved_errors": errors,
            "count": len(errors),
            "filters": {"error_type": error_type, "severity": severity}
        }
        
    except Exception as e:
        logger.error(f"Error getting unresolved errors: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/report/generate")
async def generate_resolution_report(
    error_log_id: str,
    rag_engine: RAGEngine = Depends(get_rag_engine)
):
    """Generate a comprehensive resolution report"""
    try:
        # Fetch error data
        error_data = await fetch_error_data(error_log_id)
        if not error_data:
            raise HTTPException(status_code=404, detail="Error log not found")
        
        # Generate report
        report = await rag_engine.generate_resolution_report(error_data)
        
        return report
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cluster/info")
async def get_cluster_info(rag_engine: RAGEngine = Depends(get_rag_engine)):
    """Get EKS cluster information"""
    try:
        cluster_info = rag_engine.mcp_executor.get_cluster_info()
        return cluster_info
        
    except Exception as e:
        logger.error(f"Error getting cluster info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/maintenance/cleanup")
async def cleanup_old_data(
    days_old: int = 30,
    rag_engine: RAGEngine = Depends(get_rag_engine)
):
    """Clean up old resolution data"""
    try:
        # Cleanup vector store
        deleted_resolutions = rag_engine.vector_store.cleanup_old_resolutions(days_old)
        
        # Cleanup MCP sessions
        deleted_sessions = rag_engine.mcp_executor.cleanup_old_sessions(max_age_hours=2)
        
        return {
            "cleanup_completed": True,
            "deleted_resolutions": deleted_resolutions,
            "deleted_sessions": deleted_sessions,
            "days_threshold": days_old
        }
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Background tasks
async def process_resolution_async(error_data: Dict[str, Any], auto_execute: bool):
    """Process resolution asynchronously"""
    try:
        logger.info(f"Starting async resolution for error: {error_data.get('error_type')}")
        
        result = await rag_engine.resolve_error(error_data, auto_execute)
        
        # Store result status in MongoDB for tracking
        db = mongodb_client[config.mongodb.database_name]
        async_results = db["async_resolution_results"]
        
        result_doc = {
            "error_log_id": error_data.get("error_log_id"),
            "resolution_id": result.get("resolution_id"),
            "success": result.get("success"),
            "completed_at": datetime.utcnow(),
            "result": result
        }
        
        async_results.insert_one(result_doc)
        
        logger.info(f"Async resolution completed: {result.get('resolution_id')}")
        
    except Exception as e:
        logger.error(f"Error in async resolution: {e}")

async def fetch_error_data(error_log_id: str) -> Optional[Dict[str, Any]]:
    """Fetch error data from MongoDB debug_results collection"""
    try:
        db = mongodb_client[config.mongodb.database_name]
        debug_collection = db[config.mongodb.debug_collection]
        
        # Find by error_log_id or _id
        error_doc = debug_collection.find_one({
            "$or": [
                {"error_log_id": error_log_id},
                {"_id": error_log_id}
            ]
        })
        
        if error_doc:
            # Convert ObjectId to string for JSON serialization
            error_doc["_id"] = str(error_doc["_id"])
            return error_doc
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching error data: {e}")
        return None

async def periodic_cleanup():
    """Periodic cleanup task"""
    while True:
        try:
            await asyncio.sleep(3600)  # Run every hour
            
            # Cleanup old MCP sessions
            rag_engine.mcp_executor.cleanup_old_sessions()
            
            # Cleanup old resolutions (weekly)
            current_hour = datetime.utcnow().hour
            if current_hour == 2:  # Run at 2 AM
                rag_engine.vector_store.cleanup_old_resolutions(days_old=90)
            
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")

async def periodic_data_sync():
    """Periodic data synchronization with external systems"""
    while True:
        try:
            await asyncio.sleep(1800)  # Run every 30 minutes
            
            # Sync successful resolutions to vector store
            # This would pull new successful resolutions from debug_results
            # and store them in the vector store for future RAG queries
            
            logger.debug("Periodic data sync completed")
            
        except Exception as e:
            logger.error(f"Error in periodic data sync: {e}")

def main():
    """Main function to start the service"""
    config = load_resolver_config()
    
    uvicorn.run(
        "rag_resolver.service:app",
        host="0.0.0.0",
        port=config.service_port,
        log_level=config.log_level.lower(),
        reload=False
    )

if __name__ == "__main__":
    main()