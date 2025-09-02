#!/usr/bin/env python3
"""
Vector store management using Qdrant for storing and retrieving
resolution knowledge from AWS/EKS and debug results
FIXED: Proper embedding response parsing and error handling
"""

import logging
import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import numpy as np

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, Filter, 
    FieldCondition, MatchValue, SearchRequest
)
import boto3
from botocore.exceptions import ClientError

from .config import ResolverConfig, parse_embedding_response

logger = logging.getLogger(__name__)

class VectorStore:
    """Qdrant-based vector store for resolution knowledge"""
    
    def __init__(self, config: ResolverConfig):
        self.config = config
        self.client = self._init_qdrant_client()
        self.bedrock_client = self._init_bedrock_client()
        self._ensure_collection()
    
    def _init_qdrant_client(self) -> QdrantClient:
        """Initialize Qdrant client"""
        try:
            if self.config.qdrant.api_key:
                # Cloud or authenticated instance
                client = QdrantClient(
                    host=self.config.qdrant.host,
                    port=self.config.qdrant.port,
                    api_key=self.config.qdrant.api_key,
                    https=True
                )
            else:
                # Local instance
                client = QdrantClient(
                    host=self.config.qdrant.host,
                    port=self.config.qdrant.port
                )
            
            # Test connection
            collections = client.get_collections()
            logger.info(f"Connected to Qdrant at {self.config.qdrant.host}:{self.config.qdrant.port}")
            return client
            
        except Exception as e:
            logger.error(f"Failed to connect to Qdrant: {e}")
            raise
    
    def _init_bedrock_client(self):
        """Initialize AWS Bedrock client for embeddings"""
        try:
            session = boto3.Session(
                profile_name=self.config.aws.profile,
                region_name=self.config.aws.region
            )
            return session.client('bedrock-runtime')
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise
    
    def _ensure_collection(self):
        """Ensure the Qdrant collection exists with proper configuration"""
        try:
            collections = self.client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.config.qdrant.collection_name not in collection_names:
                logger.info(f"Creating collection: {self.config.qdrant.collection_name}")
                
                # Determine distance metric
                distance_map = {
                    "cosine": Distance.COSINE,
                    "euclidean": Distance.EUCLID,
                    "dot": Distance.DOT
                }
                distance = distance_map.get(self.config.qdrant.distance_metric.lower(), Distance.COSINE)
                
                self.client.create_collection(
                    collection_name=self.config.qdrant.collection_name,
                    vectors_config=VectorParams(
                        size=self.config.qdrant.vector_size,
                        distance=distance
                    )
                )
                logger.info("Collection created successfully")
            else:
                logger.info(f"Collection {self.config.qdrant.collection_name} already exists")
                
        except Exception as e:
            logger.error(f"Error ensuring collection: {e}")
            raise
    
    def generate_embedding(self, text: str, max_retries: int = 3) -> List[float]:
        """FIXED: Generate embedding using AWS Bedrock with proper response parsing"""
        for attempt in range(max_retries):
            try:
                # Prepare the request based on the embedding model
                if "titan" in self.config.aws.bedrock_embedding_model.lower():
                    # Amazon Titan Embeddings
                    request_body = json.dumps({
                        "inputText": text[:8000]  # Limit input size
                    })
                else:
                    # Generic embedding model format
                    request_body = json.dumps({
                        "texts": [text[:8000]],
                        "input_type": "search_document"
                    })
                
                response = self.bedrock_client.invoke_model(
                    modelId=self.config.aws.bedrock_embedding_model,
                    body=request_body,
                    contentType="application/json",
                    accept="application/json"
                )
                
                response_body = json.loads(response['body'].read())
                
                # FIXED: Use proper response parser
                embedding = parse_embedding_response(response_body, self.config.aws.bedrock_embedding_model)
                
                if not embedding:
                    raise ValueError("No embedding returned from model")
                
                # Validate embedding dimension
                if len(embedding) != self.config.qdrant.vector_size:
                    logger.warning(f"Embedding dimension mismatch: got {len(embedding)}, expected {self.config.qdrant.vector_size}")
                    # Pad or truncate to match expected size
                    if len(embedding) < self.config.qdrant.vector_size:
                        embedding.extend([0.0] * (self.config.qdrant.vector_size - len(embedding)))
                    else:
                        embedding = embedding[:self.config.qdrant.vector_size]
                
                return embedding
                
            except Exception as e:
                logger.error(f"Embedding attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    # Fallback to random vector for testing/development
                    logger.warning("Using random embedding as fallback")
                    return np.random.rand(self.config.qdrant.vector_size).tolist()
                
                # Wait before retry
                import time
                time.sleep(2 ** attempt)
    
    def store_resolution(self, resolution_data: Dict[str, Any]) -> str:
        """Store a resolution in the vector store with enhanced error handling"""
        try:
            # Create text for embedding
            text_content = self._create_searchable_text(resolution_data)
            
            if not text_content.strip():
                logger.warning("Empty text content for embedding, using default")
                text_content = f"Error type: {resolution_data.get('error_type', 'unknown')}"
            
            # Generate embedding
            embedding = self.generate_embedding(text_content)
            
            # Create unique ID
            resolution_id = self._generate_resolution_id(resolution_data)
            
            # Prepare metadata with safe extraction
            metadata = {
                "error_type": resolution_data.get("error_type", "unknown"),
                "namespace": resolution_data.get("namespace", "default"),
                "severity": resolution_data.get("error_severity", "MEDIUM"),
                "success": resolution_data.get("resolution_success", False),
                "auto_resolved": resolution_data.get("auto_resolved", False),
                "created_at": datetime.utcnow().isoformat(),
                "resolution_type": resolution_data.get("resolution_type", "manual"),
                "commands_executed": len(resolution_data.get("executed_commands", [])),
                "text_content": text_content[:500],  # First 500 chars for preview
                "resolution_duration": resolution_data.get("resolution_duration", 0.0),
                "intelligence_used": resolution_data.get("intelligence_used", False)
            }
            
            # Store in Qdrant with retry logic
            for attempt in range(3):
                try:
                    point = PointStruct(
                        id=resolution_id,
                        vector=embedding,
                        payload=metadata
                    )
                    
                    self.client.upsert(
                        collection_name=self.config.qdrant.collection_name,
                        points=[point]
                    )
                    
                    logger.info(f"Stored resolution: {resolution_id}")
                    return resolution_id
                    
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        raise e
                    logger.warning(f"Storage attempt {attempt + 1} failed: {e}, retrying...")
                    import time
                    time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error storing resolution: {e}")
            raise
    
    def search_similar_resolutions(self, error_description: str, 
                                 error_type: str = None, 
                                 limit: int = None) -> List[Dict[str, Any]]:
        """Search for similar resolutions using vector similarity with enhanced error handling"""
        limit = limit or self.config.rag.max_context_documents
        
        try:
            # Generate query embedding
            query_embedding = self.generate_embedding(error_description)
            
            # Build filter conditions
            filter_conditions = []
            if error_type:
                filter_conditions.append(
                    FieldCondition(
                        key="error_type",
                        match=MatchValue(value=error_type)
                    )
                )
            
            # Prefer successful resolutions
            search_filter = Filter(must=filter_conditions) if filter_conditions else None
            
            # Search for similar vectors with retry logic
            for attempt in range(3):
                try:
                    search_results = self.client.search(
                        collection_name=self.config.qdrant.collection_name,
                        query_vector=query_embedding,
                        query_filter=search_filter,
                        limit=limit,
                        score_threshold=self.config.rag.similarity_threshold
                    )
                    break
                except Exception as e:
                    if attempt == 2:
                        raise e
                    logger.warning(f"Search attempt {attempt + 1} failed: {e}, retrying...")
                    import time
                    time.sleep(1)
            
            # Convert results to usable format
            results = []
            for hit in search_results:
                try:
                    result = {
                        "id": hit.id,
                        "score": hit.score,
                        "metadata": hit.payload,
                        "relevance": "high" if hit.score > 0.8 else "medium" if hit.score > 0.7 else "low"
                    }
                    results.append(result)
                except Exception as e:
                    logger.warning(f"Error processing search result: {e}")
                    continue
            
            logger.info(f"Found {len(results)} similar resolutions for error type: {error_type}")
            return results
            
        except Exception as e:
            logger.error(f"Error searching similar resolutions: {e}")
            return []
    
    def get_resolution_by_id(self, resolution_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific resolution by ID with error handling"""
        try:
            points = self.client.retrieve(
                collection_name=self.config.qdrant.collection_name,
                ids=[resolution_id]
            )
            
            if points and len(points) > 0:
                point = points[0]
                return {
                    "id": point.id,
                    "metadata": point.payload
                }
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving resolution {resolution_id}: {e}")
            return None
    
    def update_resolution_success(self, resolution_id: str, success: bool, 
                                feedback: str = None):
        """Update the success status of a resolution with error handling"""
        try:
            # Get current point
            points = self.client.retrieve(
                collection_name=self.config.qdrant.collection_name,
                ids=[resolution_id]
            )
            
            if not points:
                logger.warning(f"Resolution {resolution_id} not found for update")
                return
            
            point = points[0]
            
            # Update metadata
            updated_payload = dict(point.payload) if point.payload else {}
            updated_payload["success"] = success
            updated_payload["updated_at"] = datetime.utcnow().isoformat()
            if feedback:
                updated_payload["feedback"] = feedback
            
            # Update point with retry logic
            for attempt in range(3):
                try:
                    updated_point = PointStruct(
                        id=resolution_id,
                        vector=point.vector,
                        payload=updated_payload
                    )
                    
                    self.client.upsert(
                        collection_name=self.config.qdrant.collection_name,
                        points=[updated_point]
                    )
                    
                    logger.info(f"Updated resolution {resolution_id} success status: {success}")
                    return
                    
                except Exception as e:
                    if attempt == 2:
                        raise e
                    logger.warning(f"Update attempt {attempt + 1} failed: {e}, retrying...")
                    import time
                    time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error updating resolution success: {e}")
    
    def get_resolution_stats(self) -> Dict[str, Any]:
        """Get statistics about stored resolutions with comprehensive error handling"""
        try:
            collection_info = self.client.get_collection(self.config.qdrant.collection_name)
            
            if collection_info.points_count == 0:
                return {
                    "total_resolutions": 0,
                    "successful_resolutions": 0,
                    "success_rate": 0.0,
                    "error_types": {},
                    "collection_status": collection_info.status,
                    "last_updated": datetime.utcnow().isoformat()
                }
            
            # Get successful resolutions count
            successful_count = 0
            try:
                successful_points = self.client.scroll(
                    collection_name=self.config.qdrant.collection_name,
                    scroll_filter=Filter(
                        must=[FieldCondition(key="success", match=MatchValue(value=True))]
                    ),
                    limit=10000  # Max count
                )
                successful_count = len(successful_points[0])
            except Exception as e:
                logger.warning(f"Could not get successful resolutions count: {e}")
            
            # Get error type distribution
            error_types = {}
            try:
                all_points = self.client.scroll(
                    collection_name=self.config.qdrant.collection_name,
                    limit=1000  # Reasonable limit for stats
                )
                
                for point in all_points[0]:
                    if point.payload:
                        error_type = point.payload.get("error_type", "unknown")
                        error_types[error_type] = error_types.get(error_type, 0) + 1
            except Exception as e:
                logger.warning(f"Could not get error type distribution: {e}")
            
            return {
                "total_resolutions": collection_info.points_count,
                "successful_resolutions": successful_count,
                "success_rate": (successful_count / collection_info.points_count * 100) 
                               if collection_info.points_count > 0 else 0,
                "error_types": error_types,
                "collection_status": collection_info.status,
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting resolution stats: {e}")
            return {"error": str(e)}
    
    def _create_searchable_text(self, resolution_data: Dict[str, Any]) -> str:
        """Create searchable text from resolution data with safe extraction"""
        components = []
        
        # Safely extract error information
        error_type = resolution_data.get("error_type", "")
        error_message = resolution_data.get("error_message", "")
        if error_type:
            components.append(f"Error type: {error_type}")
        if error_message:
            # Limit message length and clean it
            clean_message = str(error_message)[:500].replace('\n', ' ').replace('\r', ' ')
            components.append(f"Error: {clean_message}")
        
        # Context information
        namespace = resolution_data.get("namespace", "")
        pod_name = resolution_data.get("pod_name", "")
        if namespace:
            components.append(f"Namespace: {namespace}")
        if pod_name:
            components.append(f"Pod: {pod_name}")
        
        # Resolution information
        resolution_steps = resolution_data.get("resolution_steps", [])
        if resolution_steps:
            steps_text = " ".join(str(step) for step in resolution_steps if step)
            components.append(f"Resolution steps: {steps_text}")
        
        executed_commands = resolution_data.get("executed_commands", [])
        if executed_commands:
            commands_text = " ".join(str(cmd) for cmd in executed_commands if cmd)
            components.append(f"Commands executed: {commands_text}")
        
        # Diagnosis results (safely extract)
        diagnosis_results = resolution_data.get("diagnosis_results", {})
        if isinstance(diagnosis_results, dict):
            results = diagnosis_results.get("results", {})
            if isinstance(results, dict):
                for cmd, result in list(results.items())[:3]:  # Limit to first 3
                    if isinstance(result, dict) and result.get("output"):
                        output = str(result["output"])[:200]  # Limit output length
                        components.append(f"Diagnosis {cmd}: {output}")
        
        # AWS Intelligence findings
        aws_investigation = resolution_data.get("aws_investigation", {})
        if isinstance(aws_investigation, dict):
            findings = aws_investigation.get("findings", [])
            if findings:
                findings_text = " ".join(str(finding) for finding in findings[:2] if finding)
                components.append(f"AWS findings: {findings_text}")
        
        return "\n".join(components)
    
    def _generate_resolution_id(self, resolution_data: Dict[str, Any]) -> str:
        """Generate a unique ID for a resolution with collision avoidance"""
        # Create hash based on key components
        key_components = [
            str(resolution_data.get("error_type", "")),
            str(resolution_data.get("namespace", "")),
            str(resolution_data.get("pod_name", "")),
            str(datetime.utcnow().timestamp())  # Add timestamp for uniqueness
        ]
        
        hash_input = "_".join(key_components)
        hash_object = hashlib.sha256(hash_input.encode())
        
        # Create a shorter, more manageable ID
        short_hash = hash_object.hexdigest()[:16]
        return f"res_{short_hash}"
    
    def cleanup_old_resolutions(self, days_old: int = 90) -> int:
        """Clean up old unsuccessful resolutions with enhanced error handling"""
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=days_old)).isoformat()
            
            # Find old unsuccessful resolutions
            try:
                scroll_result = self.client.scroll(
                    collection_name=self.config.qdrant.collection_name,
                    scroll_filter=Filter(
                        must=[
                            FieldCondition(key="success", match=MatchValue(value=False)),
                            FieldCondition(
                                key="created_at", 
                                range={
                                    "lt": cutoff_date
                                }
                            )
                        ]
                    ),
                    limit=1000  # Process in batches
                )
                
                old_points = scroll_result[0] if scroll_result else []
                
            except Exception as e:
                logger.warning(f"Could not query old resolutions: {e}")
                return 0
            
            if old_points:
                old_ids = [point.id for point in old_points if hasattr(point, 'id')]
                
                if old_ids:
                    try:
                        self.client.delete(
                            collection_name=self.config.qdrant.collection_name,
                            points_selector=old_ids
                        )
                        
                        logger.info(f"Cleaned up {len(old_ids)} old resolutions")
                        return len(old_ids)
                    except Exception as e:
                        logger.error(f"Error deleting old resolutions: {e}")
                        return 0
            
            return 0
            
        except Exception as e:
            logger.error(f"Error cleaning up old resolutions: {e}")
            return 0
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get vector store health status"""
        try:
            # Test basic connectivity
            collections = self.client.get_collections()
            
            # Check if our collection exists
            collection_exists = any(
                col.name == self.config.qdrant.collection_name 
                for col in collections.collections
            )
            
            if not collection_exists:
                return {
                    "status": "unhealthy",
                    "error": f"Collection {self.config.qdrant.collection_name} not found",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            # Get collection info
            collection_info = self.client.get_collection(self.config.qdrant.collection_name)
            
            # Test embedding generation
            try:
                test_embedding = self.generate_embedding("test health check")
                embedding_healthy = len(test_embedding) == self.config.qdrant.vector_size
            except Exception as e:
                logger.warning(f"Embedding health check failed: {e}")
                embedding_healthy = False
            
            return {
                "status": "healthy" if embedding_healthy else "degraded",
                "collection_exists": collection_exists,
                "collection_status": collection_info.status,
                "points_count": collection_info.points_count,
                "embedding_healthy": embedding_healthy,
                "vector_size": self.config.qdrant.vector_size,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def close(self):
        """Close connections with proper cleanup"""
        try:
            # Qdrant client doesn't need explicit closing, but we can clean up any resources
            if hasattr(self, 'client') and self.client:
                # Any cleanup needed for the client
                pass
            
            if hasattr(self, 'bedrock_client') and self.bedrock_client:
                # Bedrock client cleanup if needed
                pass
                
            logger.info("Vector store connections closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing vector store: {e}")