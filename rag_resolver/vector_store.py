#!/usr/bin/env python3
"""
Vector store management using Qdrant for storing and retrieving
resolution knowledge from AWS/EKS and debug results
"""

import logging
import json
import hashlib
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import numpy as np

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, Filter, 
    FieldCondition, MatchValue, SearchRequest
)
import boto3
from botocore.exceptions import ClientError

try:
    from rag_resolver.config import ResolverConfig
except ImportError:
    from config import ResolverConfig

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
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using AWS Bedrock"""
        try:
            # Prepare the request based on the embedding model
            if "titan" in self.config.aws.bedrock_embedding_model.lower():
                # Amazon Titan Embeddings
                request_body = json.dumps({
                    "inputText": text
                })
            else:
                # Generic embedding model format
                request_body = json.dumps({
                    "texts": [text],
                    "input_type": "search_document"
                })
            
            response = self.bedrock_client.invoke_model(
                modelId=self.config.aws.bedrock_embedding_model,
                body=request_body,
                contentType="application/json",
                accept="application/json"
            )
            
            response_body = json.loads(response['body'].read())
            
            # Extract embedding based on model type
            if "titan" in self.config.aws.bedrock_embedding_model.lower():
                embedding = response_body.get('embedding', [])
            else:
                embeddings = response_body.get('embeddings', [])
                embedding = embeddings[0] if embeddings else []
            
            if not embedding:
                raise ValueError("No embedding returned from model")
            
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            # Fallback to random vector for testing
            logger.warning("Using random embedding as fallback")
            return np.random.rand(self.config.qdrant.vector_size).tolist()
    
    def store_resolution(self, resolution_data: Dict[str, Any]) -> str:
        """Store a resolution in the vector store"""
        try:
            # Create text for embedding
            text_content = self._create_searchable_text(resolution_data)
            
            # Generate embedding
            embedding = self.generate_embedding(text_content)
            
            # Create unique ID
            resolution_id = self._generate_resolution_id(resolution_data)
            
            # Prepare metadata
            metadata = {
                "error_type": resolution_data.get("error_type", "unknown"),
                "namespace": resolution_data.get("namespace", "default"),
                "severity": resolution_data.get("error_severity", "MEDIUM"),
                "success": resolution_data.get("resolution_success", False),
                "auto_resolved": resolution_data.get("auto_resolved", False),
                "created_at": datetime.utcnow().isoformat(),
                "resolution_type": resolution_data.get("resolution_type", "manual"),
                "commands_executed": len(resolution_data.get("executed_commands", [])),
                "text_content": text_content[:500]  # First 500 chars for preview
            }
            
            # Store in Qdrant
            point = PointStruct(
                id=resolution_id,  # This should now be a UUID string
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
            logger.error(f"Error storing resolution: {e}")
            raise
    
    def search_similar_resolutions(self, error_description: str, 
                                 error_type: str = None, 
                                 limit: int = None) -> List[Dict[str, Any]]:
        """Search for similar resolutions using vector similarity"""
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
            
            # Search for similar vectors
            search_results = self.client.search(
                collection_name=self.config.qdrant.collection_name,
                query_vector=query_embedding,
                query_filter=search_filter,
                limit=limit,
                score_threshold=self.config.rag.similarity_threshold
            )
            
            # Convert results to usable format
            results = []
            for hit in search_results:
                result = {
                    "id": hit.id,
                    "score": hit.score,
                    "metadata": hit.payload,
                    "relevance": "high" if hit.score > 0.8 else "medium" if hit.score > 0.7 else "low"
                }
                results.append(result)
            
            logger.info(f"Found {len(results)} similar resolutions for error type: {error_type}")
            return results
            
        except Exception as e:
            logger.error(f"Error searching similar resolutions: {e}")
            return []
    
    def get_resolution_by_id(self, resolution_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific resolution by ID"""
        try:
            points = self.client.retrieve(
                collection_name=self.config.qdrant.collection_name,
                ids=[resolution_id]
            )
            
            if points:
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
        """Update the success status of a resolution"""
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
            updated_payload = point.payload.copy()
            updated_payload["success"] = success
            updated_payload["updated_at"] = datetime.utcnow().isoformat()
            if feedback:
                updated_payload["feedback"] = feedback
            
            # Update point
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
            
        except Exception as e:
            logger.error(f"Error updating resolution success: {e}")
    
    def get_resolution_stats(self) -> Dict[str, Any]:
        """Get statistics about stored resolutions"""
        try:
            collection_info = self.client.get_collection(self.config.qdrant.collection_name)
            
            # Get counts by success status
            successful_count = len(self.client.scroll(
                collection_name=self.config.qdrant.collection_name,
                scroll_filter=Filter(
                    must=[FieldCondition(key="success", match=MatchValue(value=True))]
                ),
                limit=10000  # Max count
            )[0])
            
            # Get counts by error type
            error_types = {}
            all_points = self.client.scroll(
                collection_name=self.config.qdrant.collection_name,
                limit=10000
            )[0]
            
            for point in all_points:
                error_type = point.payload.get("error_type", "unknown")
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
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
        """Create searchable text from resolution data"""
        components = []
        
        # Error information
        error_type = resolution_data.get("error_type", "")
        error_message = resolution_data.get("error_message", "")
        if error_type:
            components.append(f"Error type: {error_type}")
        if error_message:
            components.append(f"Error: {error_message}")
        
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
            components.append("Resolution steps: " + " ".join(resolution_steps))
        
        executed_commands = resolution_data.get("executed_commands", [])
        if executed_commands:
            components.append("Commands executed: " + " ".join(executed_commands))
        
        # Diagnosis results
        diagnosis_results = resolution_data.get("diagnosis_results", {})
        if diagnosis_results:
            for cmd, result in diagnosis_results.items():
                if isinstance(result, str) and len(result) < 200:
                    components.append(f"Diagnosis {cmd}: {result}")
        
        return "\n".join(components)
    
    def _generate_resolution_id(self, resolution_data: Dict[str, Any]) -> str:
        """Generate a unique UUID for a resolution"""
        import uuid
        
        # Create hash based on key components for deterministic UUID
        key_components = [
            resolution_data.get("error_type", ""),
            resolution_data.get("namespace", ""),
            resolution_data.get("pod_name", ""),
            str(datetime.utcnow().date())  # Include date for uniqueness
        ]
        
        hash_input = "_".join(key_components)
        hash_object = hashlib.sha256(hash_input.encode())
        
        # Generate UUID from hash for Qdrant compatibility
        uuid_from_hash = uuid.uuid5(uuid.NAMESPACE_DNS, hash_object.hexdigest())
        return str(uuid_from_hash)
    
    def cleanup_old_resolutions(self, days_old: int = 90) -> int:
        """Clean up old unsuccessful resolutions"""
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=days_old)).isoformat()
            
            # Find old unsuccessful resolutions
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
                limit=10000
            )
            
            old_points = scroll_result[0]
            
            if old_points:
                old_ids = [point.id for point in old_points]
                self.client.delete(
                    collection_name=self.config.qdrant.collection_name,
                    points_selector=old_ids
                )
                
                logger.info(f"Cleaned up {len(old_ids)} old resolutions")
                return len(old_ids)
            
            return 0
            
        except Exception as e:
            logger.error(f"Error cleaning up old resolutions: {e}")
            return 0
    
    def close(self):
        """Close connections"""
        try:
            if hasattr(self, 'client'):
                # Qdrant client doesn't need explicit closing
                pass
            logger.info("Vector store connections closed")
        except Exception as e:
            logger.error(f"Error closing vector store: {e}")