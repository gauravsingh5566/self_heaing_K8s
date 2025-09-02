#!/usr/bin/env python3
"""
Simple Qdrant collection reset using HTTP requests
"""

import requests
import json

def reset_qdrant_collection():
    """Reset the Qdrant collection using HTTP API"""
    
    QDRANT_HOST = "35.94.146.89"
    QDRANT_PORT = 6333
    COLLECTION_NAME = "k8s_resolutions"
    BASE_URL = f"http://{QDRANT_HOST}:{QDRANT_PORT}"
    
    try:
        print("Checking existing collection...")
        
        # Check if collection exists
        response = requests.get(f"{BASE_URL}/collections/{COLLECTION_NAME}")
        
        if response.status_code == 200:
            print(f"Found existing collection: {COLLECTION_NAME}")
            collection_info = response.json()
            current_size = collection_info['result']['config']['params']['vectors']['size']
            print(f"   Current vector size: {current_size}")
            
            # Delete existing collection
            print("Deleting existing collection...")
            delete_response = requests.delete(f"{BASE_URL}/collections/{COLLECTION_NAME}")
            
            if delete_response.status_code == 200:
                print("Collection deleted successfully")
            else:
                print(f"Error deleting collection: {delete_response.text}")
                return False
        
        # Create new collection with correct dimensions
        print("Creating new collection with 1024 dimensions...")
        
        collection_config = {
            "vectors": {
                "size": 1024,
                "distance": "Cosine"
            }
        }
        
        create_response = requests.put(
            f"{BASE_URL}/collections/{COLLECTION_NAME}",
            json=collection_config,
            headers={"Content-Type": "application/json"}
        )
        
        if create_response.status_code == 200:
            print("New collection created successfully!")
            print(f"   Collection: {COLLECTION_NAME}")
            print(f"   Vector size: 1024")
            print(f"   Distance metric: Cosine")
            
            # Verify collection
            verify_response = requests.get(f"{BASE_URL}/collections/{COLLECTION_NAME}")
            if verify_response.status_code == 200:
                verify_info = verify_response.json()
                actual_size = verify_info['result']['config']['params']['vectors']['size']
                print(f"Verification: Collection has {actual_size} dimensions")
                return True
            else:
                print("Warning: Could not verify collection creation")
                return True
        else:
            print(f"Error creating collection: {create_response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"Error: Could not connect to Qdrant at {BASE_URL}")
        print("Check if Qdrant is running and accessible")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    print("Qdrant Collection Reset (HTTP API)")
    print("=" * 40)
    
    success = reset_qdrant_collection()
    
    if success:
        print("\nCollection reset complete!")
        print("Now update your configuration:")
        print("   export VECTOR_SIZE=1024")
        print("\nYou can now run the resolver again")
    else:
        print("\nCollection reset failed")
        print("Check Qdrant connection and try again")