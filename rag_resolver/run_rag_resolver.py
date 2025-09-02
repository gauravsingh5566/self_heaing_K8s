#!/usr/bin/env python3
"""
Simple run script for RAG Resolver to avoid import issues
Place this file in the root directory (same level as rag_resolver/)
"""

import sys
import os

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

if __name__ == "__main__":
    # Import and run the main function
    from rag_resolver.main import main
    main()