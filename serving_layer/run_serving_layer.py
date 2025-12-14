"""
Run Serving Layer API Server
"""

import os
import sys
import uvicorn

# Set environment variables before importing
os.environ.setdefault("USE_MOCK_DATA", "true")  # Use mock data if batch views not available

if __name__ == "__main__":
    # Import after setting env vars
    import sys
    import os
    
    # Add current directory to path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    from config import serving_config
    
    print("="*60)
    print("  LEARNING ANALYTICS - SERVING LAYER")
    print("="*60)
    print(f"Starting API server on {serving_config['api']['host']}:{serving_config['api']['port']}")
    print(f"Mock data mode: {serving_config.get('use_mock_data', False)}")
    print("="*60)
    print()
    print("API Documentation: http://localhost:8000/docs")
    print("Health check: http://localhost:8000/api/v1/health")
    print()
    
    # Import app directly
    try:
        from serving_layer import app
    except ImportError:
        # Fallback: import from current directory
        import sys
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from serving_layer import app
    
    uvicorn.run(
        app,
        host=serving_config["api"]["host"],
        port=serving_config["api"]["port"],
        reload=serving_config["api"]["reload"]
    )

