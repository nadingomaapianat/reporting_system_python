#!/usr/bin/env python3
"""
Simple script to load environment variables from environment.env file
This can be used as an alternative to python-dotenv
"""
import os

def load_env_file(env_file='environment.env'):
    """Load environment variables from a file"""
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    # Split on first '=' to handle values with '=' in them
                    if '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
                        print(f"Loaded: {key.strip()} = {value.strip()}")
    else:
        print(f"Environment file {env_file} not found")

if __name__ == "__main__":
    load_env_file()
    print(f"\nNODE_BACKEND_URL: {os.getenv('NODE_BACKEND_URL', 'Not set')}")
