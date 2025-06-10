#!/usr/bin/env python3

import requests
import os
import sys

def test_github_token():
    """Test script to verify GitHub token and repository access."""
    
    # Test repository (replace with your private repo)
    repo_url = "https://github.com/RBirdwatcher/FreeBIRD_P"
    
    # Get token from environment or prompt
    token = input("Enter your GitHub Personal Access Token: ").strip()
    
    if not token:
        print("No token provided")
        return False
    
    # Normalize URL
    normalized_url = repo_url.rstrip('/')
    if normalized_url.endswith('.git'):
        normalized_url = normalized_url[:-4]
    
    # Extract owner and repo
    parts = normalized_url.replace('https://github.com/', '').split('/')
    if len(parts) < 2:
        print(f"Invalid GitHub URL format: {repo_url}")
        return False
    
    owner, repo = parts[0], parts[1]
    api_url = f"https://api.github.com/repos/{owner}/{repo}"
    
    print(f"Testing access to: {normalized_url}")
    print(f"API URL: {api_url}")
    print(f"Token format: {'Valid' if token.startswith(('ghp_', 'github_pat_')) else 'Invalid - should start with ghp_ or github_pat_'}")
    
    # Test without token
    print("\n1. Testing without token...")
    response = requests.get(api_url, timeout=10)
    print(f"   Status: {response.status_code}")
    if response.status_code != 200:
        print(f"   Response: {response.text[:200]}...")
    
    # Test with token
    print("\n2. Testing with token...")
    headers = {'Authorization': f'token {token}'}
    response = requests.get(api_url, headers=headers, timeout=10)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print("   SUCCESS: Repository is accessible with token!")
        repo_data = response.json()
        print(f"   Repository: {repo_data.get('full_name')}")
        print(f"   Private: {repo_data.get('private')}")
        return True
    else:
        print(f"   FAILED: {response.text[:200]}...")
        return False

if __name__ == "__main__":
    test_github_token()