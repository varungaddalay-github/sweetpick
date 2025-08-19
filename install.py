#!/usr/bin/env python3
"""
Installation script for Sweet Morsels RAG application.
Handles dependency installation and resolves common compatibility issues.
"""
import sys
import subprocess
import os
from pathlib import Path


def run_command(command: str, description: str) -> bool:
    """Run a shell command and return success status."""
    print(f"🔄 {description}...")
    print(f"   Running: {command}")
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True, 
            capture_output=True, 
            text=True
        )
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"   Error: {e}")
        if e.stdout:
            print(f"   Output: {e.stdout}")
        if e.stderr:
            print(f"   Error: {e.stderr}")
        return False


def check_python_version() -> bool:
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    version = sys.version_info
    
    if version >= (3, 12, 2):
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Incompatible")
        print("   Python 3.12.2 or higher is required")
        return False


def upgrade_pip() -> bool:
    """Upgrade pip to latest version."""
    return run_command(
        "python -m pip install --upgrade pip",
        "Upgrading pip"
    )


def install_core_requirements() -> bool:
    """Install core requirements."""
    return run_command(
        "pip install -r requirements.txt",
        "Installing core requirements"
    )


def install_dev_requirements() -> bool:
    """Install development requirements."""
    if Path("requirements-dev.txt").exists():
        return run_command(
            "pip install -r requirements-dev.txt",
            "Installing development requirements"
        )
    else:
        print("⚠️  requirements-dev.txt not found, skipping development tools")
        return True


def fix_marshmallow_issue() -> bool:
    """Fix marshmallow compatibility issue."""
    print("🔧 Fixing marshmallow compatibility...")
    
    # Try to uninstall and reinstall marshmallow
    commands = [
        "pip uninstall marshmallow -y",
        "pip install 'marshmallow>=3.20.0,<4.0.0'"
    ]
    
    for command in commands:
        if not run_command(command, f"Running: {command}"):
            return False
    
    return True


def verify_installation() -> bool:
    """Verify that key packages are installed correctly."""
    print("🔍 Verifying installation...")
    
    key_packages = [
        "fastapi",
        "pydantic", 
        "pydantic_settings",
        "openai",
        "serpapi",
        "pymilvus",
        "redis"
    ]
    
    all_good = True
    
    for package in key_packages:
        try:
            importlib.import_module(package.replace("-", "_"))
            print(f"✅ {package}")
        except ImportError as e:
            print(f"❌ {package}: {e}")
            all_good = False
    
    return all_good


def main():
    """Main installation process."""
    print("🚀 Sweet Morsels Installation Script")
    print("=" * 50)
    
    # Check Python version first
    if not check_python_version():
        print("\n❌ Installation cannot continue. Please upgrade Python.")
        sys.exit(1)
    
    # Import importlib for verification
    global importlib
    import importlib
    
    # Run installation steps
    steps = [
        ("Upgrading pip", upgrade_pip),
        ("Installing core requirements", install_core_requirements),
        ("Fixing marshmallow compatibility", fix_marshmallow_issue),
        ("Installing development requirements", install_dev_requirements),
        ("Verifying installation", verify_installation)
    ]
    
    failed_steps = []
    
    for step_name, step_func in steps:
        print(f"\n{'='*20} {step_name} {'='*20}")
        if not step_func():
            failed_steps.append(step_name)
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 INSTALLATION SUMMARY")
    print("=" * 50)
    
    if not failed_steps:
        print("🎉 All installation steps completed successfully!")
        print("\nNext steps:")
        print("1. Copy config.env.example to .env")
        print("2. Edit .env with your API keys")
        print("3. Run: python check_environment.py")
        print("4. Start the app: python run.py")
    else:
        print("⚠️  Some installation steps failed:")
        for step in failed_steps:
            print(f"   - {step}")
        print("\nPlease check the error messages above and try again.")
        print("You may need to:")
        print("1. Check your internet connection")
        print("2. Ensure you have write permissions")
        print("3. Try running with: pip install --user -r requirements.txt")
    
    return len(failed_steps) == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 