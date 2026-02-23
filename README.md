# BlackRoad OS - Git Server

GitLab-inspired repository manager for BlackRoad OS infrastructure.

## Features

- **Project Management**: Create, manage, and organize Git projects with namespaces
- **Merge Requests**: Full MR workflow with reviews and approvals
- **CI/CD Pipelines**: Integrated pipeline execution and status tracking
- **Service Registry**: Distributed service discovery and health checks
- **Activity Feed**: Real-time activity tracking across projects

## Architecture

- Python 3.11+
- SQLite backend at `~/.blackroad/git_server.db`
- RESTful API design
- Background health checking

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```bash
# Create a project
python src/git_server.py projects --create --namespace blackroad --name os-core

# View project activity
python src/git_server.py activity --n 10

# Create a pipeline
python src/git_server.py pipeline PROJECT_ID main abc123def456
```
