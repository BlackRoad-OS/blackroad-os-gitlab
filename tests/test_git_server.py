import pytest
from src.git_server import GitServer

def test_create_project():
    server = GitServer(":memory:")
    project = server.create_project("blackroad", "test-repo", "Test Repository")
    assert project.name == "test-repo"
    assert project.namespace == "blackroad"

def test_search_projects():
    server = GitServer(":memory:")
    server.create_project("blackroad", "os-core", "Core OS")
    results = server.search_projects("core")
    assert len(results) > 0
