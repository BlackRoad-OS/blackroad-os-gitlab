"""
BlackRoad Git Server - GitLab-inspired repository manager
Manages projects, merge requests, and CI pipelines
"""
import sqlite3
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from enum import Enum
import json
import hashlib
from pathlib import Path


class MRStatus(Enum):
    OPENED = "opened"
    MERGED = "merged"
    CLOSED = "closed"
    DRAFT = "draft"


class ReviewAction(Enum):
    APPROVE = "approve"
    REQUEST_CHANGES = "request_changes"
    COMMENT = "comment"


class PipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Project:
    """Represents a Git project/repository"""
    id: str
    name: str
    namespace: str
    description: str = ""
    visibility: str = "private"
    clone_url: str = ""
    default_branch: str = "main"
    has_ci: bool = False
    topics: List[str] = field(default_factory=list)
    star_count: int = 0
    fork_count: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    last_pushed_at: Optional[datetime] = None


@dataclass
class MergeRequest:
    """Represents a pull/merge request"""
    id: str
    project_id: str
    title: str
    description: str = ""
    source_branch: str = "feature"
    target_branch: str = "main"
    author: str = ""
    assignee: Optional[str] = None
    status: str = "opened"
    created_at: datetime = field(default_factory=datetime.now)
    merged_at: Optional[datetime] = None
    labels: List[str] = field(default_factory=list)
    review_count: int = 0


@dataclass
class Review:
    """Represents a review on a merge request"""
    id: str
    mr_id: str
    reviewer: str
    action: str
    comment: str = ""
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class Pipeline:
    """Represents a CI/CD pipeline execution"""
    id: str
    project_id: str
    ref: str
    sha: str
    status: str = "pending"
    stages: List[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_s: Optional[int] = None
    triggered_by: str = "push"


class GitServer:
    """Git hosting server and repository manager"""

    def __init__(self, db_path: Optional[str] = None):
        if db_path == ":memory:":
            self.db_path = ":memory:"
        else:
            self.db_path = db_path or str(Path.home() / ".blackroad" / "git_server.db")
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database schema"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                namespace TEXT NOT NULL,
                description TEXT,
                visibility TEXT DEFAULT 'private',
                clone_url TEXT,
                default_branch TEXT DEFAULT 'main',
                has_ci BOOLEAN DEFAULT 0,
                topics TEXT,
                star_count INTEGER DEFAULT 0,
                fork_count INTEGER DEFAULT 0,
                created_at TEXT,
                last_pushed_at TEXT,
                UNIQUE(namespace, name)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS merge_requests (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                source_branch TEXT DEFAULT 'feature',
                target_branch TEXT DEFAULT 'main',
                author TEXT,
                assignee TEXT,
                status TEXT DEFAULT 'opened',
                created_at TEXT,
                merged_at TEXT,
                labels TEXT,
                review_count INTEGER DEFAULT 0,
                FOREIGN KEY (project_id) REFERENCES projects(id)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id TEXT PRIMARY KEY,
                mr_id TEXT NOT NULL,
                reviewer TEXT,
                action TEXT,
                comment TEXT,
                created_at TEXT,
                FOREIGN KEY (mr_id) REFERENCES merge_requests(id)
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS pipelines (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                ref TEXT,
                sha TEXT,
                status TEXT DEFAULT 'pending',
                stages TEXT,
                started_at TEXT,
                finished_at TEXT,
                duration_s INTEGER,
                triggered_by TEXT DEFAULT 'push',
                FOREIGN KEY (project_id) REFERENCES projects(id)
            )
        """)

        conn.commit()
        conn.close()

    def create_project(self, namespace: str, name: str, description: str = "",
                      visibility: str = "private", default_branch: str = "main") -> Project:
        """Create a new project in the repository"""
        project_id = hashlib.md5(f"{namespace}/{name}".encode()).hexdigest()[:8]
        clone_url = f"git@git.blackroad.local:{namespace}/{name}.git"

        project = Project(
            id=project_id,
            name=name,
            namespace=namespace,
            description=description,
            visibility=visibility,
            clone_url=clone_url,
            default_branch=default_branch,
            created_at=datetime.now()
        )

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            project.id, project.name, project.namespace, project.description,
            project.visibility, project.clone_url, project.default_branch, False,
            json.dumps(project.topics), project.star_count, project.fork_count,
            project.created_at.isoformat(), None
        ))
        conn.commit()
        conn.close()
        return project

    def create_mr(self, project_id: str, title: str, source_branch: str,
                 target_branch: str, author: str, description: str = "") -> MergeRequest:
        """Create a merge request"""
        mr_id = hashlib.md5(f"{project_id}/{title}".encode()).hexdigest()[:8]

        mr = MergeRequest(
            id=mr_id,
            project_id=project_id,
            title=title,
            description=description,
            source_branch=source_branch,
            target_branch=target_branch,
            author=author,
            status=MRStatus.OPENED.value,
            created_at=datetime.now()
        )

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO merge_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            mr.id, mr.project_id, mr.title, mr.description, mr.source_branch,
            mr.target_branch, mr.author, None, mr.status, mr.created_at.isoformat(),
            None, json.dumps(mr.labels), mr.review_count
        ))
        conn.commit()
        conn.close()
        return mr

    def review_mr(self, mr_id: str, reviewer: str, action: str, comment: str = "") -> Review:
        """Add a review to a merge request"""
        review_id = hashlib.md5(f"{mr_id}/{reviewer}".encode()).hexdigest()[:8]

        review = Review(
            id=review_id,
            mr_id=mr_id,
            reviewer=reviewer,
            action=action,
            comment=comment,
            created_at=datetime.now()
        )

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO reviews VALUES (?, ?, ?, ?, ?, ?)
        """, (review.id, review.mr_id, review.reviewer, review.action, review.comment,
              review.created_at.isoformat()))

        c.execute("UPDATE merge_requests SET review_count = review_count + 1 WHERE id = ?", (mr_id,))
        conn.commit()
        conn.close()
        return review

    def merge_mr(self, mr_id: str, merged_by: str, squash: bool = False) -> MergeRequest:
        """Merge a merge request"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        merged_at = datetime.now().isoformat()
        c.execute("""
            UPDATE merge_requests SET status = ?, merged_at = ? WHERE id = ?
        """, (MRStatus.MERGED.value, merged_at, mr_id))

        conn.commit()
        conn.close()

        c.execute("SELECT * FROM merge_requests WHERE id = ?", (mr_id,))
        row = c.fetchone()
        return MergeRequest(
            id=row[0], project_id=row[1], title=row[2], description=row[3],
            source_branch=row[4], target_branch=row[5], author=row[6],
            status=row[8], created_at=datetime.fromisoformat(row[9]),
            merged_at=datetime.fromisoformat(merged_at) if merged_at else None
        )

    def create_pipeline(self, project_id: str, ref: str, sha: str,
                       triggered_by: str = "push") -> Pipeline:
        """Create a new CI/CD pipeline"""
        pipeline_id = hashlib.md5(f"{project_id}/{sha}".encode()).hexdigest()[:8]

        pipeline = Pipeline(
            id=pipeline_id,
            project_id=project_id,
            ref=ref,
            sha=sha,
            status=PipelineStatus.PENDING.value,
            triggered_by=triggered_by,
            started_at=datetime.now()
        )

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO pipelines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (pipeline.id, pipeline.project_id, pipeline.ref, pipeline.sha,
              pipeline.status, json.dumps(pipeline.stages),
              pipeline.started_at.isoformat(), None, None, pipeline.triggered_by))
        conn.commit()
        conn.close()
        return pipeline

    def update_pipeline(self, pipeline_id: str, status: str,
                       stages: Optional[List[str]] = None, duration_s: Optional[int] = None):
        """Update pipeline status and stages"""
        finished_at = datetime.now().isoformat() if status in [
            PipelineStatus.PASSED.value, PipelineStatus.FAILED.value, PipelineStatus.CANCELLED.value
        ] else None

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            UPDATE pipelines SET status = ?, stages = ?, finished_at = ?, duration_s = ? WHERE id = ?
        """, (status, json.dumps(stages or []), finished_at, duration_s, pipeline_id))
        conn.commit()
        conn.close()

    def get_project_stats(self, project_id: str) -> Dict:
        """Get statistics for a project"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM merge_requests WHERE project_id = ?", (project_id,))
        mr_count = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM pipelines WHERE project_id = ?", (project_id,))
        pipeline_count = c.fetchone()[0]

        c.execute("""
            SELECT COUNT(*) FROM pipelines
            WHERE project_id = ? AND status = ?
        """, (project_id, PipelineStatus.PASSED.value))
        passed_count = c.fetchone()[0]

        pass_rate = (passed_count / pipeline_count * 100) if pipeline_count > 0 else 0

        conn.close()
        return {
            "project_id": project_id,
            "merge_requests": mr_count,
            "pipelines": pipeline_count,
            "passed_pipelines": passed_count,
            "pass_rate": f"{pass_rate:.1f}%"
        }

    def search_projects(self, query: str, visibility: Optional[str] = None) -> List[Project]:
        """Search projects by name and description"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        if visibility:
            c.execute("""
                SELECT * FROM projects WHERE (name LIKE ? OR description LIKE ?) AND visibility = ?
            """, (f"%{query}%", f"%{query}%", visibility))
        else:
            c.execute("""
                SELECT * FROM projects WHERE name LIKE ? OR description LIKE ?
            """, (f"%{query}%", f"%{query}%"))

        rows = c.fetchall()
        conn.close()
        return [self._project_from_row(row) for row in rows]

    def get_activity_feed(self, namespace: Optional[str] = None, n: int = 20) -> Dict:
        """Get recent activity feed"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        if namespace:
            c.execute("""
                SELECT id, last_pushed_at FROM projects WHERE namespace = ? ORDER BY last_pushed_at DESC LIMIT ?
            """, (namespace, n))
        else:
            c.execute("""
                SELECT id, last_pushed_at FROM projects ORDER BY last_pushed_at DESC LIMIT ?
            """, (n,))

        pushes = c.fetchall()

        c.execute("""
            SELECT id, title, created_at FROM merge_requests ORDER BY created_at DESC LIMIT ?
        """, (n,))
        mrs = c.fetchall()

        c.execute("""
            SELECT id, status, started_at FROM pipelines ORDER BY started_at DESC LIMIT ?
        """, (n,))
        pipelines = c.fetchall()

        conn.close()

        return {
            "recent_pushes": [{"project_id": p[0], "timestamp": p[1]} for p in pushes],
            "recent_mrs": [{"id": m[0], "title": m[1], "created_at": m[2]} for m in mrs],
            "recent_pipelines": [{"id": p[0], "status": p[1], "started_at": p[2]} for p in pipelines]
        }

    def _project_from_row(self, row) -> Project:
        """Convert database row to Project object"""
        return Project(
            id=row[0], name=row[1], namespace=row[2], description=row[3],
            visibility=row[4], clone_url=row[5], default_branch=row[6],
            has_ci=bool(row[7]), topics=json.loads(row[8]),
            star_count=row[9], fork_count=row[10],
            created_at=datetime.fromisoformat(row[11]),
            last_pushed_at=datetime.fromisoformat(row[12]) if row[12] else None
        )


if __name__ == "__main__":
    print("BlackRoad Git Server")
