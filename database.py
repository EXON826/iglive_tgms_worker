"""
Database adapter for TGMS - Supabase PostgreSQL
Replaces SQLite-based DatabaseManager with PostgreSQL
"""
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)


class DatabaseManager:
    """PostgreSQL Database Manager for TGMS"""
    
    def __init__(self, database_url: str):
        """
        Initialize database connection to Supabase PostgreSQL
        
        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url
        
        # Create engine with connection pooling
        self.engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        
        # Create session factory
        self.SessionFactory = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        logger.info("DatabaseManager initialized with PostgreSQL connection pool")
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionFactory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_connection(self):
        """Context manager for raw connections (for SQLite compatibility)"""
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()
    
    # --- Managed Groups Operations ---
    
    def get_active_managed_groups(self) -> List[Dict[str, Any]]:
        """Get all active managed groups"""
        with self.get_connection() as conn:
            result = conn.execute(text("""
                SELECT group_id, title, admin_user_id, phase, 
                       final_message_allowed, member_count, is_active
                FROM managed_groups
                WHERE is_active = true
                ORDER BY group_id
            """))
            return [dict(row._mapping) for row in result.fetchall()]
    
    def get_managed_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific managed group by ID"""
        with self.get_connection() as conn:
            result = conn.execute(
                text("SELECT * FROM managed_groups WHERE group_id = :group_id"),
                {"group_id": group_id}
            )
            row = result.fetchone()
            return dict(row._mapping) if row else None

    def update_group_phase(self, group_id: int, phase: str):
        """Update group phase (growth/monitoring)"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE managed_groups 
                    SET phase = :phase, updated_at = NOW()
                    WHERE group_id = :group_id
                """),
                {"phase": phase, "group_id": group_id}
            )
            conn.commit()

    def update_member_count(self, group_id: int, count: int):
        """Update group member count"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE managed_groups 
                    SET member_count = :count, updated_at = NOW()
                    WHERE group_id = :group_id
                """),
                {"count": count, "group_id": group_id}
            )
            conn.commit()

    def deactivate_group(self, group_id: int, reason: str = None):
        """Deactivate a managed group"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE managed_groups 
                    SET is_active = false, updated_at = NOW()
                    WHERE group_id = :group_id
                """),
                {"group_id": group_id}
            )
            conn.commit()
            logger.info(f"Deactivated group {group_id}. Reason: {reason}")

    def increment_failure_count(self, group_id: int) -> int:
        """Increment consecutive failure count and return new count"""
        with self.get_connection() as conn:
            result = conn.execute(
                text("""
                    UPDATE managed_groups 
                    SET consecutive_failures = COALESCE(consecutive_failures, 0) + 1,
                        updated_at = NOW()
                    WHERE group_id = :group_id
                    RETURNING consecutive_failures
                """),
                {"group_id": group_id}
            )
            conn.commit()
            row = result.fetchone()
            return row[0] if row else 0

    def reset_failure_count(self, group_id: int):
        """Reset consecutive failure count to 0"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE managed_groups 
                    SET consecutive_failures = 0, updated_at = NOW()
                    WHERE group_id = :group_id
                """),
                {"group_id": group_id}
            )
            conn.commit()

    def upsert_managed_group(
        self,
        group_id: int,
        title: Optional[str] = None,
        admin_user_id: Optional[int] = None,
        phase: str = 'growth',
        final_message_allowed: bool = True,
    ):
        """Insert or reactivate a managed group record."""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    INSERT INTO managed_groups (group_id, admin_user_id, title, phase, is_active, final_message_allowed)
                    VALUES (:group_id, :admin_user_id, :title, :phase, true, :final_message_allowed)
                    ON CONFLICT (group_id) DO UPDATE SET
                        title = COALESCE(EXCLUDED.title, managed_groups.title),
                        admin_user_id = COALESCE(EXCLUDED.admin_user_id, managed_groups.admin_user_id),
                        phase = COALESCE(EXCLUDED.phase, managed_groups.phase),
                        final_message_allowed = COALESCE(EXCLUDED.final_message_allowed, managed_groups.final_message_allowed),
                        is_active = true,
                        updated_at = NOW()
                """),
                {
                    "group_id": group_id,
                    "admin_user_id": admin_user_id,
                    "title": title,
                    "phase": phase or 'growth',
                    "final_message_allowed": final_message_allowed,
                }
            )
            conn.commit()
            logger.info(f"Registered/updated managed group {group_id} ({title})")
