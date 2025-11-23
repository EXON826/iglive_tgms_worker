"""
Database Manager (Async)
Handles all database interactions using SQLAlchemy and asyncpg
"""
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone

from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Async Database Manager using SQLAlchemy"""
    
    def __init__(self, database_url: str):
        # Ensure we use the async driver
        if database_url and database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")
            
        self.engine = create_async_engine(
            database_url,
            echo=False,
            poolclass=NullPool,
            connect_args={"statement_cache_size": 0}
        )
        
        self.async_session_factory = sessionmaker(
            self.engine, 
            class_=AsyncSession, 
            expire_on_commit=False
        )
        logger.info("Async DatabaseManager initialized")

    @asynccontextmanager
    async def get_session(self) -> AsyncSession:
        """Provide a transactional scope around a series of operations."""
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Session rollback due to error: {e}")
                raise
            finally:
                await session.close()

    async def get_active_managed_groups(self) -> List[Dict[str, Any]]:
        """Get all active managed groups"""
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT group_id, title, admin_user_id FROM managed_groups WHERE is_active = true")
            )
            rows = result.fetchall()
            return [dict(row._mapping) for row in rows]

    async def get_managed_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific managed group"""
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT * FROM managed_groups WHERE group_id = :group_id"),
                {"group_id": group_id}
            )
            row = result.fetchone()
            return dict(row._mapping) if row else None

    async def get_insta_link(self, username: str) -> Optional[Dict[str, Any]]:
        """Get Instagram link details for a username"""
        async with self.get_session() as session:
            result = await session.execute(
                text("""
                    SELECT * FROM insta_links 
                    WHERE username = :username 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """),
                {"username": username}
            )
            row = result.fetchone()
            return dict(row._mapping) if row else None

    async def upsert_managed_group(
        self,
        group_id: int,
        title: Optional[str] = None,
        admin_user_id: Optional[int] = None,
        phase: str = 'growth',
        final_message_allowed: bool = True,
    ):
        """Insert or reactivate a managed group record."""
        async with self.get_session() as session:
            await session.execute(
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
            logger.info(f"Registered/updated managed group {group_id} ({title})")

    async def insert_join_request(self, user_id: int, chat_id: int, username: Optional[str] = None):
        """Insert a new join request record"""
        async with self.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO join_requests (user_id, chat_id, status, created_at)
                    VALUES (:user_id, :chat_id, 'pending', NOW())
                    ON CONFLICT (user_id, chat_id, status) DO NOTHING
                """),
                {"user_id": user_id, "chat_id": chat_id}
            )
            logger.debug(f"Inserted join request: user {user_id} → chat {chat_id}")

    async def user_in_managed_group(self, user_id: int) -> bool:
        """Check if user is already in a TGMS managed group"""
        async with self.get_session() as session:
            result = await session.execute(
                text("SELECT groups FROM all_tele_users WHERE id = :user_id AND groups IS NOT NULL"),
                {"user_id": user_id}
            )
            return result.fetchone() is not None

    async def update_member_count(self, group_id: int, count: int):
        """Update member count for a group"""
        async with self.get_session() as session:
            await session.execute(
                text("UPDATE managed_groups SET member_count = :count, updated_at = NOW() WHERE group_id = :group_id"),
                {"count": count, "group_id": group_id}
            )

    async def log_sent_message(self, group_id: int, message_id: int, debug_code: str):
        """Log a successfully sent message"""
        # This table might need to be created if it doesn't exist, but assuming it does based on context
        pass 

    async def log_deleted_message(self, group_id: int, message_id: int, username: str):
        """Log a deleted message"""
        # Placeholder for logging logic
        pass

    async def reset_failure_count(self, group_id: int):
        """Reset failure count for a group"""
        async with self.get_session() as session:
            await session.execute(
                text("UPDATE managed_groups SET failure_count = 0 WHERE group_id = :group_id"),
                {"group_id": group_id}
            )

    async def increment_failure_count(self, group_id: int):
        """Increment failure count for a group"""
        async with self.get_session() as session:
            await session.execute(
                text("UPDATE managed_groups SET failure_count = COALESCE(failure_count, 0) + 1 WHERE group_id = :group_id"),
                {"group_id": group_id}
            )

    async def deactivate_group(self, group_id: int, reason: str):
        """Deactivate a group"""
        async with self.get_session() as session:
            await session.execute(
                text("UPDATE managed_groups SET is_active = false, deactivation_reason = :reason WHERE group_id = :group_id"),
                {"group_id": group_id, "reason": reason}
            )
            logger.warning(f"Deactivated group {group_id}: {reason}")

    async def update_last_used_link_index(self, link_id: int, index: int):
        """Update the last used link index"""
        async with self.get_session() as session:
            await session.execute(
                text("UPDATE insta_links SET last_used_link_index = :index WHERE id = :id"),
                {"index": index, "id": link_id}
            )

    async def update_last_used_image_index(self, link_id: int, index: int):
        """Update the last used image index"""
        async with self.get_session() as session:
            await session.execute(
                text("UPDATE insta_links SET last_used_image_index = :index WHERE id = :id"),
                {"index": index, "id": link_id}
            )

    async def ensure_user_exists(self, user_data: dict):
        """Ensure user exists in all_tele_users"""
        async with self.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO all_tele_users (id, username, first_name, updated_at, last_seen)
                    VALUES (:id, :username, :first_name, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        username = COALESCE(EXCLUDED.username, all_tele_users.username),
                        first_name = COALESCE(EXCLUDED.first_name, all_tele_users.first_name),
                        updated_at = NOW(),
                        last_seen = NOW()
                """),
                user_data
            )

    async def claim_notification_slot(self, group_id: int, username: str, debug_code: str) -> Tuple[bool, Optional[int]]:
        """
        Attempt to claim the notification slot for a user in a group.
        Returns (True, message_id) if claimed successfully, (False, None) if another job is processing.
        """
        async with self.get_session() as session:
            # Check if a recent notification exists (within last 15 seconds)
            result = await session.execute(
                text("""
                    SELECT created_at, message_id FROM live_notification_messages 
                    WHERE group_id = :group_id AND username = :username
                """),
                {"group_id": str(group_id), "username": username}
            )
            row = result.fetchone()
            
            current_message_id = 0
            if row:
                created_at = row[0]
                current_message_id = row[1]
                # If created less than 15 seconds ago, assume another job is handling it or just finished
                if (datetime.now(timezone.utc) - created_at).total_seconds() < 15:
                    logger.info(f"DB: Slot locked for {username} in {group_id} (created {created_at}) - SKIPPING")
                    return False, None
            
            logger.info(f"DB: Claiming slot for {username} in {group_id} (debug_code: {debug_code})")
            
            # Update timestamp to 'claim' it.
            await session.execute(
                text("""
                    INSERT INTO live_notification_messages (group_id, username, message_id, created_at)
                    VALUES (:group_id, :username, 0, NOW())
                    ON CONFLICT (group_id, username) DO UPDATE SET
                        created_at = NOW()
                """),
                {"group_id": str(group_id), "username": username}
            )
            # Commit happens automatically via context manager
            return True, current_message_id

    async def save_notification(self, group_id: int, username: str, message_id: int):
        """Save or update the last notification message ID"""
        async with self.get_session() as session:
            await session.execute(
                text("""
                    INSERT INTO live_notification_messages (group_id, username, message_id, created_at)
                    VALUES (:group_id, :username, :message_id, NOW())
                    ON CONFLICT (group_id, username) DO UPDATE SET
                        message_id = EXCLUDED.message_id,
                        created_at = NOW()
                """),
                {"group_id": str(group_id), "username": username, "message_id": message_id}
            )
            logger.info(f"DB: save_notification({group_id}, {username}, {message_id}) - SAVED")

    async def fetch_pending_job(self, bot_token: str) -> Optional[Dict[str, Any]]:
        """Fetch and lock a pending job"""
        async with self.get_session() as session:
            # Postgres specific: FOR UPDATE SKIP LOCKED
            result = await session.execute(
                text("""
                    SELECT * FROM jobs
                    WHERE status = 'pending'
                      AND bot_token = :bot_token
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """),
                {'bot_token': bot_token}
            )
            row = result.fetchone()
            if row:
                job = dict(row._mapping)
                # Update status to processing immediately
                await session.execute(
                    text("""
                        UPDATE jobs
                        SET status = 'processing', updated_at = :now
                        WHERE job_id = :job_id
                    """),
                    {
                        'now': datetime.now(timezone.utc),
                        'job_id': job['job_id']
                    }
                )
                return job
            return None

    async def update_job_status(self, job_id: int, status: str, retries: int):
        """Update job status"""
        async with self.get_session() as session:
            await session.execute(
                text("""
                    UPDATE jobs
                    SET status = :status, retries = :retries, updated_at = :now
                    WHERE job_id = :job_id
                """),
                {
                    'status': status,
                    'retries': retries,
                    'now': datetime.now(timezone.utc),
                    'job_id': job_id
                }
            )

    async def close(self):
        """Close database connections"""
        await self.engine.dispose()
        logger.info("Database connections closed")
