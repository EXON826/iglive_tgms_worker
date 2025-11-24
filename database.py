"""
Database adapter for TGMS - Supabase PostgreSQL
Replaces SQLite-based DatabaseManager with PostgreSQL
"""
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
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

    def ensure_user_exists(self, user_data: dict):
        """Ensure user exists in all_tele_users table, create/update if needed"""
        try:
            with self.get_session() as session:
                session.execute(text("""
                    INSERT INTO all_tele_users (
                        id, is_bot, first_name, last_name, username, language_code,
                        is_premium, added_to_attachment_menu, can_join_groups,
                        can_read_all_group_messages, supports_inline_queries,
                        can_connect_to_business, has_main_web_app, updated_at, last_seen
                    ) VALUES (
                        :id, :is_bot, :first_name, :last_name, :username, :language_code,
                        :is_premium, :added_to_attachment_menu, :can_join_groups,
                        :can_read_all_group_messages, :supports_inline_queries,
                        :can_connect_to_business, :has_main_web_app, NOW(), NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        username = EXCLUDED.username,
                        language_code = EXCLUDED.language_code,
                        is_premium = EXCLUDED.is_premium,
                        updated_at = NOW(),
                        last_seen = NOW()
                """), {
                    'id': user_data.get('id'),
                    'is_bot': user_data.get('is_bot', False),
                    'first_name': user_data.get('first_name', 'Unknown'),
                    'last_name': user_data.get('last_name'),
                    'username': user_data.get('username'),
                    'language_code': user_data.get('language_code'),
                    'is_premium': user_data.get('is_premium', False),
                    'added_to_attachment_menu': user_data.get('added_to_attachment_menu', False),
                    'can_join_groups': user_data.get('can_join_groups', True),
                    'can_read_all_group_messages': user_data.get('can_read_all_group_messages', False),
                    'supports_inline_queries': user_data.get('supports_inline_queries', False),
                    'can_connect_to_business': user_data.get('can_connect_to_business', False),
                    'has_main_web_app': user_data.get('has_main_web_app', False)
                })
                return True
        except Exception as e:
            logger.error(f"Failed to ensure user exists: {e}", exc_info=True)
            return False

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

    def log_sent_message(self, group_id: int, message_id: int, debug_code: str):
        """Log a sent message in the database"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    INSERT INTO sent_messages (group_id, message_id, debug_code, sent_at)
                    VALUES (:group_id, :message_id, :debug_code, NOW())
                    ON CONFLICT DO NOTHING
                """),
                {
                    "group_id": group_id,
                    "message_id": message_id,
                    "debug_code": debug_code,
                }
            )
            conn.commit()
            conn.commit()
            logger.debug(f"Logged sent message {message_id} to group {group_id}")

    def log_deleted_message(self, group_id: int, message_id: int, username: str):
        """Log a deleted message in the database"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    INSERT INTO deleted_messages (group_id, message_id, username, deleted_at)
                    VALUES (:group_id, :message_id, :username, NOW())
                    ON CONFLICT DO NOTHING
                """),
                {
                    "group_id": str(group_id),
                    "message_id": message_id,
                    "username": username,
                }
            )
            conn.commit()
            logger.debug(f"Logged deleted message {message_id} for {username} in {group_id}")

    def update_last_used_image_index(self, link_id: int, new_index: int):
        """Update the last_used_image_index for a specific insta_links record."""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE insta_links 
                    SET last_used_image_index = :new_index,
                        last_updated = NOW()
                    WHERE id = :link_id
                """),
                {"new_index": new_index, "link_id": link_id}
            )
            conn.commit()
            logger.info(f"Updated last_used_image_index to {new_index} for link_id {link_id}")

    def update_last_used_link_index(self, link_id: int, new_index: int):
        """Update the last_used_link_index for a specific insta_links record."""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE insta_links 
                    SET last_used_link_index = :new_index,
                        last_updated = NOW()
                    WHERE id = :link_id
                """),
                {"new_index": new_index, "link_id": link_id}
            )
            conn.commit()
            logger.info(f"Updated last_used_link_index to {new_index} for link_id {link_id}")

    def get_insta_link(self, username: str) -> Optional[Dict[str, Any]]:
        """Get the latest insta_links record for a given username."""
        with self.get_connection() as conn:
            result = conn.execute(
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

    # --- Join Request Operations ---
    
    def insert_join_request(self, user_id: int, chat_id: int, username: Optional[str] = None):
        """Insert a new join request record"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    INSERT INTO join_requests (user_id, chat_id, status, created_at)
                    VALUES (:user_id, :chat_id, 'pending', NOW())
                    ON CONFLICT (user_id, chat_id, status) DO NOTHING
                """),
                {"user_id": user_id, "chat_id": chat_id}
            )
            conn.commit()
            logger.debug(f"Inserted join request: user {user_id} → chat {chat_id}")
    
    def update_join_request_status_by_user_chat(self, user_id: int, chat_id: int, status: str):
        """Update join request status by user and chat ID"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    UPDATE join_requests 
                    SET status = :status
                    WHERE user_id = :user_id AND chat_id = :chat_id
                      AND created_at = (
                          SELECT MAX(created_at) FROM join_requests 
                          WHERE user_id = :user_id AND chat_id = :chat_id
                      )
                """),
                {"user_id": user_id, "chat_id": chat_id, "status": status}
            )
            conn.commit()
            logger.debug(f"Updated join request status to {status}: user {user_id} → chat {chat_id}")

    def user_in_managed_group(self, user_id: int) -> bool:
        """Check if user is already in a TGMS managed group"""
        with self.get_connection() as conn:
            result = conn.execute(
                text("SELECT groups FROM all_tele_users WHERE id = :user_id AND groups IS NOT NULL"),
                {"user_id": user_id}
            )
            return result.fetchone() is not None
    
    def update_user_group(self, user_id: int, group_id: int, first_name: str = 'Unknown', last_name: str = None, username: str = None):
        """Update user's group membership and ensure user exists"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    INSERT INTO all_tele_users (
                        id, groups, first_name, last_name, username, 
                        updated_at, last_seen
                    )
                    VALUES (
                        :user_id, :group_id, :first_name, :last_name, :username,
                        NOW(), NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        groups = EXCLUDED.groups,
                        first_name = COALESCE(EXCLUDED.first_name, all_tele_users.first_name),
                        last_name = COALESCE(EXCLUDED.last_name, all_tele_users.last_name),
                        username = COALESCE(EXCLUDED.username, all_tele_users.username),
                        updated_at = NOW(),
                        last_seen = NOW()
                """),
                {
                    "user_id": user_id, 
                    "group_id": group_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "username": username
                }
            )
            conn.commit()
            logger.info(f"Updated user {user_id} group membership to {group_id}")

    def get_last_notification(self, group_id: int, username: str) -> Optional[int]:
        """Get the last notification message ID for a specific user in a group"""
        with self.get_connection() as conn:
            result = conn.execute(
                text("""
                    SELECT message_id FROM live_notification_messages 
                    WHERE group_id = :group_id AND username = :username
                """),
                {"group_id": str(group_id), "username": username}
            )
            row = result.fetchone()
            msg_id = row[0] if row else None
            logger.debug(f"DB: get_last_notification({group_id}, {username}) -> {msg_id}")
            return msg_id

    def save_notification(self, group_id: int, username: str, message_id: int):
        """Save or update the last notification message ID"""
        with self.get_connection() as conn:
            conn.execute(
                text("""
                    INSERT INTO live_notification_messages (group_id, username, message_id, created_at)
                    VALUES (:group_id, :username, :message_id, NOW())
                    ON CONFLICT (group_id, username) DO UPDATE SET
                        message_id = EXCLUDED.message_id,
                        created_at = NOW()
                """),
                {"group_id": str(group_id), "username": username, "message_id": message_id}
            )
            conn.commit()
            logger.info(f"DB: save_notification({group_id}, {username}, {message_id}) - SAVED")

    def claim_notification_slot(self, group_id: int, username: str, debug_code: str) -> tuple[bool, int | None]:
        """
        Attempt to claim the notification slot for a user in a group.
        Returns (True, message_id) if claimed successfully, (False, None) if another job is processing.
        """
        with self.get_connection() as conn:
            # Check if a recent notification exists (within last 15 seconds)
            # This acts as a simple lock to prevent rapid-fire duplicates
            result = conn.execute(
                text("""
                    SELECT created_at, message_id FROM live_notification_messages 
                    WHERE group_id = :group_id AND username = :username
                """),
                {"group_id": str(group_id), "username": username}
            )
            row = result.fetchone()
            
            current_message_id = None
            if row:
                created_at = row[0]
                raw_msg_id = row[1]
                logger.info(f"DB: Found record - created_at={created_at}, raw_message_id={raw_msg_id} ({type(raw_msg_id).__name__})")
                current_message_id = row[1] if row[1] and row[1] > 0 else None
                logger.info(f"DB: After validation - current_message_id={current_message_id}")
                # If created less than 15 seconds ago, assume another job is handling it or just finished
                if (datetime.now(timezone.utc) - created_at).total_seconds() < 15:
                    logger.info(f"DB: Slot locked for {username} in {group_id} (created {created_at}) - SKIPPING")
                    return False, None
            else:
                logger.info(f"DB: No existing record for {username} in {group_id}")
            
            logger.info(f"DB: Claiming slot for {username} in {group_id} (debug_code: {debug_code})")
            
            # Update timestamp to 'claim' it. We preserve the existing message_id if it exists.
            # If it's a new record, message_id defaults to 0.
            conn.execute(
                text("""
                    INSERT INTO live_notification_messages (group_id, username, message_id, created_at)
                    VALUES (:group_id, :username, 0, NOW())
                    ON CONFLICT (group_id, username) DO UPDATE SET
                        created_at = NOW()
                """),
                {"group_id": str(group_id), "username": username}
            )
            conn.commit()
            return True, current_message_id

    def close(self):
        """Close database connections"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("Database connections closed")
