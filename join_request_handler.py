"""
Join Request Handler
Auto-approves join requests for managed groups
"""
import logging
from telegram_api import TelegramAPI
from database import DatabaseManager

logger = logging.getLogger(__name__)


class JoinRequestHandler:
    """Handles chat join requests"""
    
    def __init__(self, bot_token: str, db_manager: DatabaseManager):
        self.api = TelegramAPI(bot_token)
        self.db = db_manager
    
    async def process_join_request(self, chat_id: int, user_id: int, username: str = None):
        """
        Process a join request by auto-approving it
        
        Args:
            chat_id: The chat ID where join request was made
            user_id: The user requesting to join
            username: Username of the user (optional)
        
        Returns:
            bool: True if successfully approved
        """
        try:
            # Check if this group is managed
            group = self.db.get_managed_group(chat_id)
            if not group:
                logger.warning(f"Join request for non-managed group {chat_id}")
                return False
            
            if not group.get('is_active'):
                logger.warning(f"Join request for inactive group {chat_id}")
                return False
            
            # Insert join request to database
            self.db.insert_join_request(user_id, chat_id, username)
            
            # Auto-approve
            response = self.api.approve_join_request(chat_id, user_id)
            
            if response.get("ok"):
                logger.info(f"✓ Approved join request: user {user_id} ({username}) → group {chat_id}")
                
                # Update request status in database
                self.db.update_join_request_status_by_user_chat(user_id, chat_id, 'approved')
                return True
            else:
                error = response.get("error", "Unknown error")
                logger.error(f"✗ Failed to approve join request: {error}")
                # Mark as failed for audit
                self.db.update_join_request_status_by_user_chat(user_id, chat_id, 'failed')
                return False
        
        except Exception as e:
            logger.error(f"Error processing join request: {e}", exc_info=True)
            # Mark as failed if DB insert already happened
            try:
                self.db.update_join_request_status_by_user_chat(user_id, chat_id, 'failed')
            except Exception:
                pass
            return False
