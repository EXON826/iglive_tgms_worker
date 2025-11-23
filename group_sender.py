"""
Group message sender with rate limiting
Handles broadcasting to managed groups
"""
import time
import secrets
import logging
from typing import List, Dict, Any
from telegram_api import TelegramAPI
from database import DatabaseManager

logger = logging.getLogger(__name__)


class GroupMessageSender:
    """Sends messages to managed groups with rate limiting"""
    
    def __init__(self, bot_token: str, db_manager: DatabaseManager):
        self.api = TelegramAPI(bot_token)
        self.db = db_manager
        self.rate_limit = 5  # messages per second
        self.last_send_time = 0
        self.max_consecutive_failures = 7
    
    def _generate_debug_code(self) -> str:
        """Generate unique debug code"""
        return f"DBG:{secrets.token_hex(3).upper()}"
    
    def _rate_limit_delay(self):
        """Apply rate limiting"""
        now = time.time()
        time_since_last = now - self.last_send_time
        min_interval = 1.0 / self.rate_limit
        
        if time_since_last < min_interval:
            time.sleep(min_interval - time_since_last)
        
        self.last_send_time = time.time()

    def _create_url_button_markup(self, watch_link: str = None) -> dict:
        """Create inline keyboard markup with URL button"""
        if not watch_link:
            return None
            
        # Create inline keyboard with single URL button
"""
Group message sender with rate limiting
Handles broadcasting to managed groups
"""
import time
import secrets
import logging
from typing import List, Dict, Any
from telegram_api import TelegramAPI
from database import DatabaseManager

logger = logging.getLogger(__name__)


class GroupMessageSender:
    """Sends messages to managed groups with rate limiting"""
    
    def __init__(self, bot_token: str, db_manager: DatabaseManager):
        self.api = TelegramAPI(bot_token)
        self.db = db_manager
        self.rate_limit = 5  # messages per second
        self.last_send_time = 0
        self.max_consecutive_failures = 7
    
    def _generate_debug_code(self) -> str:
        """Generate unique debug code"""
        return f"DBG:{secrets.token_hex(3).upper()}"
    
    def _rate_limit_delay(self):
        """Apply rate limiting"""
        now = time.time()
        time_since_last = now - self.last_send_time
        min_interval = 1.0 / self.rate_limit
        
        if time_since_last < min_interval:
            time.sleep(min_interval - time_since_last)
        
        self.last_send_time = time.time()

    def _create_url_button_markup(self, watch_link: str = None) -> dict:
        """Create inline keyboard markup with URL button"""
        if not watch_link:
            return None
            
        # Create inline keyboard with single URL button
        inline_keyboard = {
            "inline_keyboard": [
                [
                    {
                        "text": "🚀 JOIN LIVE",
                        "url": watch_link
                    }
                ]
            ]
        }
        return inline_keyboard
    
    def send_to_groups(self, photo_url: str = None, caption: str = None, text: str = None, watch_link: str = None, instagram_username: str = None):
        """
        Send message to all active managed groups
        
        Args:
            photo_url: URL of photo to send
            caption: Caption for photo
            text: Text message (if no photo)
            watch_link: URL for watch button (creates inline keyboard)
            instagram_username: Username of the Instagram user going live (for tracking previous messages)
        
        Returns:
            Dict with success count and failed groups
        """
        groups = self.db.get_active_managed_groups()
        results = {
            "total": len(groups),
            "success": 0,
            "failed": [],
            "sent_to": []
        }
        
        # Create inline keyboard markup if watch link is provided
        reply_markup = self._create_url_button_markup(watch_link)
        
        logger.info(f"Sending message to {len(groups)} groups")
        
        for group in groups:
            group_id = group["group_id"]
            if caption:
                caption_with_debug = f"{caption}\n\n[Debug: {debug_code}]"
            elif text:
                text = f"{text}\n\n[Debug: {debug_code}]"
            
            # Send message
            try:
                # Double-check last notification to prevent race conditions
                if instagram_username:
                    new_check_id = self.db.get_last_notification(group_id, instagram_username)
                    if new_check_id and new_check_id != last_msg_id:
                         logger.info(f"Race condition detected for {instagram_username} in {group_id}. Deleting new last_msg_id {new_check_id}")
                         try:
                             self.api.delete_message(group_id, new_check_id)
                         except Exception as e:
                             logger.warning(f"Failed to delete race-condition message {new_check_id}: {e}")

                if photo_url:
                    response = self.api.send_photo(
                        chat_id=group_id,
                        photo=photo_url,
                        caption=caption_with_debug if caption else debug_code,
                        parse_mode="MarkdownV2",
                        reply_markup=reply_markup
                    )
                else:
                    response = self.api.send_message(
                        chat_id=group_id,
                        text=text,
                            logger.error(f"Failed to save notification for {instagram_username} in {group_id}: {e}")

                    self.db.reset_failure_count(group_id)
                    results["success"] += 1
                    results["sent_to"].append(group_id)
                    logger.info(f"✓ Sent to group {group_id} ({debug_code})")
                else:
                    raise Exception(response.get("error", "Unknown error"))
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"✗ Failed to send to group {group_id}: {error_msg}")
                
                # Check for critical Telegram errors
                if "403" in error_msg or "Forbidden" in error_msg or "kicked" in error_msg.lower():
                    self.db.deactivate_group(group_id, f"Critical error: {error_msg}")
                    logger.warning(f"Deactivated group {group_id} immediately due to critical error")
                    results["failed"].append({"group_id": group_id, "error": error_msg})
                    continue

                failure_count = self.db.increment_failure_count(group_id)
                
                # Deactivate after max failures
                if failure_count >= self.max_consecutive_failures:
                    self.db.deactivate_group(group_id, f"3 consecutive failures: {e}")
                    logger.warning(f"Deactivated group {group_id} after {failure_count} failures")
                
                results["failed"].append({"group_id": group_id, "error": str(e)})
            
            # Add spacing between groups
            time.sleep(3)
        
        logger.info(f"Broadcast complete: {results['success']}/{results['total']} successful")
        return results