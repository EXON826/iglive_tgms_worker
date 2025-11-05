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
        self.max_consecutive_failures = 3
    
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
    
    def send_to_groups(self, photo_url: str = None, caption: str = None, text: str = None):
        """
        Send message to all active managed groups
        
        Args:
            photo_url: URL of photo to send
            caption: Caption for photo
            text: Text message (if no photo)
        
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
        
        logger.info(f"Sending message to {len(groups)} groups")
        
        for group in groups:
            group_id = group["group_id"]
            
            # Check if final message allowed
            if not group.get("final_message_allowed", True):
                logger.debug(f"Skipping group {group_id} - final_message_allowed=False")
                continue
            
            # Apply rate limiting
            self._rate_limit_delay()
            
            # Generate debug code
            debug_code = self._generate_debug_code()
            
            # Add debug code to message
            if caption:
                caption_with_debug = f"{caption}\n\n{debug_code}"
            elif text:
                text = f"{text}\n\n{debug_code}"
            
            # Send message
            try:
                if photo_url:
                    response = self.api.send_photo(
                        chat_id=group_id,
                        photo=photo_url,
                        caption=caption_with_debug if caption else debug_code,
                        parse_mode="Markdown"
                    )
                else:
                    response = self.api.send_message(
                        chat_id=group_id,
                        text=text,
                        parse_mode="Markdown"
                    )
                
                if response.get("ok"):
                    message_id = response.get("result", {}).get("message_id")
                    self.db.log_sent_message(group_id, message_id, debug_code)
                    self.db.reset_failure_count(group_id)
                    results["success"] += 1
                    results["sent_to"].append(group_id)
                    logger.info(f"✓ Sent to group {group_id} ({debug_code})")
                else:
                    raise Exception(response.get("error", "Unknown error"))
                    
            except Exception as e:
                logger.error(f"✗ Failed to send to group {group_id}: {e}")
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
