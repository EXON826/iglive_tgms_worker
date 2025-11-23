"""
Group Message Sender
Handles broadcasting messages to managed groups (Async)
"""
import logging
import asyncio
import time
import uuid
from telegram_api import TelegramAPI
from database import DatabaseManager

logger = logging.getLogger(__name__)


class GroupMessageSender:
    """Handles sending messages to groups with rate limiting (Async)"""
    
    def __init__(self, bot_token: str, db_manager: DatabaseManager):
        self.api = TelegramAPI(bot_token)
        self.db = db_manager
    
    async def send_to_groups(self, photo_url: str = None, caption: str = None, text: str = None, watch_link: str = None, instagram_username: str = None):
        """
        Send message to all active managed groups
        
        Args:
            photo_url: URL of photo to send (optional)
            caption: Caption for photo (optional)
            text: Text message if no photo (optional)
            watch_link: The link to include in buttons
            instagram_username: The username for tracking notifications
        """
        # Get all active groups
        groups = await self.db.get_active_managed_groups()
        if not groups:
            logger.info("No active managed groups found")
            return {"success": 0, "total": 0}
        
        logger.info(f"Starting broadcast to {len(groups)} groups for {instagram_username}")
        
        success_count = 0
        failed_count = 0
        
        # Prepare inline keyboard
        reply_markup = None
        if watch_link:
            reply_markup = {
                "inline_keyboard": [[
                    {"text": "🚀 JOIN LIVE", "url": watch_link}
                ]]
            }
        
        for group in groups:
                        
                        # Log success
                        await self.db.log_sent_message(group_id, message_id, debug_code, session=session)
                        
                        # Save notification ID for future deletion
                        if instagram_username:
                            try:
                                await self.db.save_notification(group_id, instagram_username, message_id, session=session)
                            except Exception as e:
                                logger.error(f"Failed to save notification ID {message_id} for {instagram_username}: {e}")
                        
                        # Reset failure count
                        await self.db.reset_failure_count(group_id, session=session)
                        
                    else:
                        failed_count += 1
                        error = response.get("description", "Unknown error")
                        logger.error(f"✗ Failed to send to group {group_id}: {error}")
                        
                        # Handle specific errors (e.g. bot kicked)
                        if "Forbidden" in error or "kicked" in error:
                            await self.db.deactivate_group(group_id, reason="Bot kicked", session=session)
                        else:
                            await self.db.increment_failure_count(group_id, session=session)
            
            except Exception as e:
                failed_count += 1
                logger.error(f"Error sending to group {group_id}: {e}")
                # We can't use the session here as it might be broken/rolled back
                await self.db.increment_failure_count(group_id)
            
            # Rate limiting (Async sleep)
            await asyncio.sleep(3)
            
        return {"success": success_count, "total": len(groups)}