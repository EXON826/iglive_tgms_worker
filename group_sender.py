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
            group_id = group['group_id']
            debug_code = str(uuid.uuid4())[:8]
            
            # Claim notification slot (Locking)
            if instagram_username:
                claimed, last_msg_id = await self.db.claim_notification_slot(group_id, instagram_username, debug_code)
                if not claimed:
                    logger.info(f"Skipping group {group_id} for {instagram_username} - Notification slot locked")
                    continue

                # Delete previous notification
                if last_msg_id and last_msg_id > 0:
                    try:
                        delete_response = await self.api.delete_message(group_id, last_msg_id)
                        if delete_response.get("ok"):
                            logger.debug(f"Deleted previous notification {last_msg_id} for {instagram_username} in {group_id}")
                            try:
                                await self.db.log_deleted_message(group_id, last_msg_id, instagram_username)
                            except Exception as e:
                                logger.error(f"Failed to log deletion of {last_msg_id}: {e}")
                        else:
                            logger.warning(f"Failed to delete previous message {last_msg_id} in {group_id}: {delete_response.get('description')}")
                    except Exception as e:
                        logger.warning(f"Failed to delete previous message {last_msg_id} in {group_id}: {e}")

            # Send new message
            try:
                if photo_url:
                    response = await self.api.send_photo(
                        chat_id=group_id,
                        photo=photo_url,
                        caption=caption,
                        parse_mode="MarkdownV2",
                        reply_markup=reply_markup
                    )
                else:
                    response = await self.api.send_message(
                        chat_id=group_id,
                        text=text,
                        parse_mode="MarkdownV2",
                        reply_markup=reply_markup
                    )
                
                if response.get("ok"):
                    message_id = response['result']['message_id']
                    success_count += 1
                    logger.info(f"✓ Sent to group {group_id} (msg_id: {message_id})")
                    
                    # Log success
                    await self.db.log_sent_message(group_id, message_id, debug_code)
                    
                    # Save notification ID for future deletion
                    if instagram_username:
                        try:
                            await self.db.save_notification(group_id, instagram_username, message_id)
                        except Exception as e:
                            logger.error(f"Failed to save notification ID {message_id} for {instagram_username}: {e}")
                    
                    # Reset failure count
                    await self.db.reset_failure_count(group_id)
                    
                else:
                    failed_count += 1
                    error = response.get("description", "Unknown error")
                    logger.error(f"✗ Failed to send to group {group_id}: {error}")
                    
                    # Handle specific errors (e.g. bot kicked)
                    if "Forbidden" in error or "kicked" in error:
                        await self.db.deactivate_group(group_id, reason="Bot kicked")
                    else:
                        await self.db.increment_failure_count(group_id)
            
            except Exception as e:
                failed_count += 1
                logger.error(f"Error sending to group {group_id}: {e}")
                await self.db.increment_failure_count(group_id)
            
            # Rate limiting (Async sleep)
            await asyncio.sleep(3)
            
        return {"success": success_count, "total": len(groups)}