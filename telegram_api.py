"""
Simplified Telegram API handler for TGMS worker (Async)
"""
import logging
import aiohttp
import asyncio

logger = logging.getLogger(__name__)


class TelegramAPI:
    """Simple Telegram Bot API wrapper (Async)"""
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.session = None
        self.bot_id = None

    async def init_session(self):
        """Initialize aiohttp session"""
        if not self.session:
            self.session = aiohttp.ClientSession()
            await self.refresh_bot_identity()

    async def close(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def _request(self, method: str, **kwargs):
        """Make API request with error handling"""
        if not self.session:
            await self.init_session()
            
        url = f"{self.base_url}/{method}"
        try:
            async with self.session.post(url, json=kwargs, timeout=30) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"API request failed: {method} - {e}")
            return {"ok": False, "error": str(e)}

    async def get_me(self):
        """Fetch basic bot information."""
        return await self._request("getMe")

    async def refresh_bot_identity(self):
        """Ensure bot_id is cached for subsequent requests."""
        result = await self.get_me()
        if result.get("ok"):
            self.bot_id = result["result"].get("id")
        else:
            logger.warning(f"Could not refresh bot identity: {result.get('error')}")

    async def send_message(self, chat_id: int, text: str, reply_markup=None, **kwargs):
        """Send text message with optional inline keyboard"""
        payload = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        payload.update(kwargs)
        return await self._request("sendMessage", **payload)
    
    async def send_photo(self, chat_id: int, photo: str, reply_markup=None, **kwargs):
        """Send photo with optional inline keyboard"""
        payload = {"chat_id": chat_id, "photo": photo}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        payload.update(kwargs)
        return await self._request("sendPhoto", **payload)
    
    async def approve_join_request(self, chat_id: int, user_id: int):
        """Approve chat join request"""
        return await self._request("approveChatJoinRequest", chat_id=chat_id, user_id=user_id)
    
    async def decline_join_request(self, chat_id: int, user_id: int):
        """Decline chat join request"""
        return await self._request("declineChatJoinRequest", chat_id=chat_id, user_id=user_id)
    
    async def kick_member(self, chat_id: int, user_id: int):
        """Kick (ban) member from chat"""
        return await self._request("banChatMember", chat_id=chat_id, user_id=user_id)
    
    async def get_chat_members_count(self, chat_id: int):
        """Get member count (uses getChatMemberCount with fallback)"""
        result = await self._request("getChatMemberCount", chat_id=chat_id)
        if not result.get("ok"):
            # Fallback to legacy/misspelled variant if any
            result = await self._request("getChatMembersCount", chat_id=chat_id)
        return result.get("result", 0) if result.get("ok") else 0
    
    async def delete_message(self, chat_id: int, message_id: int):
        """Delete message"""
        return await self._request("deleteMessage", chat_id=chat_id, message_id=message_id)

    async def get_chat_member(self, chat_id: int, user_id: int):
        """Retrieve membership info for a user within a chat."""
        return await self._request("getChatMember", chat_id=chat_id, user_id=user_id)

    async def get_bot_member_status(self, chat_id: int):
        """Return the bot's status (administrator/member/etc.) for a chat."""
        if not self.bot_id:
            await self.refresh_bot_identity()
        if not self.bot_id:
            logger.error("Bot ID unavailable; cannot determine membership status")
            return None

        result = await self.get_chat_member(chat_id, self.bot_id)
        if result.get("ok"):
            return result["result"].get("status")

        logger.warning(f"getChatMember failed for chat {chat_id}: {result.get('error')}")
        return None
