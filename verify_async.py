import asyncio
import os
import logging
from dotenv import load_dotenv
from database import DatabaseManager
from telegram_api import TelegramAPI

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify():
    # Load env
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../.env'))
    
    bot_token = os.environ.get("TGMS_BOT_TOKEN")
    db_url = os.environ.get("DATABASE_URL")
    
    if not bot_token or not db_url:
        logger.error("Missing credentials")
        return

    logger.info("Initializing Async Components...")
    
    # Init DB
    db = DatabaseManager(db_url)
    
    # Init API
    api = TelegramAPI(bot_token)
    
    try:
        # Test DB Connection
        logger.info("Testing Database Connection...")
        groups = await db.get_active_managed_groups()
        logger.info(f"Database Connection Successful! Found {len(groups)} active groups.")
        
        # Test API Connection
        logger.info("Testing Telegram API Connection...")
        await api.init_session()
        me = await api.get_me()
        if me.get("ok"):
            logger.info(f"API Connection Successful! Bot: {me['result']['username']}")
        else:
            logger.error(f"API Connection Failed: {me}")
            
    except Exception as e:
        logger.error(f"Verification Failed: {e}", exc_info=True)
    finally:
        logger.info("Closing connections...")
        await api.close()
        await db.close()

if __name__ == "__main__":
    asyncio.run(verify())
