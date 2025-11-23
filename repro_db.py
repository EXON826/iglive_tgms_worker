import os
import logging
from dotenv import load_dotenv
from database import DatabaseManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load env
dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path=dotenv_path)
DATABASE_URL = os.environ.get('DATABASE_URL')

def test_db_logic():
    db = DatabaseManager(DATABASE_URL)
    
    group_id = -123456789
    username = "test_user_debug"
    debug_code = "DBG:TEST"
    
    logger.info(f"Testing with group_id={group_id}, username={username}")
    
    # 1. Clean up
    with db.get_connection() as conn:
        conn.execute(text("DELETE FROM live_notification_messages WHERE group_id = :gid AND username = :u"), 
                     {"gid": str(group_id), "u": username})
        conn.commit()
    
    # 2. Claim slot (First run)
    logger.info("--- Run 1 ---")
    claimed, msg_id = db.claim_notification_slot(group_id, username, debug_code)
    logger.info(f"Claimed: {claimed}, MsgID: {msg_id}")
    
    # 3. Get last notification (should be None or 0)
    # Note: claim_notification_slot returns the ID *before* this claim, which should be 0 if we just deleted it.
    logger.info(f"Last msg id returned by claim (should be 0): {msg_id}")
    
    # 4. Save notification
    msg_id_1 = 1001
    db.save_notification(group_id, username, msg_id_1)
    logger.info(f"Saved msg_id {msg_id_1}")
    
    # 5. Verify saved
    saved_msg_id = db.get_last_notification(group_id, username)
    logger.info(f"Verified saved msg_id: {saved_msg_id}")
    
    if saved_msg_id != msg_id_1:
        logger.error("FAIL: Message ID not saved correctly!")
        return

    # 6. Claim slot (Second run)
    logger.info("--- Run 2 ---")
    claimed, last_msg_id_2 = db.claim_notification_slot(group_id, username, "DBG:TEST2")
    logger.info(f"Claimed: {claimed}, MsgID: {last_msg_id_2}")
    
    # 7. Get last notification (should be msg_id_1)
    logger.info(f"Last msg id returned by claim (should be {msg_id_1}): {last_msg_id_2}")
    
    if last_msg_id_2 != msg_id_1:
        logger.error(f"FAIL: Expected {msg_id_1}, got {last_msg_id_2}")
        # Check if it was reset to 0
        if last_msg_id_2 == 0:
            logger.error("FAIL: Message ID was reset to 0 by claim_notification_slot!")
    else:
        logger.info("SUCCESS: Message ID preserved!")

    # Clean up
    with db.get_connection() as conn:
        conn.execute(text("DELETE FROM live_notification_messages WHERE group_id = :gid AND username = :u"), 
                     {"gid": str(group_id), "u": username})
        conn.commit()

if __name__ == "__main__":
    from sqlalchemy import text
    test_db_logic()
