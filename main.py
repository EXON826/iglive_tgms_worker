"""
TGMS Worker Main Entry Point
Handles group management and broadcasting jobs
"""
import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from database import DatabaseManager
from telegram_api import TelegramAPI
from group_sender import GroupMessageSender
from join_request_handler import JoinRequestHandler

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [TGMS] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

POLLING_INTERVAL = 2  # seconds


async def process_tgms_job(job, db_manager, telegram_api, group_sender, join_handler):
    """
    Process a TGMS job from the queue
    
    Job types:
    - process_join_request: Auto-approve join requests
    - send_to_groups: Broadcast message to managed groups
    - update_member_counts: Update member counts for all groups
    - kick_inactive_members: Kick inactive members from groups
    """
    job_id = job['job_id']
    job_type = job['job_type']
    # Normalize TGMS job types: 'tgms_process_join_request' -> 'process_join_request'
    if isinstance(job_type, str) and job_type.startswith('tgms_'):
        job_type = job_type[len('tgms_'):]
    payload_data = job.get('payload', '{}')
    
    # Parse payload
    try:
        if isinstance(payload_data, str):
            payload = json.loads(payload_data)
        elif isinstance(payload_data, dict):
            payload = payload_data
        else:
            logger.error(f"Invalid payload for job_id: {job_id}")
            return False
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(f"Invalid JSON payload for job_id: {job_id}. Error: {e}")
        return False
    
    logger.info(f"Processing TGMS job_id: {job_id} of type: {job_type}")
    
    try:
        if job_type == 'process_join_request':
            # Handle chat join request
            chat_join_request = payload.get('chat_join_request', {})
            chat_id = chat_join_request.get('chat', {}).get('id')
            user_id = chat_join_request.get('from', {}).get('id')
            username = chat_join_request.get('from', {}).get('username', '')

            if chat_id and user_id:
                managed_group = db_manager.get_managed_group(chat_id)
                if not managed_group:
                    status = telegram_api.get_bot_member_status(chat_id)
                    if status in {'administrator', 'creator'}:
                        logger.info(f"Bot is admin in {chat_id}; auto-registering group before join handling")
                        try:
                            inviter = chat_join_request.get('from', {})
                            db_manager.upsert_managed_group(
                                group_id=chat_id,
                                title=chat_join_request.get('chat', {}).get('title'),
                                admin_user_id=inviter.get('id'),
                            )
                        except Exception as reg_err:
                            logger.error(f"Failed to auto-register group {chat_id}: {reg_err}", exc_info=True)

                success = await join_handler.process_join_request(
                    chat_id=chat_id,
                    user_id=user_id,
                    username=username
                )
                return success
            else:
                logger.error(f"Missing chat_id or user_id in join request payload")
                return False

        elif job_type == 'register_group':
            my_chat_member = payload.get('my_chat_member', {})
            chat = my_chat_member.get('chat', {})
            new_member = my_chat_member.get('new_chat_member', {})
            inviter = my_chat_member.get('from', {})

            status = new_member.get('status')
            chat_id = chat.get('id')
            title = chat.get('title')
            admin_user_id = inviter.get('id')

            if status not in {'administrator', 'creator'}:
                logger.info("Register group skipped because bot is no longer admin")
                return True

            if not chat_id:
                logger.error("Cannot register group: missing chat id in my_chat_member payload")
                return False

            db_manager.upsert_managed_group(
                group_id=chat_id,
                title=title,
                admin_user_id=admin_user_id,
            )

            try:
                member_count = telegram_api.get_chat_members_count(chat_id)
                if member_count:
                    db_manager.update_member_count(chat_id, member_count)
            except Exception as e:
                logger.warning(f"Could not fetch member count for group {chat_id}: {e}")

            logger.info(f"Registered managed group {chat_id} ({title})")
            return True

        elif job_type == 'send_to_groups':
            # Broadcast message to all managed groups
            photo_url = payload.get('photo_url')
            caption = payload.get('caption')
            text = payload.get('text')
            
            results = group_sender.send_to_groups(
                photo_url=photo_url,
                caption=caption,
                text=text
            )
            
            logger.info(f"Broadcast results: {results['success']}/{results['total']} successful")
            return results['success'] > 0
        
        elif job_type == 'update_member_counts':
            # Update member counts for all active groups
            await update_member_counts(db_manager, telegram_api)
            return True
        
        elif job_type == 'kick_inactive_members':
            # Kick inactive members (implement later)
            logger.info("Kick inactive members job - not yet implemented")
            return True
        
        else:
            logger.warning(f"Unknown TGMS job_type: {job_type}")
            return False
    
    except Exception as e:
        logger.error(f"Error processing TGMS job {job_id}: {e}", exc_info=True)
        return False


async def update_member_counts(db_manager: DatabaseManager, telegram_api: TelegramAPI):
    """Update member counts for all active groups"""
    groups = db_manager.get_active_managed_groups()
    logger.info(f"Updating member counts for {len(groups)} groups")
    
    for group in groups:
        group_id = group['group_id']
        try:
            count = telegram_api.get_chat_members_count(group_id)
            db_manager.update_member_count(group_id, count)
            logger.info(f"Group {group_id}: {count} members")
            await asyncio.sleep(1)  # Rate limiting
        except Exception as e:
            logger.error(f"Failed to update member count for group {group_id}: {e}")


async def worker_main_loop(session_factory, db_manager, telegram_api, group_sender, join_handler, run_once=False):
    """
    Main loop for TGMS worker
    - Fetches pending TGMS jobs from database
    - Processes them
    - Updates job status
    """
    run_once_retries = 0
    while True:
        job_to_process = None
        session = session_factory()
        
        try:
            # --- 1. Fetch and Lock a Job ---
            select_query = text("""
                SELECT * FROM jobs
                WHERE status = 'pending'
                  AND bot_token = :bot_token
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            result = session.execute(select_query, {'bot_token': os.environ.get('TGMS_BOT_TOKEN')}).fetchone()
            
            if result:
                job_to_process = dict(result._mapping)
                update_query = text("""
                    UPDATE jobs
                    SET status = 'processing', updated_at = :now
                    WHERE job_id = :job_id
                """)
                session.execute(update_query, {
                    'now': datetime.now(timezone.utc),
                    'job_id': job_to_process['job_id']
                })
                session.commit()
                logger.info(f"Locked and picked up job_id: {job_to_process['job_id']}")
            
            # Process the job
            if job_to_process:
                success = await process_tgms_job(
                    job_to_process,
                    db_manager,
                    telegram_api,
                    group_sender,
                    join_handler
                )
                
                # Update job status
                retries = job_to_process.get('retries', 0)
                if success:
                    final_status = 'completed'
                else:
                    if retries < 3:
                        final_status = 'pending'  # Retry
                    else:
                        final_status = 'failed'
                
                update_query = text("""
                    UPDATE jobs
                    SET status = :status, retries = :retries, updated_at = :now
                    WHERE job_id = :job_id
                """)
                session.execute(update_query, {
                    'status': final_status,
                    'retries': retries + 1 if not success else retries,
                    'now': datetime.now(timezone.utc),
                    'job_id': job_to_process['job_id']
                })
                session.commit()
                logger.info(f"Job {job_to_process['job_id']} finished with status: {final_status}")
                
                if run_once:
                    break
            else:
                if run_once:
                    if run_once_retries >= 2:
                        logger.info("run_once mode: No job found after retries, exiting")
                        break
                    run_once_retries += 1
                    await asyncio.sleep(1)
                    continue
                
                await asyncio.sleep(POLLING_INTERVAL)
        
        except Exception as e:
            logger.error(f"Error in TGMS worker main loop: {e}", exc_info=True)
            if session.is_active:
                session.rollback()
            await asyncio.sleep(POLLING_INTERVAL * 2)
        finally:
            session.close()


def main(run_once=False):
    """Main entry point for TGMS worker"""
    # Load environment variables
    dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
    load_dotenv(dotenv_path=dotenv_path)
    
    DATABASE_URL = os.environ.get('DATABASE_URL')
    TGMS_BOT_TOKEN = os.environ.get('TGMS_BOT_TOKEN')
    
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environment")
    if not TGMS_BOT_TOKEN:
        raise ValueError("TGMS_BOT_TOKEN not found in environment")
    
    # Create database connection
    try:
        engine = create_engine(DATABASE_URL)
        SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        logger.info("Database engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}", exc_info=True)
        exit(1)
    
    # Initialize components
    db_manager = DatabaseManager(DATABASE_URL)
    telegram_api = TelegramAPI(TGMS_BOT_TOKEN)
    group_sender = GroupMessageSender(TGMS_BOT_TOKEN, db_manager)
    
    # Import join request handler
    from join_request_handler import JoinRequestHandler
    join_handler = JoinRequestHandler(TGMS_BOT_TOKEN, db_manager)
    
    logger.info("TGMS Worker starting...")
    logger.info("Handles: Group management, join requests, broadcasting")
    
    # Run worker
    try:
        asyncio.run(worker_main_loop(
            SessionFactory,
            db_manager,
            telegram_api,
            group_sender,
            join_handler,
            run_once=run_once
        ))
    except KeyboardInterrupt:
        logger.info("TGMS worker stopped by user")
    finally:
        db_manager.close()


if __name__ == '__main__':
    main()
