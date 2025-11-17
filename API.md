# TGMS API Reference

## Broadcasting Endpoint

### POST `/api/tgms/send`

Broadcast a message to all active managed groups.

#### Authentication
```
x-api-key: <ADMIN_API_KEY>
```

#### Request Body

**Text-only message:**
```json
{
  "text": "Your message here with *Markdown* support"
}
```

**Photo with caption:**
```json
{
  "photo_url": "https://example.com/image.jpg",
  "caption": "Photo caption with *Markdown*"
}
```

**Photo without caption:**
```json
{
  "photo_url": "https://example.com/image.jpg"
}
```

#### Response

**Success:**
```json
{
  "status": "ok",
  "message": "Broadcast enqueued"
}
```

**Unauthorized:**
```json
{
  "status": "error",
  "message": "Unauthorized"
}
```

**Server Error:**
```json
{
  "status": "error",
  "message": "Failed to enqueue"
}
```

#### Example

```bash
curl -X POST https://your-app.vercel.app/api/tgms/send \
  -H "x-api-key: your_admin_key_here" \
  -H "Content-Type: application/json" \
  -d '{
    "photo_url": "https://example.com/promo.jpg",
    "caption": "🎉 *New Feature Alert!*\n\nCheck out our latest update!"
  }'
```

#### Python Example

```python
import requests

url = "https://your-app.vercel.app/api/tgms/send"
headers = {
    "x-api-key": "your_admin_key_here",
    "Content-Type": "application/json"
}
payload = {
    "text": "Hello from TGMS! 👋"
}

response = requests.post(url, json=payload, headers=headers)
print(response.json())
```

---

## Job Types

Jobs in the `jobs` table with type starting with `tgms_` are processed by TGMS worker:

### `tgms_process_join_request`
Auto-approve a join request.

**Payload:**
```json
{
  "chat_join_request": {
    "chat": {"id": -1001234567890},
    "from": {"id": 123456789, "username": "john_doe"}
  }
}
```

### `tgms_send_to_groups`
Broadcast to all managed groups.

**Payload:**
```json
{
  "text": "Message text",
  "photo_url": "https://example.com/photo.jpg",
  "caption": "Photo caption"
}
```

### `tgms_update_member_counts`
Update member counts for all groups.

**Payload:**
```json
{}
```

### `tgms_kick_inactive_members`
Kick inactive members (not yet implemented).

**Payload:**
```json
{}
```

---

## Database Operations

### Get Active Groups

```python
from database import DatabaseManager

db = DatabaseManager(DATABASE_URL)
groups = db.get_active_managed_groups()

for group in groups:
    print(f"Group {group['group_id']}: {group['title']}")
    print(f"  Members: {group['member_count']}")
    print(f"  Phase: {group['phase']}")
```

### Add New Group

```sql
INSERT INTO managed_groups (
    group_id, 
    admin_user_id, 
    title, 
    phase, 
    is_active, 
    final_message_allowed
) VALUES (
    -1001234567890,  -- Group ID
    123456789,        -- Admin user ID
    'Test Group',     -- Group title
    'growth',         -- Phase: growth or monitoring
    true,             -- Is active
    true              -- Broadcasting allowed
);
```

### Check Recent Join Requests

```sql
SELECT 
    jr.request_id,
    jr.user_id,
    jr.username,
    jr.chat_id,
    jr.status,
    jr.created_at,
    mg.title as group_name
FROM join_requests jr
LEFT JOIN managed_groups mg ON jr.chat_id = mg.group_id
ORDER BY jr.created_at DESC
LIMIT 10;
```

### Check Broadcast Status

```sql
SELECT 
    sm.message_id,
    sm.chat_id,
    mg.title as group_name,
    sm.debug_code,
    sm.sent_at
FROM sent_messages sm
LEFT JOIN managed_groups mg ON sm.chat_id = mg.group_id
ORDER BY sm.sent_at DESC
LIMIT 10;
```

---

## Rate Limits

- **Messages per second:** 5
- **Delay between groups:** 3 seconds
- **Retry attempts:** 3
- **Auto-deactivation:** After 3 consecutive failures

---

## Debug Codes

Every broadcast message includes a unique debug code in the format: `DBG:XXXXXX`

Example: `DBG:A3F2E1`

Use this code to track messages in the `sent_messages` table:

```sql
SELECT * FROM sent_messages WHERE debug_code = 'DBG:A3F2E1';
```

---

## Health Monitoring

### Check Worker Status

```sql
SELECT * FROM bot_health WHERE bot_name = 'tgms_worker';
```

### Check Pending Jobs

```sql
SELECT 
    job_id, 
    job_type, 
    status, 
    retries, 
    created_at,
    updated_at
FROM jobs 
WHERE job_type LIKE 'tgms_%' 
  AND status != 'completed'
ORDER BY created_at;
```

### Check Failed Jobs

```sql
SELECT 
    job_id,
    job_type,
    payload,
    retries,
    created_at
FROM jobs 
WHERE job_type LIKE 'tgms_%' 
  AND status = 'failed'
ORDER BY created_at DESC
LIMIT 10;
```

---

## Error Codes

### Broadcasting Errors

**"Group not active"**
- Group has `is_active = false`
- Check: `SELECT * FROM managed_groups WHERE group_id = <id>`

**"Bot is not admin"**
- TGMS bot lacks admin permissions
- Fix: Make bot admin in group settings

**"Chat not found"**
- Group ID is invalid or bot was removed
- Action: Deactivate group in database

**"Too many requests"**
- Rate limit exceeded
- Worker will automatically retry

### Join Request Errors

**"Chat not found"**
- Group doesn't exist in `managed_groups` table
- Action: Add group to database

**"User not found"**
- User blocked the bot or deleted account
- Action: Request is automatically marked as failed

---

## Permissions Required

### TGMS Bot Permissions (in each group)

- ✅ **Add Members** (required for approving join requests)
- ✅ **Send Messages** (required for broadcasting)
- ✅ **Send Photos** (required for photo broadcasts)
- ⚠️ **Delete Messages** (optional, for cleanup)
- ⚠️ **Ban Users** (optional, for kicking inactive members)

---

## Testing

### Test Join Request Flow

1. Add bot to a group as admin with "Add Members" permission
2. Make group invite-only (approval required)
3. Insert group into database:
   ```sql
   INSERT INTO managed_groups (group_id, admin_user_id, title, phase, is_active)
   VALUES (-1001234567890, 123456789, 'Test Group', 'growth', true);
   ```
4. Request to join from a test account
5. Check logs: Request should be auto-approved
6. Verify in database:
   ```sql
   SELECT * FROM join_requests WHERE chat_id = -1001234567890 ORDER BY created_at DESC LIMIT 1;
   ```

### Test Broadcasting

```bash
# Test text broadcast
curl -X POST http://localhost:8000/api/tgms/send \
  -H "x-api-key: test_key" \
  -H "Content-Type: application/json" \
  -d '{"text": "Test broadcast 🚀"}'

# Test photo broadcast
curl -X POST http://localhost:8000/api/tgms/send \
  -H "x-api-key: test_key" \
  -H "Content-Type: application/json" \
  -d '{
    "photo_url": "https://picsum.photos/800/600",
    "caption": "Test photo broadcast 📸"
  }'
```

---

## Troubleshooting

### Broadcasts not sending

1. Check groups are active:
   ```sql
   SELECT * FROM managed_groups WHERE is_active = true;
   ```

2. Check bot token is valid:
   ```bash
   curl "https://api.telegram.org/bot<TGMS_BOT_TOKEN>/getMe"
   ```

3. Check pending jobs:
   ```sql
   SELECT * FROM jobs WHERE job_type = 'tgms_send_to_groups' AND status = 'pending';
   ```

4. Check worker is running:
   ```bash
   ps aux | grep tgms_worker
   ```

### Join requests not auto-approved

1. Verify group exists in database and is active
2. Verify bot is admin with "Add Members" permission
3. Check webhook is receiving updates:
   ```bash
   curl "https://api.telegram.org/bot<TGMS_BOT_TOKEN>/getWebhookInfo"
   ```
4. Check join_requests table for failed attempts

---

## Security Best Practices

1. ✅ Store `ADMIN_API_KEY` securely (use secrets manager)
2. ✅ Rotate API keys regularly
3. ✅ Use HTTPS for all API calls
4. ✅ Never commit `.env` files to git
5. ✅ Restrict `/api/tgms/send` to trusted IPs only (use firewall rules)
6. ✅ Monitor `jobs` table for suspicious activity
7. ✅ Set up alerts for failed jobs

---

## Version

**TGMS API Version:** 1.0  
**Last Updated:** 2025-10-10
