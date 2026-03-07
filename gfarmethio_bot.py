#!/usr/bin/env python3
"""
gfarmethio_bot.py
Two-stage approval (screenshot -> hold -> final admin approval -> balance)
+ one-task-per-user locking (1 hour lock timeout), automatic lock expiry,
+ delete task permanently after final approval.

Usage:
  export TELEGRAM_BOT_TOKEN="..."
  export ADMIN_ID="12345678"
  python gfarmethio_bot.py

Requires: python-telegram-bot v20+, Python 3.9+
"""
import os
import json
import logging
import time
import uuid
import random
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)

# ---------------- CONFIG ----------------
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8597916092:AAEcJE5CdT0ptTHDTkKx3rKu8zRE4Z2WbxI")
ADMIN_ID = os.environ.get("ADMIN_ID", "5535222774")
ADMIN_ID = str(int(ADMIN_ID))
DATA_FILE = "bot_data.json"
BACKUP_DIR = "backups"

MIN_WITHDRAW = 200.0
TEST_MODE = False
HOLD_SECONDS = 10 if TEST_MODE else 3 * 24 * 3600   # production: 3 days
HOLD_CHECK_INTERVAL = 10 if TEST_MODE else 3600     # how often job_release runs

REFERRAL_PERCENT = 0.10
SUPPORT_USERNAME = "T1N4E"  # without '@'

# Lock settings: one task locked to a user for this many seconds (1 hour rule)
LOCK_TIMEOUT_SECONDS = 60 * 60  # 1 hour

MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["Tasks", "Balance", "History"],
        ["Withdraw", "Customer Support", "Referral"],
        ["Help"]
    ],
    resize_keyboard=True,
)

MENU_KEYWORDS = {"tasks", "balance", "history", "withdraw", "customer support", "tutorial", "referral", "help", "cancel", "submit task", "submit"}

# ---------------- AddTask Conversation states (kept for optional flow) ----------------
ADD_FIELD_NAME, ADD_FIELD_VALUE = range(2)

# ---------------- Logging ----------------
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- Data model ----------------
DEFAULT_DATA = {
    "users": {},
    "tasks": {},
    "submissions": {},            # submission records
    "pending_proofs_by_user": {}, # uid -> submission_id waiting for photo
    "holds": {},                  # hold entries for admin review/final
    "withdrawals": {},
    "withdraw_states": {},
    "transactions": [],
    "audit": [],
    "task_views": {},             # uid -> {task_id, ts, min_time}
    "task_locks": {},             # task_id -> {"user_id": uid, "locked_at": ts}
}
_data: Dict[str, Any] = {}

# ---------------- Persistence ----------------
def load_data():
    global _data
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                _data = json.load(f)
        except Exception:
            logger.exception("Failed to load data file; starting fresh.")
            _data = DEFAULT_DATA.copy()
    else:
        _data = DEFAULT_DATA.copy()
    # ensure keys exist
    for k, v in DEFAULT_DATA.items():
        if k not in _data:
            _data[k] = v.copy() if isinstance(v, dict) else v

def save_data():
    tmp = DATA_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, DATA_FILE)
    except Exception:
        logger.exception("Failed to save data; attempting backup.")
        try:
            os.makedirs(BACKUP_DIR, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            with open(os.path.join(BACKUP_DIR, f"backup_{ts}.json"), "w", encoding="utf-8") as fb:
                json.dump(_data, fb, indent=2, ensure_ascii=False)
        except Exception:
            logger.exception("Backup also failed.")

def audit(event: str, actor: str, details: dict = None):
    _data.setdefault("audit", []).append({
        "id": str(uuid.uuid4()),
        "ts": int(time.time()),
        "actor": actor,
        "event": event,
        "details": details or {}
    })
    save_data()

# ---------------- Helpers ----------------
def uid_key(uid: int) -> str:
    return str(uid)

def ensure_user(user) -> str:
    uid = uid_key(user.id)
    users = _data.setdefault("users", {})
    if uid not in users:
        users[uid] = {
            "telegram_id": uid,
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "username": (user.username or "").lstrip("@"),
            "referrer": None,
            "referrals": [],
            "referred_count": 0,
            "balance": 0.0,
            "holds": [],                  # list of hold_ids
            "statistics": {"submitted": 0, "approved": 0, "rejected": 0},
        }
        audit("user_created", uid, {"name": users[uid]["first_name"]})
        save_data()
    else:
        users[uid].setdefault("username", (user.username or "").lstrip("@"))
    return uid

def create_task(title: str, reward: float, description: str, creator: str, howto: Optional[str] = None) -> str:
    task_id = str(uuid.uuid4())
    _data.setdefault("tasks", {})[task_id] = {
        "task_id": task_id,
        "title": title,
        "reward": float(reward),
        "description": description,
        "published": True,
        "created_by": creator,
        "created_at": int(time.time()),
        "howto": howto,
    }
    audit("task_created", creator, {"task_id": task_id, "title": title})
    save_data()
    return task_id

def delete_task(task_id: str) -> bool:
    tasks = _data.get("tasks", {})
    if task_id not in tasks:
        return False
    # remove locks for this task
    _data.setdefault("task_locks", {}).pop(task_id, None)
    # Mark related submissions as rejected and alert users + clear pending_proofs_by_user
    affected_subs = []
    for sid, s in list(_data.get("submissions", {}).items()):
        if s.get("task_id") == task_id and s.get("status") in ("awaiting_proof", "pending", "approved_stage1", "waiting_final"):
            s["status"] = "rejected"
            s["admin_note"] = "task_deleted"
            s["decision_at"] = int(time.time())
            affected_subs.append((sid, s.get("user_id")))
    # remove pending proof mappings that reference these submissions and notify users
    for uid, pending_sid in list(_data.get("pending_proofs_by_user", {}).items()):
        sub = _data.get("submissions", {}).get(pending_sid)
        if sub and sub.get("task_id") == task_id:
            _data["pending_proofs_by_user"].pop(uid, None)
            # notify user
            try:
                # transport-safe attempt
                from telegram import Bot
                bot = Bot(TOKEN)
                bot.send_message(int(uid), "⚠️ The task you were completing was deleted by admin. Your pending submission was cancelled.")
            except Exception:
                pass
    # delete the task
    del _data["tasks"][task_id]
    audit("task_deleted", ADMIN_ID, {"task_id": task_id})
    save_data()
    return True

def create_submission(user_id: str, task_id: str) -> str:
    sub_id = str(uuid.uuid4())
    _data.setdefault("submissions", {})[sub_id] = {
        "submission_id": sub_id,
        "user_id": user_id,
        "task_id": task_id,
        "status": "awaiting_proof",
        "created_at": int(time.time()),
        "decision_at": None,
        "admin_note": None,
    }
    _data.setdefault("users", {}).setdefault(user_id, {}).setdefault("statistics", {"submitted": 0, "approved": 0, "rejected": 0})
    _data["users"][user_id]["statistics"]["submitted"] = _data["users"][user_id]["statistics"].get("submitted", 0) + 1
    audit("submission_created", user_id, {"submission_id": sub_id, "task_id": task_id})
    save_data()
    return sub_id

def create_hold_for_proof(sub_id: str, user_id: str, task_id: str, amount: float, photo_file_id: str) -> str:
    hid = str(uuid.uuid4())
    now = int(time.time())
    _data.setdefault("holds", {})[hid] = {
        "hold_id": hid,
        "sub_id": sub_id,
        "user_id": user_id,
        "task_id": task_id,
        "amount": float(amount),
        "photo_file_id": photo_file_id,
        "status": "pending",  # pending until admin first approves/rejects
        "created_at": now,
        # approved_at / release_at set when admin does first approval
    }
    _data.setdefault("users", {}).setdefault(user_id, {}).setdefault("holds", []).append(hid)
    # mark submission pending (proof submitted)
    sub = _data.get("submissions", {}).get(sub_id)
    if sub:
        sub["status"] = "pending"
    save_data()
    audit("hold_created", ADMIN_ID, {"hold_id": hid, "user": user_id, "amount": amount})
    return hid

def approve_initial_stage(hid: str) -> bool:
    """
    Admin initial approval: set hold to approved_stage1, set release_at = now + HOLD_SECONDS.
    This does NOT credit balance. Referral hold is created here (approved_stage1) with same release_at.
    """
    h = _data.get("holds", {}).get(hid)
    if not h or h.get("status") != "pending":
        return False
    now = int(time.time())
    h["status"] = "approved_stage1"
    h["approved_at"] = now
    h["release_at"] = now + HOLD_SECONDS
    # update submission
    sub = _data.get("submissions", {}).get(h.get("sub_id"))
    if sub:
        sub["status"] = "approved_stage1"
        sub["decision_at"] = now
        sub["admin_note"] = "approved_stage1"
        _data.setdefault("users", {}).setdefault(h["user_id"], {}).setdefault("statistics", {})
        _data["users"][h["user_id"]]["statistics"]["approved"] = _data["users"][h["user_id"]]["statistics"].get("approved", 0) + 1
    # Create referral hold (same release_at) if ref exists
    ref = _data.get("users", {}).get(h["user_id"], {}).get("referrer")
    if ref:
        ref_amount = round(h["amount"] * REFERRAL_PERCENT, 2)
        if ref_amount > 0:
            ref_hid = str(uuid.uuid4())
            _data.setdefault("holds", {})[ref_hid] = {
                "hold_id": ref_hid,
                "sub_id": h.get("sub_id"),
                "user_id": ref,
                "task_id": h.get("task_id"),
                "amount": float(ref_amount),
                "photo_file_id": None,
                "status": "approved_stage1",
                "created_at": now,
                "approved_at": now,
                "release_at": h["release_at"],
            }
            _data.setdefault("users", {}).setdefault(str(ref), {}).setdefault("holds", []).append(ref_hid)
            record_transaction(str(ref), "referral_hold", ref_amount, origin=h.get("sub_id"), status="scheduled")
    save_data()
    audit("hold_approved_stage1", ADMIN_ID, {"hold": hid, "release_at": h["release_at"]})
    return True

def reject_hold(hid: str) -> bool:
    h = _data.get("holds", {}).get(hid)
    if not h or h.get("status") not in ("pending", "approved_stage1", "waiting_final"):
        return False
    h["status"] = "rejected"
    h["rejected_at"] = int(time.time())
    sub = _data.get("submissions", {}).get(h.get("sub_id"))
    if sub:
        sub["status"] = "rejected"
        sub["decision_at"] = int(time.time())
        sub["admin_note"] = "rejected_by_admin"
        _data.setdefault("users", {}).setdefault(h["user_id"], {}).setdefault("statistics", {})
        _data["users"][h["user_id"]]["statistics"]["rejected"] = _data["users"][h["user_id"]]["statistics"].get("rejected", 0) + 1
    # release lock for the task if any and not completed
    task_id = h.get("task_id")
    if task_id:
        locks = _data.setdefault("task_locks", {})
        lock = locks.get(task_id)
        if lock and lock.get("user_id") == h.get("user_id"):
            locks.pop(task_id, None)
    save_data()
    audit("hold_rejected", ADMIN_ID, {"hold": hid})
    return True

def final_approve_and_credit(hid: str) -> bool:
    """
    Called when admin does FINAL approve after waiting window.
    Credits user's balance and marks hold 'released'.
    Also permanently deletes the task from pool so it never shows again.
    """
    h = _data.get("holds", {}).get(hid)
    if not h or h.get("status") not in ("waiting_final", "approved_stage1"):
        return False
    uid = h["user_id"]
    amt = h["amount"]
    _data.setdefault("users", {}).setdefault(uid, {}).setdefault("balance", 0.0)
    _data["users"][uid]["balance"] += amt
    h["status"] = "released"
    h["released_at"] = int(time.time())
    # update submission if present
    sub = _data.get("submissions", {}).get(h.get("sub_id"))
    if sub:
        sub["status"] = "released"
        sub["decision_at"] = int(time.time())
        sub["admin_note"] = "released_to_balance"
    # record transaction
    record_transaction(uid, "reward_release", amt, origin=h.get("sub_id"), status="credited")
    # permanently remove task from pool so it never shows again
    task_id = h.get("task_id")
    if task_id and task_id in _data.get("tasks", {}):
        # remove locks
        _data.setdefault("task_locks", {}).pop(task_id, None)
        # delete task
        del _data["tasks"][task_id]
        audit("task_removed_after_completion", ADMIN_ID, {"task_id": task_id, "hold": hid})
    save_data()
    audit("hold_final_approved", ADMIN_ID, {"hold": hid, "amount": amt})
    return True

def record_transaction(user_id: str, ttype: str, amount: float, origin: Optional[str] = None, status: str = "credited") -> str:
    tx_id = str(uuid.uuid4())
    _data.setdefault("transactions", []).append({
        "tx_id": tx_id,
        "user_id": user_id,
        "type": ttype,
        "amount": float(amount),
        "status": status,
        "ts": int(time.time()),
        "origin": origin,
    })
    save_data()
    return tx_id

# ---------------- Task helpers (with locking) ----------------
def release_expired_locks():
    """Free locks that have expired."""
    now = int(time.time())
    locks = _data.setdefault("task_locks", {})
    expired = []
    for tid, lock in list(locks.items()):
        if now - lock.get("locked_at", 0) >= LOCK_TIMEOUT_SECONDS:
            expired.append(tid)
    for tid in expired:
        locks.pop(tid, None)
        audit("task_lock_expired", "system", {"task_id": tid})
    if expired:
        save_data()
    return expired

def find_next_task_for_user(uid: str):
    """
    Find next available task for user and lock it to them.
    Skips tasks locked by others (unless lock expired).
    Skips tasks that are finished (final released).
    Also avoids assigning the same task to the same user twice.
    """
    tasks = _data.get("tasks", {})
    published = sorted([t for t in tasks.values() if t.get("published", True)], key=lambda x: x.get("created_at", 0))
    now = int(time.time())
    locks = _data.setdefault("task_locks", {})

    for t in published:
        tid = t["task_id"]

        # Skip if task has been finally released (completed) by anyone
        already_final = any(
            s.get("task_id") == tid and s.get("status") == "released"
            for s in _data.get("submissions", {}).values()
        )
        if already_final:
            continue

        # Skip if this same user already submitted this task (do not allow duplicate same-task submissions)
        user_already_submitted = any(
            s.get("task_id") == tid and s.get("user_id") == uid
            for s in _data.get("submissions", {}).values()
        )
        if user_already_submitted:
            continue

        lock = locks.get(tid)
        if lock:
            # expired?
            if now - lock.get("locked_at", 0) >= LOCK_TIMEOUT_SECONDS:
                # free it
                locks.pop(tid, None)
                save_data()
                lock = None
            else:
                # locked by someone else? skip
                if lock.get("user_id") != uid:
                    continue
                # else locked by same user -> return it
                return t

        # Lock for this user and return
        locks[tid] = {"user_id": uid, "locked_at": now}
        save_data()
        audit("task_locked", uid, {"task_id": tid})
        return t
    return None

def _format_task_display(task: Dict[str, Any]) -> str:
    """
    Return the exact text that should be shown to users for a task.
    Behavior:
      - If task.meta.raw_text exists -> normalize Reward line and return cleaned raw_text
      - Else fallback to compact text built from title/reward/description
    Avoid double reward lines and normalize formats like "Reward:35.00ETB" etc.
    """
    # helper to find reward value
    reward_val = 0.0
    if "reward" in task:
        try:
            reward_val = float(task.get("reward", 0.0))
        except Exception:
            reward_val = 0.0
    else:
        meta = task.get("meta") or {}
        for k in ("reward", "Reward", "amount", "Amount"):
            if k in meta:
                try:
                    reward_val = float(re.sub(r"[^\d\.]", "", str(meta[k])))
                    break
                except Exception:
                    pass

    formatted_reward_line = f"Reward: {reward_val:.2f} ETB"

    raw = None
    if isinstance(task.get("meta"), dict):
        raw = task["meta"].get("raw_text")
    if not raw:
        raw = task.get("text")
    if raw:
        # Remove any existing Reward: lines then insert a single normalized Reward line
        lines = raw.splitlines()
        filtered = [ln for ln in lines if not re.match(r"\s*Reward\s*:", ln, flags=re.IGNORECASE)]
        # insert the formatted reward after first non-empty line if present, else at top
        if filtered:
            # find first non-empty index
            for i, ln in enumerate(filtered):
                if ln.strip():
                    insert_idx = i + 1
                    break
            else:
                insert_idx = 0
            filtered.insert(insert_idx, formatted_reward_line)
        else:
            filtered = [formatted_reward_line]
        new = "\n".join(filtered).strip()
        # Trim repetitive whitespace
        new = re.sub(r"\n\s+\n", "\n\n", new)
        return new
    # fallback
    title = task.get("title", "").strip()
    desc = task.get("description", "").strip()
    out_lines = []
    if title:
        out_lines.append(title)
    out_lines.append(f"Reward: {reward_val:.2f} ETB")
    if desc:
        out_lines.append(desc)
    return "\n".join(out_lines).strip()

async def send_next_task_to_user(uid: str, bot):
    next_task = find_next_task_for_user(uid)
    if not next_task:
        try:
            await bot.send_message(int(uid), "You have no available tasks right now. ✅")
        except Exception:
            pass
        return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("i complete ✅", callback_data="complete:" + next_task['task_id'])],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_task")],
        [InlineKeyboardButton("❓ How To", callback_data="howto:" + next_task['task_id'])],
    ])

    # Use the strict display text (raw_text normalized) so nothing extra is added
    text = _format_task_display(next_task)

    try:
        _data.setdefault("task_views", {})[str(uid)] = {
            "task_id": next_task['task_id'],
            "ts": int(time.time()),
            "min_time": random.randint(12, 20)
        }
        save_data()
        await bot.send_message(int(uid), text, reply_markup=kb)
    except Exception:
        pass

# ---------------- Bot handlers ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = ensure_user(user)
    if context.args:
        r = context.args[0].strip()
        ref_uid = None
        if r.isdigit():
            if r in _data.get("users", {}) and r != uid:
                ref_uid = r
        else:
            uname = r.lstrip("@")
            for u_id, rec in _data.get("users", {}).items():
                if rec.get("username") and rec.get("username").lower() == uname.lower() and u_id != uid:
                    ref_uid = u_id
                    break
        if ref_uid and _data["users"][uid].get("referrer") is None:
            _data["users"][uid]["referrer"] = ref_uid
            _data["users"].setdefault(ref_uid, {}).setdefault("referrals", []).append(uid)
            _data["users"][ref_uid]["referred_count"] = _data["users"][ref_uid].get("referred_count", 0) + 1
            audit("referral_used", uid, {"ref": ref_uid})
            save_data()
    await update.message.reply_text("Toggle the Menu: Use the buttons below 👇", reply_markup=MENU_KEYBOARD)

async def show_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = ensure_user(update.effective_user)
    await send_next_task_to_user(uid, context.bot)

# Task callback actions (complete/howto/cancel)
async def callback_task_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data_cb = query.data
    uid = uid_key(query.from_user.id)

    if data_cb == "cancel_task":
        # release lock (if any) for the shown task for this user
        view = _data.get("task_views", {}).get(str(uid))
        if view:
            task_id = view.get("task_id")
            locks = _data.setdefault("task_locks", {})
            lock = locks.get(task_id)
            if lock and lock.get("user_id") == uid:
                locks.pop(task_id, None)
                audit("task_lock_released_by_user", uid, {"task_id": task_id})
                save_data()
            _data["task_views"].pop(str(uid), None)
            save_data()
        try:
            await query.edit_message_text("❌ Task cancelled.")
        except Exception:
            pass
        return

    if data_cb.startswith("howto:"):
        task_id = data_cb.split(":", 1)[1]
        task = _data.get("tasks", {}).get(task_id)
        if not task:
            await query.edit_message_text("Task not found.")
            return
        howto = task.get("howto") or (task.get("meta") or {}).get("How-to") or (task.get("fields") or {}).get("How-to")
        if not howto:
            await query.edit_message_text("No how-to file for this task.")
            return
        try:
            if os.path.exists(howto):
                await context.bot.send_video(chat_id=int(uid), video=open(howto, "rb"),
                                             caption="📘 How To: watch this video and follow steps carefully.")
            else:
                await context.bot.send_video(chat_id=int(uid), video=howto,
                                             caption="📘 How To: watch this video and follow steps carefully.")
        except Exception:
            await context.bot.send_message(int(uid), "Failed to send how-to file. Contact support.")
        return

    if data_cb.startswith("complete:"):
        task_id = data_cb.split(":", 1)[1]
        view = _data.get("task_views", {}).get(str(uid))
        started_at = view.get("ts", 0) if view else 0
        min_time_required = view.get("min_time", 15) if view else 15
        elapsed = int(time.time()) - int(started_at)
        if elapsed < int(min_time_required):
            try:
                await query.answer("❌ You are not complete 🔒 Be sure to use the specified data, otherwise the account will not be paid.", show_alert=True)
            except Exception:
                pass
            return
        # create submission and keep lock (task remains locked to this user)
        sub_id = create_submission(uid, task_id)
        _data.setdefault("pending_proofs_by_user", {})[uid] = sub_id
        save_data()
        try:
            # single simplified message requested
            await context.bot.send_message(int(uid), "Please send a screenshot proof now.")
            await query.edit_message_text("Please send a screenshot proof now.")
        except Exception:
            pass
        return

# Photo handler - proof
async def handle_photo_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = uid_key(update.effective_user.id)
    pending = _data.get("pending_proofs_by_user", {})
    sub_id = pending.get(uid)
    if not sub_id:
        await update.message.reply_text("No proof expected right now. Use Tasks to get a task.")
        return
    photos = update.message.photo or []
    if not photos:
        await update.message.reply_text("Please send a photo (screenshot).")
        return
    file_id = photos[-1].file_id
    sub = _data.get("submissions", {}).get(sub_id)
    if not sub:
        # cleanup stale mapping and inform user
        _data["pending_proofs_by_user"].pop(uid, None)
        save_data()
        await update.message.reply_text("Submission not found or task deleted. Use Tasks to get a new task.")
        return
    task = _data.get("tasks", {}).get(sub.get("task_id"))
    # determine amount robustly (supports both old and new structure)
    amount = 0.0
    if task:
        if "reward" in task:
            try:
                amount = float(task.get("reward", 0.0))
            except Exception:
                amount = 0.0
        else:
            meta = task.get("meta", {}) or task.get("fields", {}) or {}
            for key in ("reward", "Reward", "amount", "Amount"):
                if key in meta:
                    try:
                        amount = float(meta[key])
                        break
                    except Exception:
                        pass
    # create hold and attach to user
    hold_id = create_hold_for_proof(sub_id, uid, sub.get("task_id"), amount, file_id)
    # remove pending proof mapping so user isn't 'stuck' (we still have the submission)
    _data["pending_proofs_by_user"].pop(uid, None)
    save_data()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve (stage 1)", callback_data="approve_proof:" + hold_id),
         InlineKeyboardButton("❌ Reject", callback_data="reject_proof:" + hold_id)]
    ])
    # admin-friendly caption: include normalized full task display
    task_display = _format_task_display(task) if task else "(task deleted)"
    caption = f"Proof {hold_id[:8]}\nUser: {uid}\nAmount: {amount:.2f}\n\nTask:\n{task_display}"
    try:
        # send photo to admin with caption and inline approval buttons
        await context.bot.send_photo(int(ADMIN_ID), photo=file_id, caption=caption[:1000], reply_markup=kb)
    except Exception:
        try:
            await context.bot.send_message(int(ADMIN_ID), caption, reply_markup=kb)
        except Exception:
            pass
    await update.message.reply_text("✅ Proof received. Waiting for admin approval (stage 1). Thank you.")

# Admin approve/reject callbacks (initial)
async def callback_admin_approve_initial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data_cb = query.data

    if data_cb.startswith("approve_proof:"):
        hid = data_cb.split(":", 1)[1]
        h = _data.get("holds", {}).get(hid)
        if not h or h.get("status") != "pending":
            try:
                await query.edit_message_caption("Hold not found or already processed.")
            except Exception:
                pass
            return
        ok = approve_initial_stage(hid)
        if ok:
            uid = h["user_id"]
            # notify user that it's approved and on hold
            release_dt = datetime.fromtimestamp(h["release_at"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            try:
                await context.bot.send_message(int(uid), f"✅ Your submission passed initial review. {h['amount']:.2f} ETB is on hold and will be available for final review at {release_dt}.")
            except Exception:
                pass
            try:
                await query.edit_message_caption(f"✅ Approved stage 1 and scheduled release at {release_dt}.")
            except Exception:
                await query.edit_message_text(f"Approved stage 1 and scheduled release at {release_dt}.")
        else:
            try:
                await query.edit_message_caption("Failed to approve (maybe already processed).")
            except Exception:
                pass
        return

    if data_cb.startswith("reject_proof:"):
        hid = data_cb.split(":", 1)[1]
        h = _data.get("holds", {}).get(hid)
        if not h or h.get("status") != "pending":
            try:
                await query.edit_message_caption("Hold not found or already processed.")
            except Exception:
                pass
            return
        ok = reject_hold(hid)
        if ok:
            uid = h["user_id"]
            try:
                await context.bot.send_message(int(uid), "❌ Your submission was rejected by admin and will not be paid.")
            except Exception:
                pass
            try:
                await query.edit_message_caption("❌ Rejected by admin.")
            except Exception:
                await query.edit_message_text("Rejected.")
            audit("hold_rejected_notify", ADMIN_ID, {"hold": hid})
        else:
            try:
                await query.edit_message_caption("Failed to reject.")
            except Exception:
                pass
        return

# Admin final approve/reject callbacks (after release_at reached)
async def callback_admin_final_decision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data_cb = query.data

    if data_cb.startswith("final_approve:"):
        hid = data_cb.split(":", 1)[1]
        h = _data.get("holds", {}).get(hid)
        if not h or h.get("status") not in ("waiting_final", "approved_stage1"):
            try:
                await query.edit_message_text("Hold not found or not ready for final approval.")
            except Exception:
                pass
            return
        ok = final_approve_and_credit(hid)
        if ok:
            uid = h["user_id"]
            amt = h["amount"]
            try:
                await context.bot.send_message(int(uid), f"🎉 Final approval done. {amt:.2f} ETB was added to your balance.")
            except Exception:
                pass
            try:
                await query.edit_message_text("✅ Final approved and paid to user balance.")
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text("Failed to finalize payment.")
            except Exception:
                pass
        return

    if data_cb.startswith("final_reject:"):
        hid = data_cb.split(":", 1)[1]
        h = _data.get("holds", {}).get(hid)
        if not h or h.get("status") not in ("waiting_final", "approved_stage1"):
            try:
                await query.edit_message_text("Hold not found or not ready for final rejection.")
            except Exception:
                pass
            return
        ok = reject_hold(hid)
        if ok:
            uid = h["user_id"]
            try:
                await context.bot.send_message(int(uid), "❌ Your submission was rejected during final verification and will not be paid.")
            except Exception:
                pass
            try:
                await query.edit_message_text("❌ Final rejected. Hold marked rejected.")
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text("Failed to reject hold.")
            except Exception:
                pass
        return

# Admin view holds (status & release info)
async def admin_view_holds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return
    pending = [h for h in _data.get("holds", {}).values() if h.get("status") in ("pending", "approved_stage1", "waiting_final")]
    if not pending:
        await update.message.reply_text("No pending/approved holds.")
        return
    for h in pending[:50]:
        user = _data.get("users", {}).get(h["user_id"], {})
        task = _data.get("tasks", {}).get(h["task_id"], {})
        status = h.get("status")
        rel = h.get("release_at")
        rel_s = datetime.fromtimestamp(rel, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if rel else "-"
        # include the full normalized task display so admin sees everything
        task_display = _format_task_display(task) if task else "(task deleted)"
        caption = (f"Hold {h['hold_id'][:8]}\nUser: {user.get('first_name','')} ({h['user_id']})\n"
                   f"Amount: {h['amount']:.2f}\nStatus: {status}\nRelease at: {rel_s}\n\nTask:\n{task_display}")
        # Choose buttons depending on state
        if status == "pending":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Approve (stage1)", callback_data="approve_proof:" + h['hold_id']),
                 InlineKeyboardButton("❌ Reject", callback_data="reject_proof:" + h['hold_id'])]
            ])
        elif status in ("approved_stage1", "waiting_final"):
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Final Approve", callback_data="final_approve:" + h['hold_id']),
                 InlineKeyboardButton("❌ Final Reject", callback_data="final_reject:" + h['hold_id'])]
            ])
        else:
            kb = None
        if h.get("photo_file_id"):
            try:
                if kb:
                    await context.bot.send_photo(int(ADMIN_ID), photo=h["photo_file_id"], caption=caption[:1000], reply_markup=kb)
                else:
                    await context.bot.send_photo(int(ADMIN_ID), photo=h["photo_file_id"], caption=caption[:1000])
                continue
            except Exception:
                pass
        if kb:
            await context.bot.send_message(int(ADMIN_ID), caption, reply_markup=kb)
        else:
            await context.bot.send_message(int(ADMIN_ID), caption)

# ---------------- Withdraw flow ----------------
def looks_like_phone_or_method(txt: str) -> bool:
    s = txt.strip()
    digits = any(c.isdigit() for c in s)
    cleaned_len = len(re.sub(r"\s+", "", s))
    return digits and cleaned_len >= 6

def create_withdrawal_request(user_id: str, amount: float, method_text: str) -> str:
    bal = _data.get("users", {}).get(user_id, {}).get("balance", 0.0)
    if amount <= 0 or amount > bal:
        raise ValueError("Invalid amount or insufficient balance")
    reqid = str(uuid.uuid4())
    _data.setdefault("withdrawals", {})[reqid] = {
        "request_id": reqid,
        "user_id": user_id,
        "amount": float(amount),
        "method": method_text,
        "status": "pending",
        "created_at": int(time.time()),
        "processed_at": None,
        "admin_note": None,
        "tx_reference": None,
    }
    _data.setdefault("transactions", []).append({
        "tx_id": reqid,
        "user_id": user_id,
        "type": "withdraw_request",
        "amount": float(amount),
        "method": method_text,
        "status": "pending",
        "ts": int(time.time()),
        "origin": None,
    })
    audit("withdrawal_requested", user_id, {"request_id": reqid, "amount": amount})
    save_data()
    return reqid

async def start_withdraw_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = uid_key(update.effective_user.id)
    _data.setdefault("withdraw_states", {})
    _data["withdraw_states"][uid] = {"step": "await_method", "method": None}
    save_data()
    await update.message.reply_text("Please send your Telebirr phone number.")

async def generic_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = uid_key(update.effective_user.id)
    wstates = _data.setdefault("withdraw_states", {})
    state = wstates.get(uid)

    # help quick-cancel if user uses menu keyword
    if state and text.lower() in MENU_KEYWORDS and text.lower() != "cancel":
        wstates.pop(uid, None)
        save_data()

    if state:
        step = state.get("step")
        if step == "await_method":
            if text.lower() == "cancel":
                wstates.pop(uid, None)
                save_data()
                await update.message.reply_text("Withdraw flow cancelled.")
                return
            if not looks_like_phone_or_method(text):
                await update.message.reply_text("Please send a valid phone number (digits), or 'cancel' to abort.")
                return
            state["method"] = text
            state["step"] = "await_amount"
            save_data()
            await update.message.reply_text(f"Amount (minimum {MIN_WITHDRAW:.2f} ETB).")
            return

        if step == "await_amount":
            if text.lower() == "cancel":
                wstates.pop(uid, None)
                save_data()
                await update.message.reply_text("Withdraw flow cancelled.")
                return
            num_text = text.replace(",", ".").strip()
            m = re.search(r"(-?\d+(\.\d+)?)", num_text)
            if not m:
                await update.message.reply_text("Invalid amount. Send a number like 200 or 250.50, or 'cancel' to abort.")
                return
            try:
                amount = float(m.group(1))
            except Exception:
                await update.message.reply_text("Invalid amount format. Send a number like 200 or 250.50.")
                return
            if amount < MIN_WITHDRAW:
                await update.message.reply_text(f"Minimum withdraw is {MIN_WITHDRAW:.2f} ETB. Enter a higher amount or send 'cancel' to abort.")
                return
            user_bal = _data.get("users", {}).get(uid, {}).get("balance", 0.0)
            if amount > user_bal:
                await update.message.reply_text(f"Insufficient balance ({user_bal:.2f}). Enter a smaller amount or send 'cancel' to abort.")
                return
            method_text = state.get("method", "")
            try:
                reqid = create_withdrawal_request(uid, amount, method_text)
            except ValueError:
                await update.message.reply_text("Invalid withdraw or insufficient balance.")
                wstates.pop(uid, None)
                save_data()
                return
            wstates.pop(uid, None)
            save_data()
            await update.message.reply_text("✅ Withdrawal request submitted. Admin will review and process.")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Mark Paid", callback_data="withdraw_paid:" + reqid),
                 InlineKeyboardButton("Reject", callback_data="withdraw_reject:" + reqid)]
            ])
            try:
                await context.bot.send_message(int(ADMIN_ID), "Withdrawal " + reqid[:8] + " from user " + uid + "\nAmount: " + str(amount) + "\nMethod: " + method_text, reply_markup=kb)
            except Exception:
                pass
            return

    # Normal menu handling
    if text.lower() in ("tasks", "task"):
        await show_tasks(update, context)
        return
    if text.lower() in ("balance",):
        await balance_cmd(update, context)
        return
    if text.lower() in ("history", "submit task", "submit"):
        await show_history(update, context)
        return
    if text.lower() in ("withdraw",):
        await start_withdraw_flow(update, context)
        return
    if text.lower() in ("referral",):
        user = _data.get("users", {}).get(uid, {})
        ref = user.get("referrer") or "None"
        bot_username = getattr(context.bot, "username", "yourbot")
        # create a share link that opens the Telegram share dialog (not /start directly)
        share_url = (
            f"https://t.me/share/url?"
            f"url=https://t.me/{bot_username}?start={uid}"
            f"&text=Join%20and%20earn%20by%20doing%20simple%20tasks!"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Share/referral link ↗️", url=share_url)]
        ])
        await update.message.reply_text(
            f"👥 Referral Program\n\nYour referrer: {ref}\nInvite friends and earn 10% of their task rewards.\n\nYour link:\nhttps://t.me/{bot_username}?start={uid}",
            reply_markup=kb
        )
        return
    if text.lower() in ("customer support", "tutorial"):
        # want to go directly to support chat: provide link button
        try:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open Customer Support", url=f"https://t.me/{SUPPORT_USERNAME}")]])
            await update.message.reply_text("Customer Support: contact via the button below.", reply_markup=kb)
        except Exception:
            await update.message.reply_text("Customer Support: contact @" + SUPPORT_USERNAME)
        return
    if text.lower() in ("help",):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("What is hold", callback_data="help_hold")],
            [InlineKeyboardButton("How does the referral system work?", callback_data="help_referral")],
            [InlineKeyboardButton('Why is the account "Unavailable"?', callback_data="help_unavailable")]
        ])
        await update.message.reply_text(
            "Help — common questions. Tap a button to view details.",
            reply_markup=kb
        )
        return

# ---------------- small helpers: balance/history/tasks listing ----------------
async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = ensure_user(update.effective_user)
    user = _data.get("users", {}).get(uid, {})
    pending = 0.0
    for hid in user.get("holds", []):
        h = _data.get("holds", {}).get(hid)
        if h and h.get("status") in ("pending", "approved_stage1", "waiting_final"):
            pending += h.get("amount", 0.0)
    await update.message.reply_text(f"Balance: {user.get('balance',0.0):.2f} ETB\nOn hold: {pending:.2f} ETB")

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = uid_key(update.effective_user.id)
    txs = [t for t in _data.get("transactions", []) if t.get("user_id") == uid]
    if not txs:
        await update.message.reply_text("No transaction history yet.")
        return
    lines = []
    for t in sorted(txs, key=lambda x: x.get("ts", 0), reverse=True)[:10]:
        ttime = datetime.fromtimestamp(t.get("ts", 0), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines.append(f"- {ttime}: {t.get('type')} {t.get('amount'):.2f} ETB (status: {t.get('status')})")
    await update.message.reply_text("Your recent transactions:\n" + "\n".join(lines))

# ---------------- task count and listing ----------------
async def taskcount_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = uid_key(update.effective_user.id)
    total_published = len([t for t in _data.get("tasks", {}).values() if t.get("published", True)])
    available = 0
    now = int(time.time())
    locks = _data.setdefault("task_locks", {})
    for t in _data.get("tasks", {}).values():
        if not t.get("published", True):
            continue
        tid = t.get("task_id")
        # skip if the user already submitted this task (avoid duplicate same-task submissions)
        skip = False
        for s in _data.get("submissions", {}).values():
            if s.get("user_id") == uid and s.get("task_id") == tid:
                skip = True
                break
        if skip:
            continue
        # skip if locked by other user and not expired
        lock = locks.get(tid)
        if lock and (now - lock.get("locked_at", 0) < LOCK_TIMEOUT_SECONDS) and lock.get("user_id") != uid:
            continue
        available += 1
    await update.message.reply_text(f"Total published tasks: {total_published}\nAvailable for you: {available}")

async def listtasks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # If admin, show all with ids and delete button per task; otherwise list available tasks briefly (first line)
    if str(update.effective_user.id) == ADMIN_ID:
        tasks = _data.get("tasks", {})
        if not tasks:
            await update.message.reply_text("No tasks exist.")
            return
        for t in tasks.values():
            # Admin preview: show stored raw_text if exists, otherwise title/desc
            display = _format_task_display(t)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Delete task", callback_data="admin_delete_task:" + t["task_id"]),
                 InlineKeyboardButton("Taskcount", callback_data="admin_task_count")],
            ])
            try:
                # send a reasonable-length preview (truncate to 300 chars)
                preview = display if len(display) <= 300 else display[:300] + "..."
                await update.message.reply_text(preview, reply_markup=kb)
            except Exception:
                pass
    else:
        uid = uid_key(update.effective_user.id)
        now = int(time.time())
        locks = _data.setdefault("task_locks", {})
        tasks = []
        for t in _data.get("tasks", {}).values():
            if not t.get("published", True):
                continue
            tid = t.get("task_id")
            # avoid showing a task the user already submitted
            skip = False
            for s in _data.get("submissions", {}).values():
                if s.get("user_id") == uid and s.get("task_id") == tid:
                    skip = True
                    break
            if skip:
                continue
            lock = locks.get(tid)
            if lock and (now - lock.get("locked_at", 0) < LOCK_TIMEOUT_SECONDS) and lock.get("user_id") != uid:
                continue
            tasks.append(t)
        if not tasks:
            await update.message.reply_text("No available tasks right now.")
            return
        # show first non-empty line from each task's display
        lines = []
        for i, t in enumerate(tasks[:20]):
            d = _format_task_display(t)
            first_line = d.splitlines()[0] if d else "Task"
            # include reward (normalized)
            reward = 0.0
            try:
                reward = float(t.get('reward', 0.0))
            except Exception:
                meta = t.get("meta") or {}
                for k in ("reward", "Reward", "amount", "Amount"):
                    if k in meta:
                        try:
                            reward = float(re.sub(r"[^\d\.]", "", str(meta[k])))
                            break
                        except Exception:
                            pass
            lines.append(f"{i+1}. {first_line} — Reward: {reward:.2f} ETB")
        await update.message.reply_text("Available tasks:\n" + "\n".join(lines))

# ---------------- Admin: delete task via command ----------------
async def deletetask_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return
    if not context.args:
        await update.message.reply_text("Usage: /deletetask <task_id>")
        return
    task_id = context.args[0].strip()
    if delete_task(task_id):
        await update.message.reply_text(f"Task {task_id[:8]} deleted.")
    else:
        await update.message.reply_text("Task not found.")

# ---------------- Admin callback shortcuts (dashboard + delete handling) ----------------
async def admin_callback_shortcuts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "view_holds_admin":
        await admin_view_holds(update, context)
        try:
            await query.edit_message_text("Sent pending holds to admin chat.")
        except Exception:
            pass
        return

    if data == "admin_add_task":
        try:
            await query.edit_message_text("To add a task use command:\n/addtask (raw text; must include a 'Reward:' line)")
        except Exception:
            pass
        return

    if data == "admin_list_tasks":
        tasks = _data.get("tasks", {})
        if not tasks:
            try:
                await query.edit_message_text("No tasks exist.")
            except Exception:
                pass
            return
        for t in tasks.values():
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Delete task", callback_data="admin_delete_task:" + t["task_id"]),
                 InlineKeyboardButton("Taskcount", callback_data="admin_task_count")],
            ])
            try:
                preview = _format_task_display(t)
                await context.bot.send_message(int(ADMIN_ID), preview, reply_markup=kb)
            except Exception:
                pass
        try:
            await query.edit_message_text("Sent task list to admin chat.")
        except Exception:
            pass
        return

    if data == "admin_task_count":
        total_published = len([t for t in _data.get("tasks", {}).values() if t.get("published", True)])
        try:
            await query.edit_message_text(f"Total published tasks: {total_published}")
        except Exception:
            pass
        return

    if data.startswith("admin_delete_task:"):
        task_id = data.split(":", 1)[1]
        ok = delete_task(task_id)
        if ok:
            try:
                await query.edit_message_text(f"Task {task_id[:8]} deleted by admin.")
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text("Task not found or already deleted.")
            except Exception:
                pass
        return

# ---------------- Admin withdraw callbacks ----------------
def mark_withdrawal_paid(request_id: str, tx_ref: str = None) -> bool:
    req = _data.get("withdrawals", {}).get(request_id)
    if not req or req.get("status") != "pending":
        return False
    uid = req["user_id"]
    amt = req["amount"]
    if _data.get("users", {}).get(uid, {}).get("balance", 0.0) < amt:
        return False
    _data["users"][uid]["balance"] -= amt
    req["status"] = "paid"
    req["processed_at"] = int(time.time())
    req["tx_reference"] = tx_ref or "manual"
    for t in _data.get("transactions", []):
        if t.get("tx_id") == request_id and t.get("type") == "withdraw_request":
            t["status"] = "paid"
            t["ts_processed"] = int(time.time())
            break
    save_data()
    return True

async def callback_withdraw_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data_cb = query.data
    if data_cb.startswith("withdraw_paid:"):
        reqid = data_cb.split(":", 1)[1]
        ok = mark_withdrawal_paid(reqid, tx_ref="manual")
        if ok:
            await query.edit_message_text("Marked as PAID and user balance deducted.")
            req = _data.get("withdrawals", {}).get(reqid)
            try:
                await context.bot.send_message(int(req["user_id"]), "💸 Your withdrawal " + reqid[:8] + " was marked PAID by admin.")
            except Exception:
                pass
        else:
            await query.edit_message_text("Failed to mark paid (maybe insufficient balance).")
        return
    if data_cb.startswith("withdraw_reject:"):
        reqid = data_cb.split(":", 1)[1]
        req = _data.get("withdrawals", {}).get(reqid)
        if not req or req.get("status") != "pending":
            await query.edit_message_text("Request not found or already processed.")
            return
        req["status"] = "rejected"
        req["processed_at"] = int(time.time())
        req["admin_note"] = f"rejected by {ADMIN_ID}"
        for t in _data.get("transactions", []):
            if t.get("tx_id") == reqid and t.get("type") == "withdraw_request":
                t["status"] = "rejected"
                t["ts_processed"] = int(time.time())
                break
        save_data()
        await query.edit_message_text("Withdrawal rejected.")
        try:
            await context.bot.send_message(int(req["user_id"]), "❌ Your withdrawal " + reqid[:8] + " was rejected by admin.")
        except Exception:
            pass
        return

# ---------------- Job release: send final-review prompts when release_at arrives ----------------
async def job_release(context: ContextTypes.DEFAULT_TYPE):
    now = int(time.time())
    changed = False
    # when a hold is approved_stage1 and release_at <= now: mark waiting_final and notify admin for final decision
    for hid, hold in list(_data.get("holds", {}).items()):
        if hold.get("status") == "approved_stage1" and hold.get("release_at") and hold.get("release_at") <= now:
            hold["status"] = "waiting_final"
            hold["waiting_since"] = now
            changed = True
            # notify admin with final approve/reject
            user = _data.get("users", {}).get(hold["user_id"], {})
            task = _data.get("tasks", {}).get(hold["task_id"], {})
            task_display = _format_task_display(task) if task else "(task deleted)"
            caption = (f"⏳ Final review ready for Hold {hid[:8]}\nUser: {user.get('first_name','')} ({hold['user_id']})\n"
                       f"Amount: {hold['amount']:.2f}\nSubmitted at: {datetime.fromtimestamp(hold.get('created_at',0), tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\nTask:\n{task_display}")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Final Approve", callback_data="final_approve:" + hid),
                 InlineKeyboardButton("❌ Final Reject", callback_data="final_reject:" + hid)]
            ])
            try:
                if hold.get("photo_file_id"):
                    await context.bot.send_photo(int(ADMIN_ID), photo=hold["photo_file_id"], caption=caption[:1000], reply_markup=kb)
                else:
                    await context.bot.send_message(int(ADMIN_ID), caption, reply_markup=kb)
            except Exception:
                logger.exception("Failed to notify admin for final hold.")
    # release expired task locks (returns tasks to pool)
    expired = release_expired_locks()
    if expired:
        changed = True
    if changed:
        save_data()

# ---------------- Admin dashboard ----------------
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return
    total_users = len(_data.get("users", {}))
    total_balance = sum(u.get("balance", 0.0) for u in _data.get("users", {}).values())
    total_holds = sum(h.get("amount", 0.0) for h in _data.get("holds", {}).values() if h.get("status") in ("pending", "approved_stage1", "waiting_final"))
    pending_subs = len([s for s in _data.get("submissions", {}).values() if s.get("status") in ("awaiting_proof", "pending")])
    pending_withdraws = len([w for w in _data.get("withdrawals", {}).values() if w.get("status") == "pending"])
    text = (f"Admin Dashboard\nUsers: {total_users}\nTotal balance: {total_balance:.2f}\n"
            f"Pending holds: {total_holds:.2f}\nPending submissions awaiting proof/pending: {pending_subs}\nPending withdrawals: {pending_withdraws}")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("View holds", callback_data="view_holds_admin")],
        [InlineKeyboardButton("Add task", callback_data="admin_add_task")],
        [InlineKeyboardButton("List tasks", callback_data="admin_list_tasks"),
         InlineKeyboardButton("Taskcount", callback_data="admin_task_count")],
    ])
    await update.message.reply_text(text, reply_markup=kb)

# ---------------- STRICT /addtask (raw text, Reward required) ----------------
async def addtask_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Strict single-command addtask:
    - Accepts raw text after /addtask
    - Requires a 'Reward:' line with a number
    - Stores the task with title = first non-empty line (or entire text) and reward parsed
    - Stores remaining raw text in the task's meta (under 'raw_text')
    """
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return

    # The raw message (including /addtask). We want everything after the command.
    full = update.message.text or ""
    # remove leading command portion (handles '/addtask ' or '/addtask@BotName ')
    task_text = re.sub(r"^/addtask(@\S+)?\s*", "", full, count=1).strip()

    if not task_text:
        await update.message.reply_text(
            "Usage (strict):\n"
            "/addtask <task text>\n\n"
            "Task text must include a line like:\nReward: 180\n\nExample:\n/addtask Follow our channel\nReward: 180\nhttps://t.me/example"
        )
        return

    # Find Reward: number (allow currency suffix or decimals)
    reward_match = re.search(r"Reward\s*:\s*([0-9]+(?:\.[0-9]+)?)", task_text, flags=re.IGNORECASE)
    if not reward_match:
        await update.message.reply_text("❌ Reward is required. Include a line like: `Reward: 180`")
        return

    try:
        reward_value = float(reward_match.group(1))
    except Exception:
        await update.message.reply_text("❌ Couldn't parse the reward number. Use digits like `Reward: 180` or `Reward: 180.50`.")
        return

    if reward_value <= 0:
        await update.message.reply_text("❌ Reward must be greater than 0.")
        return

    # Determine a title: first non-empty line (excluding the Reward line)
    lines = [ln.strip() for ln in task_text.splitlines() if ln.strip()]
    title = None
    rest_lines = []
    for ln in lines:
        # skip the Reward line for title selection
        if re.match(r"Reward\s*:\s*[0-9]+(?:\.[0-9]+)?", ln, flags=re.IGNORECASE):
            continue
        if title is None:
            title = ln
        else:
            rest_lines.append(ln)
    if not title:
        # fallback if all lines are reward or empty
        title = f"Task {str(uuid.uuid4())[:8]}"

    description = "\n".join(rest_lines) if rest_lines else ""
    # create the task using existing helper (keeps same schema)
    tid = create_task(title, reward_value, description, update.effective_user.username or ADMIN_ID, howto=None)
    # attach raw_text meta and full text for clarity (we use meta.raw_text as the canonical user-visible content)
    _data.setdefault("tasks", {}).setdefault(tid, {})["meta"] = {"raw_text": task_text}
    save_data()

    # Send back polished preview exactly as users will see it (normalized reward line)
    preview = _format_task_display(_data["tasks"][tid])
    await update.message.reply_text(preview + "\n\n(Saved)")

# ---------------- Legacy addtask (kept as /addtask_legacy) ----------------
async def addtask_legacy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Format: /addtask_legacy Title | Reward | Description | optional_howto_filename_or_fileid
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return
    payload = " ".join(context.args) if context.args else ""
    if not payload:
        await update.message.reply_text("Usage: /addtask_legacy Title | Reward | Description | optional_howto")
        return
    try:
        parts = [p.strip() for p in payload.split("|")]
        title = parts[0]
        reward = float(parts[1])
        desc = parts[2] if len(parts) > 2 else ""
        howto = parts[3] if len(parts) > 3 else None
    except Exception:
        await update.message.reply_text("Bad format. Use: /addtask_legacy Title | Reward | Description | optional_howto")
        return
    tid = create_task(title, reward, desc, update.effective_user.username or ADMIN_ID, howto)

    # send a polished example preview back to admin so they can check formatting
    preview_lines = [
        f"Title: {title}",
        f"Reward: {reward:.2f} ETB",
        f"Description: {desc or '—'}",
    ]
    if howto:
        preview_lines.append(f"How-to: {howto}")
    preview = "\n".join(preview_lines)
    await update.message.reply_text(f"Task created: {title} ({tid[:8]})\n\nPreview:\n{preview}")

# ---------------- OPTIONAL: Dynamic Add Task (Conversation) kept under /addtask_conv ----------------
async def addtask_start_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # entry point for /addtask_conv conversation
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return ConversationHandler.END

    context.user_data["new_task_fields"] = {}
    context.user_data["current_field"] = None

    await update.message.reply_text(
        "Add task — send a field name, then its value.\n"
        "Examples of field names: Title, Reward, Description, How-to, Country, Platform.\n"
        "When finished type: done\nTo cancel type: cancel"
    )
    return ADD_FIELD_NAME

async def addtask_field_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("Send a field name (or 'done' / 'cancel').")
        return ADD_FIELD_NAME

    low = text.lower()
    if low == "done":
        # save task from collected fields
        fields = context.user_data.get("new_task_fields", {})
        if not fields:
            await update.message.reply_text("No fields added; aborting.")
            context.user_data.clear()
            return ConversationHandler.END

        # build canonical fields for backward compatibility
        lmap = {k.lower(): v for k, v in fields.items()}

        # title
        title = lmap.get("title") or lmap.get("name") or next(iter(fields.values()), "Untitled")

        # reward parsing
        reward = 0.0
        for k in ("reward", "amount"):
            if k in lmap:
                try:
                    reward = float(re.sub(r"[^\d\.\-]", "", lmap[k]))
                    break
                except Exception:
                    reward = 0.0

        description = lmap.get("description") or lmap.get("desc") or ""

        # howto
        howto = lmap.get("howto") or lmap.get("how-to") or lmap.get("how to") or None

        # create the task using existing helper (keeps same schema)
        tid = create_task(title, reward, description, update.effective_user.username or ADMIN_ID, howto)

        # store the rest of fields as meta for display
        meta = {}
        for k, v in fields.items():
            kl = k.lower()
            if kl in ("title", "name", "reward", "amount", "description", "desc", "howto", "how-to", "how to"):
                continue
            meta[k] = v
        if meta:
            _data.setdefault("tasks", {}).setdefault(tid, {})["meta"] = meta
            save_data()

        # preview to admin
        preview_lines = [
            f"Title: {title}",
            f"Reward: {reward:.2f} ETB",
            f"Description: {description or '—'}",
        ]
        if howto:
            preview_lines.append(f"How-to: {howto}")
        if meta:
            preview_lines.append("Extra:")
            for k, v in meta.items():
                preview_lines.append(f"{k}: {v}")
        preview = "\n".join(preview_lines)

        await update.message.reply_text(f"Task created: {title} ({tid[:8]})\n\nPreview:\n{preview}")
        context.user_data.clear()
        return ConversationHandler.END

    if low == "cancel":
        context.user_data.clear()
        await update.message.reply_text("Task creation cancelled.")
        return ConversationHandler.END

    # set current field waiting for value next
    context.user_data["current_field"] = text
    await update.message.reply_text(f"Send value for '{text}':")
    return ADD_FIELD_VALUE

async def addtask_field_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = (update.message.text or "").strip()
    field = context.user_data.get("current_field")
    if not field:
        await update.message.reply_text("Unexpected state; aborting.")
        context.user_data.clear()
        return ConversationHandler.END

    # store the field-value
    context.user_data.setdefault("new_task_fields", {})[field] = value
    context.user_data["current_field"] = None
    await update.message.reply_text("Field saved. Send next field name (or type 'done' / 'cancel').")
    return ADD_FIELD_NAME

# ---------------- Help callback handler ----------------
async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "help_hold":
        await query.edit_message_text(
            "What is hold?\n\nWhen you submit proof and admin approves stage 1, the reward is placed on *hold*.\nAfter the waiting period it will be available for final approval and release to your balance.\n(Usually 3 days unless configured otherwise.)"
        )
    elif data == "help_referral":
        await query.edit_message_text(
            "How does the referral system work?\n\nShare your referral link. When someone registers using that link and completes tasks that are approved, you earn 10% of their approved rewards (credited after stage1->final)."
        )
    elif data == "help_unavailable":
        await query.edit_message_text(
            'Why is the account "Unavailable"?\n\nAn account may be unavailable if it was removed, deleted, or previously completed. If you see this, contact customer support.'
        )
    else:
        await query.edit_message_text("Help item not recognized.")

# ---------------- Admin: list users (balances & holds) ----------------
async def admin_list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_ID:
        await update.message.reply_text("Forbidden")
        return
    users = _data.get("users", {})
    if not users:
        await update.message.reply_text("No users yet.")
        return
    lines = []
    for uid, rec in list(users.items())[:200]:
        bal = rec.get("balance", 0.0)
        hold_total = 0.0
        for hid in rec.get("holds", []):
            h = _data.get("holds", {}).get(hid)
            if h and h.get("status") in ("pending", "approved_stage1", "waiting_final"):
                hold_total += h.get("amount", 0.0)
        lines.append(f"{rec.get('first_name','')} ({uid}) — Balance: {bal:.2f} ETB — On hold: {hold_total:.2f} ETB — Submitted: {rec.get('statistics',{}).get('submitted',0)}")
    await update.message.reply_text("Users:\n" + "\n".join(lines))

# ---------------- Boot ----------------
def main():
    load_data()
    app = ApplicationBuilder().token(TOKEN).build()

    # user commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("tasks", lambda u,c: show_tasks(u,c)))
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CommandHandler("history", show_history))
    app.add_handler(CommandHandler("withdraw", start_withdraw_flow))
    app.add_handler(CommandHandler("taskcount", taskcount_cmd))
    app.add_handler(CommandHandler("listtasks", listtasks_cmd))
    app.add_handler(CommandHandler("deletetask", deletetask_cmd))
    # Strict addtask (raw text, Reward required)
    app.add_handler(CommandHandler("addtask", addtask_cmd))
    # legacy and optional flows
    app.add_handler(CommandHandler("addtask_legacy", addtask_legacy_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("holds", admin_view_holds))  # admin only
    app.add_handler(CommandHandler("users", admin_list_users))  # admin only - list users & holds

    # AddTask conversation handler kept under /addtask_conv for advanced entry
    addtask_conv = ConversationHandler(
        entry_points=[CommandHandler("addtask_conv", addtask_start_conv)],
        states={
            ADD_FIELD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, addtask_field_name)],
            ADD_FIELD_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, addtask_field_value)],
        },
        fallbacks=[],
    )
    app.add_handler(addtask_conv)

    # callback handlers
    app.add_handler(CallbackQueryHandler(callback_task_actions, pattern="^(complete:|cancel_task|howto:)"))
    app.add_handler(CallbackQueryHandler(callback_admin_approve_initial, pattern="^(approve_proof:|reject_proof:)"))
    app.add_handler(CallbackQueryHandler(callback_admin_final_decision, pattern="^(final_approve:|final_reject:)"))
    app.add_handler(CallbackQueryHandler(callback_withdraw_admin, pattern="^(withdraw_paid:|withdraw_reject:)"))
    app.add_handler(CallbackQueryHandler(admin_callback_shortcuts, pattern="^(view_holds_admin|admin_add_task|admin_list_tasks|admin_task_count|admin_delete_task:.*)$"))
    app.add_handler(CallbackQueryHandler(help_callback, pattern="^help_"))
    # photo (proof) handler
    app.add_handler(MessageHandler(filters.PHOTO & (~filters.COMMAND), handle_photo_messages))
    # generic text handler (withdraw flow and menu)
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), generic_message_handler))

    # schedule job that moves holds to waiting_final when release_at reached and releases expired locks
    app.job_queue.run_repeating(job_release, interval=HOLD_CHECK_INTERVAL, first=10)

    logger.info("Bot started")
    app.run_polling()

if __name__ == "__main__":
    main()
