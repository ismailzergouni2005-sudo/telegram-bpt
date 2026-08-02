import os
import asyncio
import sqlite3
import mimetypes
from datetime import datetime, timedelta
from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand
)
import threading
from flask import Flask
import nest_asyncio

# إنشاء Event Loop رئيسي
try:
    asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

nest_asyncio.apply()

# ==================== خادم الويب ====================
web_app = Flask('')

@web_app.route('/')
def home():
    return "Bot is Running 24/7!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

threading.Thread(target=run_web, daemon=True).start()

# ==================== 1. الإعدادات والبيانات ====================
API_ID = 32087655
API_HASH = "0276a0250c2cfc8a1dde70b0f9f92fcd"
BOT_TOKEN = "8811469771:AAFYUx7hBFRCzD5cX6HsN0lGW71ZFnzDwP8"
OWNER_ID = 2071492262

POST_INTERVAL = 300            
is_paused = True               
last_post_time = None          
user_states = {}            
temp_posts = {}               
edit_posts = {}

app = Client("my_scheduler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

BUTTON_TEXTS = [
    "🟢 النشر شغال (اضغط للإيقاف)",
    "🔴 النشر متوقف (اضغط للتشغيل)",
    "⏱️ تغيير الفارق الزمني",
    "📢 إدارة القنوات",
    "📊 حالة النشر والطابور",
    "🔄 المنشورات المكررة",
    "🗑️ إفراغ الطابور",
    "📈 إحصائيات النشر"
]

# ==================== 2. إدارة قاعدة البيانات ====================

def init_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS channels (channel_username TEXT PRIMARY KEY)")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            media_group_id TEXT,
            channels TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS recurring_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            media_group_id TEXT,
            channels TEXT,
            next_run_timestamp REAL,
            repeat_interval_seconds INTEGER,
            remaining_repeats INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS publish_stats (
            channel_username TEXT PRIMARY KEY,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def increment_stat(channel, success=True):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO publish_stats (channel_username, success_count, fail_count) VALUES (?, 0, 0)", (channel,))
    if success:
        cursor.execute("UPDATE publish_stats SET success_count = success_count + 1 WHERE channel_username = ?", (channel,))
    else:
        cursor.execute("UPDATE publish_stats SET fail_count = fail_count + 1 WHERE channel_username = ?", (channel,))
    conn.commit()
    conn.close()

def get_stats():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT channel_username, success_count, fail_count FROM publish_stats ORDER BY success_count DESC")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_channels():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT channel_username FROM channels")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]

def add_channel_db(ch):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO channels VALUES (?)", (ch,))
    conn.commit()
    conn.close()

def remove_channel_db(ch):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM channels WHERE channel_username = ?", (ch,))
    conn.commit()
    conn.close()

def add_to_queue_db(chat_id, message_id, media_group_id, channels_list):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    chs_str = ",".join(channels_list)
    cursor.execute("INSERT INTO queue (chat_id, message_id, media_group_id, channels) VALUES (?, ?, ?, ?)", 
                   (chat_id, message_id, str(media_group_id), chs_str))
    conn.commit()
    conn.close()

def add_recurring_db(chat_id, message_id, media_group_id, channels_list, next_run_dt, interval_sec, repeats):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    chs_str = ",".join(channels_list)
    ts = next_run_dt.timestamp()
    cursor.execute("""
        INSERT INTO recurring_posts 
        (chat_id, message_id, media_group_id, channels, next_run_timestamp, repeat_interval_seconds, remaining_repeats)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (chat_id, message_id, str(media_group_id), chs_str, ts, interval_sec, repeats))
    conn.commit()
    conn.close()

def get_queue_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, chat_id, message_id, media_group_id, channels FROM queue ORDER BY id ASC")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_recurring_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, chat_id, message_id, media_group_id, channels, next_run_timestamp, repeat_interval_seconds, remaining_repeats FROM recurring_posts ORDER BY next_run_timestamp ASC")
    rows = cursor.fetchall()
    conn.close()
    return rows

def pop_queue_db(queue_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM queue WHERE id = ?", (queue_id,))
    conn.commit()
    conn.close()

def delete_recurring_db(rec_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM recurring_posts WHERE id = ?", (rec_id,))
    conn.commit()
    conn.close()

def update_recurring_next_run(rec_id, next_ts, remaining):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    if remaining == 0:
        cursor.execute("DELETE FROM recurring_posts WHERE id = ?", (rec_id,))
    else:
        cursor.execute("UPDATE recurring_posts SET next_run_timestamp = ?, remaining_repeats = ? WHERE id = ?", (next_ts, remaining, rec_id))
    conn.commit()
    conn.close()

def clear_queue_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM queue")
    conn.commit()
    conn.close()

# ==================== 3. لوحة الأزرار وتصميم الواجهات ====================

def make_progress_bar(success, fail):
    total = success + fail
    if total == 0:
        return "⚪⚪⚪⚪⚪ (0%)"
    ratio = success / total
    filled = int(ratio * 5)
    bar = "🟩" * filled + "🟥" * (5 - filled)
    return f"{bar} ({int(ratio*100)}%)"

def get_main_reply_keyboard():
    status_btn = "🔴 النشر متوقف (اضغط للتشغيل)" if is_paused else "🟢 النشر شغال (اضغط للإيقاف)"
    keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton(status_btn)],
            [KeyboardButton("⏱️ تغيير الفارق الزمني"), KeyboardButton("📢 إدارة القنوات")],
            [KeyboardButton("📊 حالة النشر والطابور"), KeyboardButton("🔄 المنشورات المكررة")],
            [KeyboardButton("📈 إحصائيات النشر"), KeyboardButton("🗑️ إفراغ الطابور")]
        ],
        resize_keyboard=True
    )
    return keyboard

def format_time_label(seconds):
    if seconds == 0:
        return "فوري (الآن)"
    mins = seconds // 60
    if mins < 60:
        return f"{mins} دقيقة"
    hours = mins // 60
    rem_mins = mins % 60
    if rem_mins > 0:
        return f"{hours} ساعة و {rem_mins} دقيقة"
    return f"{hours} ساعة"

def build_recurring_main_kb(user_id):
    data = temp_posts.get(user_id, {})
    start_sec = data.get('rec_start', 0)
    interval_sec = data.get('rec_interval', 3600)
    repeats_val = data.get('rec_repeats', -1)

    repeats_str = "♾️ غير محدود" if repeats_val == -1 else f"{repeats_val} مرة"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⏰ بدء النشر: {format_time_label(start_sec)}", callback_data="rec_menu_start")],
        [InlineKeyboardButton(f"⏱️ الزمن بين التكرارات: {format_time_label(interval_sec)}", callback_data="rec_menu_interval")],
        [InlineKeyboardButton(f"🔁 عدد التكرارات: {repeats_str}", callback_data="rec_menu_repeats")],
        [InlineKeyboardButton("✅ تأكيد وجدولة النشر", callback_data="rec_confirm_save")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="rec_back_to_post"), InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
    ])
    return kb

def build_edit_recurring_kb(r_id, user_id):
    p = edit_posts.get(user_id, {})
    next_ts = p.get('next_ts', datetime.now().timestamp())
    interval_sec = p.get('interval_sec', 3600)
    remaining = p.get('remaining', -1)

    dt_str = datetime.fromtimestamp(next_ts).strftime('%H:%M - %Y/%m/%d')
    rem_str = "غير محدود" if remaining == -1 else f"{remaining} مرة"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⏰ موعد التكرار القادم: {dt_str}", callback_data=f"edit_field_start_{r_id}")],
        [InlineKeyboardButton(f"⏱️ الزمن بين التكرارات: {format_time_label(interval_sec)}", callback_data=f"edit_field_int_{r_id}")],
        [InlineKeyboardButton(f"🔁 عدد المرات: {rem_str}", callback_data=f"edit_field_rep_{r_id}")],
        [InlineKeyboardButton("✅ حفظ التعديلات", callback_data=f"edit_save_{r_id}")],
        [InlineKeyboardButton("🔙 رجوع للقائمة", callback_data="back_to_recs"), InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
    ])
    return kb

# ==================== 4. محرك النشر ====================

async def publish_item(chat_id, msg_id, media_group_id, channels_list):
    for ch in channels_list:
        ch = ch.strip()
        if not ch:
            continue
        try:
            if media_group_id and media_group_id != "None":
                await app.copy_media_group(chat_id=ch, from_chat_id=chat_id, message_id=msg_id)
            else:
                await app.copy_message(chat_id=ch, from_chat_id=chat_id, message_id=msg_id)
            increment_stat(ch, success=True)
        except Exception as e:
            increment_stat(ch, success=False)
            print(f"[!] خطأ نشر في {ch}: {e}")

async def publish_worker():
    global last_post_time, is_paused
    while True:
        try:
            now = datetime.now()
            now_ts = now.timestamp()

            # 1. المنشورات المكررة
            recurring_items = get_recurring_db()
            for r in recurring_items:
                r_id, chat_id, msg_id, media_group_id, chs_str, next_run_ts, interval_sec, remaining = r
                if now_ts >= next_run_ts:
                    channels = chs_str.split(",")
                    await publish_item(chat_id, msg_id, media_group_id, channels)
                    
                    new_remaining = remaining - 1 if remaining > 0 else -1
                    next_ts = now_ts + interval_sec
                    update_recurring_next_run(r_id, next_ts, new_remaining)

            # 2. الطابور الرئيسي
            if not is_paused:
                queue_items = get_queue_db()
                if queue_items:
                    should_publish = False
                    if last_post_time is None:
                        should_publish = True
                    else:
                        if (now - last_post_time).total_seconds() >= POST_INTERVAL:
                            should_publish = True

                    if should_publish:
                        item = queue_items[0]
                        q_id, chat_id, msg_id, media_group_id, chs_str = item
                        await publish_item(chat_id, msg_id, media_group_id, chs_str.split(","))
                        last_post_time = datetime.now()
                        pop_queue_db(q_id)

        except Exception as e:
            print(f"[!] خطأ في المحرك: {e}")
            
        await asyncio.sleep(3)

# ==================== 5. الأوامر والتحكم بالأزرار ====================

admin_filter = filters.private & filters.user(OWNER_ID)

@app.on_message(filters.command("start") & admin_filter)
async def start_cmd(client: Client, message: Message):
    welcome_text = (
        "✨ **أهلاً بك في بوت النشر المجدول والمكرر!**\n"
        "───────────────────\n"
        "⚡ **تحكم كامل بسهولة وأناقة:**\n"
        "• أرسل أي نص، صورة، أو ألبوم لإضافته للطابور.\n"
        "• اختر بين النشر المجدول العادي أو التكرار الشبه تلقائي.\n"
        "───────────────────\n"
        "👇 **استخدم القائمة أدناه للتحكم:**"
    )
    await message.reply_text(welcome_text, reply_markup=get_main_reply_keyboard())

@app.on_message(admin_filter & filters.text & filters.create(lambda _, __, m: m.text in BUTTON_TEXTS))
async def handle_reply_buttons(client: Client, message: Message):
    global is_paused, POST_INTERVAL, user_states, last_post_time
    text = message.text.strip()

    # 1. بدء / إيقاف النشر
    if text.startswith("🟢") or text.startswith("🔴"):
        is_paused = not is_paused
        status_text = (
            "🔴 **تم إيقاف النشر مؤقتاً.**" if is_paused 
            else f"🟢 **تم تشغيل النشر بنجاح!**\n⏱️ **الفارق الزمني الحالي:** `{round(POST_INTERVAL/60, 1)}` دقيقة."
        )
        await message.reply_text(status_text, reply_markup=get_main_reply_keyboard())

    # 2. تغيير الفارق الزمني
    elif text == "⏱️ تغيير الفارق الزمني":
        time_inline_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ 5 دقائق", callback_data="set_time_5"), InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="set_time_30"), InlineKeyboardButton("🕐 ساعة", callback_data="set_time_60")],
            [InlineKeyboardButton("🕒 3 ساعات", callback_data="set_time_180"), InlineKeyboardButton("🕕 12 ساعة", callback_data="set_time_720")],
            [InlineKeyboardButton("✏️ إدخال يدوي (بالدقائق)", callback_data="set_custom_time")],
            [InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")]
        ])
        await message.reply_text(
            f"⏱️ **تحديد الفارق الزمني بين المنشورات:**\n"
            f"───────────────────\n"
            f"💡 **الضبط الحالي:** `{round(POST_INTERVAL/60, 1)}` دقيقة.\n"
            f"اختر الفارق المطلوب من الأزرار أو أدخله يدوياً:",
            reply_markup=time_inline_keyboard
        )

    # 3. إدارة القنوات
    elif text == "📢 إدارة القنوات":
        target_channels = get_channels()
        ch_text = "📢 **القنوات المضافة حالياً:**\n───────────────────\n"
        if target_channels:
            for i, c in enumerate(target_channels, 1):
                ch_text += f"**{i}.** 📌 `{c}`\n"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ إضافة قناة", callback_data="add_channel"), InlineKeyboardButton("❌ حذف قناة", callback_data="remove_channel_menu")],
                [InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")]
            ])
        else:
            ch_text += "⚠️ *لم تقم بإضافة أي قناة بعد!*"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ إضافة قناة الآن", callback_data="add_channel")],
                [InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")]
            ])

        await message.reply_text(ch_text, reply_markup=kb)

    # 4. حالة النشر والطابور
    elif text == "📊 حالة النشر والطابور":
        queue = get_queue_db()
        queue_len = len(queue)
        
        status_icon = "🔴 متوقف" if is_paused else "🟢 يعمل الآن"
        next_post_str = "غير معروف"
        if not is_paused and last_post_time and queue_len > 0:
            remaining_sec = max(0, POST_INTERVAL - (datetime.now() - last_post_time).total_seconds())
            next_post_str = f"بعد {int(remaining_sec // 60)}د و {int(remaining_sec % 60)}ث"
        elif not is_paused and queue_len > 0:
            next_post_str = "فوري"

        msg_text = (
            f"📊 **حالة النشر العامة:**\n"
            f"───────────────────\n"
            f"• **حالة المحرك:** {status_icon}\n"
            f"• **عدد المنتظر بانتظار النشر:** `{queue_len}` منشور\n"
            f"• **موعد المنشور القادم:** `{next_post_str}`\n"
            f"───────────────────\n"
        )
        
        buttons = []
        for idx, item in enumerate(queue, 1):
            q_id = item[0]
            buttons.append([
                InlineKeyboardButton(f"👁️ معاينة #{idx}", callback_data=f"preview_main_q_{q_id}"),
                InlineKeyboardButton(f"🗑️ حذف #{idx}", callback_data=f"delete_main_q_{q_id}")
            ])

        buttons.append([InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")])
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

    # 5. المنشورات المكررة
    elif text == "🔄 المنشورات المكررة":
        recs = get_recurring_db()
        if not recs:
            await message.reply_text("🔄 **لا توجد منشورات مكررة نشطة حالياً.**")
            return
        
        msg_text = "🔄 **قائمة المنشورات المكررة المبرمجة:**\n───────────────────\n"
        buttons = []
        for r in recs:
            r_id, _, _, _, _, next_run_ts, interval_sec, remaining = r
            dt_str = datetime.fromtimestamp(next_run_ts).strftime('%H:%M - %Y/%m/%d')
            rem_str = "بلا نهاية" if remaining == -1 else f"{remaining} مرة"
            
            msg_text += (
                f"📌 **مُعرّف:** `{r_id}`\n"
                f"⏰ **التكرار القادم:** `{dt_str}`\n"
                f"⏱️ **كل:** `{format_time_label(interval_sec)}` | 🔁 **المتبقي:** `{rem_str}`\n"
                f"───────────────────\n"
            )
            buttons.append([
                InlineKeyboardButton(f"✏️ تعديل #{r_id}", callback_data=f"edit_rec_{r_id}"),
                InlineKeyboardButton(f"🗑️ إزالة #{r_id}", callback_data=f"delete_rec_{r_id}")
            ])

        buttons.append([InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")])
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

    # 6. إفراغ الطابور
    elif text == "🗑️ إفراغ الطابور":
        queue_len = len(get_queue_db())
        if queue_len == 0:
            await message.reply_text("⚠️ **الطابور الرئيسي فارغ بالفعل.**")
            return
        confirm_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، تأكيد الإفراغ", callback_data="confirm_clear_queue")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
        ])
        await message.reply_text(f"⚠️ **تأكيد:** هل أنت متأكد من حذف جميع المنشورات (`{queue_len}`) من الطابور؟", reply_markup=confirm_kb)

    # 7. إحصائيات النشر
    elif text == "📈 إحصائيات النشر":
        stats = get_stats()
        if not stats:
            await message.reply_text("📈 **لا توجد إحصائيات نشر مسجلة بعد.**")
            return
        
        msg_text = "📈 **سجل إحصائيات النشر القنوات:**\n───────────────────\n"
        for ch, success, fail in stats:
            p_bar = make_progress_bar(success, fail)
            msg_text += f"📢 `{ch}`\n✅ نجاح: `{success}` | ❌ فشل: `{fail}`\n📊 **النسبة:** {p_bar}\n───────────────────\n"
            
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")]]))

# ==================== 6. الأزرار التفاعلية والردود ====================

@app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery):
    global POST_INTERVAL, user_states, temp_posts, edit_posts
    data = query.data
    user_id = query.from_user.id

    try:
        target_channels = get_channels()

        if data == "action_cancel":
            user_states[user_id] = None
            if user_id in temp_posts:
                del temp_posts[user_id]
            if user_id in edit_posts:
                del edit_posts[user_id]
            await query.message.delete()

        elif data == "add_channel":
            user_states[user_id] = "waiting_add_channel"
            await query.message.edit_text("📢 **أرسل معرّف القناة الآن:**\n*(مثال: `@mychannel`)*")

        elif data == "remove_channel_menu":
            buttons = []
            for ch in target_channels:
                buttons.append([InlineKeyboardButton(f"❌ {ch}", callback_data=f"remove_ch_{ch}")])
            buttons.append([InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")])
            await query.message.edit_text("❌ **اختر القناة المراد إزالتها:**", reply_markup=InlineKeyboardMarkup(buttons))

        elif data.startswith("remove_ch_"):
            ch_to_rem = data.replace("remove_ch_", "")
            remove_channel_db(ch_to_rem)
            await query.answer("تم حذف القناة!", show_alert=True)
            await query.message.edit_text(f"✅ **تم حذف القناة `{ch_to_rem}` بنجاح.**")

        elif data.startswith("set_time_"):
            val = data.replace("set_time_", "")
            if val == "custom":
                user_states[user_id] = "waiting_custom_time"
                await query.message.edit_text("⏱️ **أرسل الفارق الزمني المطلوب بالدقائق (أرقام فقط):**")
            else:
                POST_INTERVAL = int(val) * 60
                await query.message.edit_text(f"✅ **تم ضبط الفارق الزمني على `{val}` دقيقة.**")

        elif data.startswith("delete_rec_"):
            r_id = int(data.replace("delete_rec_", ""))
            delete_recurring_db(r_id)
            await query.answer("تم حذف المنشور المكرر!", show_alert=True)
            await query.message.edit_text("🗑️ **تم إزالة المنشور المكرر بنجاح.**")

        elif data.startswith("edit_rec_"):
            r_id = int(data.replace("edit_rec_", ""))
            conn = sqlite3.connect("bot_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT id, chat_id, message_id, media_group_id, channels, next_run_timestamp, repeat_interval_seconds, remaining_repeats FROM recurring_posts WHERE id = ?", (r_id,))
            row = cursor.fetchone()
            conn.close()
            if row:
                r_id, chat_id, msg_id, media_group_id, chs, next_ts, interval_sec, remaining = row
                edit_posts[user_id] = {
                    'r_id': r_id,
                    'next_ts': next_ts,
                    'interval_sec': interval_sec,
                    'remaining': remaining
                }
                await query.message.edit_text(
                    f"✏️ **تعديل المنشور المكرر #{r_id}**\n"
                    f"───────────────────\n"
                    f"اختر الإعداد المراد تعديله:",
                    reply_markup=build_edit_recurring_kb(r_id, user_id)
                )
            else:
                await query.answer("المنشور غير موجود!", show_alert=True)

        elif data.startswith("edit_field_start_"):
            r_id = int(data.replace("edit_field_start_", ""))
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ فوري (الآن)", callback_data=f"set_edit_start_{r_id}_0"), InlineKeyboardButton("⏱️ 5 دقائق", callback_data=f"set_edit_start_{r_id}_300")],
                [InlineKeyboardButton("⏱️ 30 دقيقة", callback_data=f"set_edit_start_{r_id}_1800"), InlineKeyboardButton("🕐 ساعة", callback_data=f"set_edit_start_{r_id}_3600")],
                [InlineKeyboardButton("🕒 5 ساعات", callback_data=f"set_edit_start_{r_id}_18000"), InlineKeyboardButton("🕕 6 ساعات", callback_data=f"set_edit_start_{r_id}_21600")],
                [InlineKeyboardButton("🕛 12 ساعة", callback_data=f"set_edit_start_{r_id}_43200"), InlineKeyboardButton("🗓️ 24 ساعة", callback_data=f"set_edit_start_{r_id}_86400")],
                [InlineKeyboardButton("✏️ إدخال يدوي (بالدقائق)", callback_data=f"custom_edit_start_{r_id}")],
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_rec_{r_id}")]
            ])
            await query.message.edit_text("⏰ **اختر الموعد القادم الجديد أو أدخله يدوياً:**", reply_markup=kb)

        elif data.startswith("custom_edit_start_"):
            r_id = int(data.replace("custom_edit_start_", ""))
            user_states[user_id] = f"waiting_custom_edit_start_{r_id}"
            await query.message.edit_text("✏️ **أرسل وقت البدء بعد كم دقيقة من الآن (أرقام فقط):**\n*(مثال: أرسل `45` لبدء النشر بعد 45 دقيقة)*")

        elif data.startswith("set_edit_start_"):
            parts = data.replace("set_edit_start_", "").split("_")
            r_id, sec = int(parts[0]), int(parts[1])
            new_ts = datetime.now().timestamp() + sec
            if user_id in edit_posts:
                edit_posts[user_id]['next_ts'] = new_ts
            await query.answer("تم تحديث الموعد!")
            await query.message.edit_text(
                f"✏️ **تعديل المنشور المكرر #{r_id}**\n───────────────────\nاختر الإعداد المراد تعديله:",
                reply_markup=build_edit_recurring_kb(r_id, user_id)
            )

        elif data.startswith("edit_field_int_"):
            r_id = int(data.replace("edit_field_int_", ""))
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏱️ 5 دقائق", callback_data=f"set_edit_int_{r_id}_300"), InlineKeyboardButton("⏱️ 30 دقيقة", callback_data=f"set_edit_int_{r_id}_1800")],
                [InlineKeyboardButton("🕐 ساعة", callback_data=f"set_edit_int_{r_id}_3600"), InlineKeyboardButton("🕒 5 ساعات", callback_data=f"set_edit_int_{r_id}_18000")],
                [InlineKeyboardButton("✏️ إدخال يدوي (بالدقائق)", callback_data=f"custom_edit_int_{r_id}")],
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_rec_{r_id}")]
            ])
            await query.message.edit_text("⏱️ **اختر الفارق الزمني الجديد بين كل تكرار أو أدخله يدوياً:**", reply_markup=kb)

        elif data.startswith("custom_edit_int_"):
            r_id = int(data.replace("custom_edit_int_", ""))
            user_states[user_id] = f"waiting_custom_edit_int_{r_id}"
            await query.message.edit_text("✏️ **أرسل الفارق الزمني الجديد بين التكرارات بالدقائق (أرقام فقط):**\n*(مثال: أرسل `120` للتكرار كل ساعتين)*")

        elif data.startswith("set_edit_int_"):
            parts = data.replace("set_edit_int_", "").split("_")
            r_id, sec = int(parts[0]), int(parts[1])
            if user_id in edit_posts:
                edit_posts[user_id]['interval_sec'] = sec
            await query.answer("تم تحديث الفارق الزمني!")
            await query.message.edit_text(
                f"✏️ **تعديل المنشور المكرر #{r_id}**\n───────────────────\nاختر الإعداد المراد تعديله:",
                reply_markup=build_edit_recurring_kb(r_id, user_id)
            )

        elif data.startswith("edit_field_rep_"):
            r_id = int(data.replace("edit_field_rep_", ""))
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("♾️ غير محدود", callback_data=f"set_edit_rep_{r_id}_-1"), InlineKeyboardButton("1️⃣ مرة واحدة", callback_data=f"set_edit_rep_{r_id}_1")],
                [InlineKeyboardButton("3️⃣ ثلاث مرات", callback_data=f"set_edit_rep_{r_id}_3"), InlineKeyboardButton("5️⃣ 5 مرات", callback_data=f"set_edit_rep_{r_id}_5")],
                [InlineKeyboardButton("🔟 10 مرات", callback_data=f"set_edit_rep_{r_id}_10")],
                [InlineKeyboardButton("✏️ إدخال يدوي", callback_data=f"custom_edit_rep_{r_id}")],
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_rec_{r_id}")]
            ])
            await query.message.edit_text("🔁 **اختر عدد مرات التكرار الجديدة:**", reply_markup=kb)

        elif data.startswith("custom_edit_rep_"):
            r_id = int(data.replace("custom_edit_rep_", ""))
            user_states[user_id] = f"waiting_custom_edit_rep_{r_id}"
            await query.message.edit_text("✏️ **أرسل عدد مرات التكرار الجديدة (أرقام فقط):**")

        elif data.startswith("set_edit_rep_"):
            parts = data.replace("set_edit_rep_", "").split("_")
            r_id, rep = int(parts[0]), int(parts[1])
            if user_id in edit_posts:
                edit_posts[user_id]['remaining'] = rep
            await query.answer("تم تحديث عدد التكرارات!")
            await query.message.edit_text(
                f"✏️ **تعديل المنشور المكرر #{r_id}**\n───────────────────\nاختر الإعداد المراد تعديله:",
                reply_markup=build_edit_recurring_kb(r_id, user_id)
            )

        elif data.startswith("edit_save_"):
            r_id = int(data.replace("edit_save_", ""))
            if user_id in edit_posts:
                p = edit_posts[user_id]
                conn = sqlite3.connect("bot_data.db")
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE recurring_posts 
                    SET next_run_timestamp = ?, repeat_interval_seconds = ?, remaining_repeats = ? 
                    WHERE id = ?
                """, (p['next_ts'], p['interval_sec'], p['remaining'], r_id))
                conn.commit()
                conn.close()
                del edit_posts[user_id]
                await query.answer("تم حفظ التعديلات بنجاح!", show_alert=True)
                await query.message.edit_text("✅ **تم حفظ إعدادات المنشور المكرر المحدثة بنجاح.**")

        elif data == "back_to_recs":
            if user_id in edit_posts:
                del edit_posts[user_id]
            recs = get_recurring_db()
            msg_text = "🔄 **قائمة المنشورات المكررة المبرمجة:**\n───────────────────\n"
            buttons = []
            for r in recs:
                r_id, _, _, _, _, next_run_ts, interval_sec, remaining = r
                dt_str = datetime.fromtimestamp(next_run_ts).strftime('%H:%M - %Y/%m/%d')
                rem_str = "بلا نهاية" if remaining == -1 else f"{remaining} مرة"
                msg_text += f"📌 **مُعرّف:** `{r_id}`\n⏰ **التكرار القادم:** `{dt_str}`\n⏱️ **كل:** `{format_time_label(interval_sec)}` | 🔁 **المتبقي:** `{rem_str}`\n───────────────────\n"
                buttons.append([InlineKeyboardButton(f"✏️ تعديل #{r_id}", callback_data=f"edit_rec_{r_id}"), InlineKeyboardButton(f"🗑️ إزالة #{r_id}", callback_data=f"delete_rec_{r_id}")])
            buttons.append([InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")])
            await query.message.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

        elif data.startswith("preview_main_q_"):
            q_id = int(data.replace("preview_main_q_", ""))
            conn = sqlite3.connect("bot_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id, message_id, media_group_id FROM queue WHERE id = ?", (q_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                chat_id, msg_id, media_group_id = row
                try:
                    if media_group_id and media_group_id != "None":
                        await app.copy_media_group(chat_id=query.from_user.id, from_chat_id=chat_id, message_id=msg_id)
                    else:
                        await app.copy_message(chat_id=query.from_user.id, from_chat_id=chat_id, message_id=msg_id)
                    await query.answer("تم إرسال المعاينة لك في الخاص!", show_alert=True)
                except Exception as e:
                    await query.answer(f"تعذر إرسال المعاينة: {e}", show_alert=True)
            else:
                await query.answer("لم يتم العثور على المنشور!", show_alert=True)

        elif data.startswith("delete_main_q_"):
            q_id = int(data.replace("delete_main_q_", ""))
            pop_queue_db(q_id)
            await query.answer("تم الحذف من الطابور!", show_alert=True)
            await query.message.edit_text("🗑️ **تم إزالة المنشور المحدد من الطابور.**")

        elif data == "confirm_clear_queue":
            clear_queue_db()
            await query.answer("تم إفراغ الطابور!", show_alert=True)
            await query.message.edit_text("🗑️ **تم إفراغ الطابور الرئيسي بالكامل.**")

        # خيارات إعداد المنشور المكرر الجديد
        elif data == "post_type_recurring":
            temp_posts[user_id]['rec_start'] = 0
            temp_posts[user_id]['rec_interval'] = 3600
            temp_posts[user_id]['rec_repeats'] = -1
            await query.message.edit_text(
                "🔄 **إعداد جدول النشر المكرر:**\n───────────────────\nقم بضبط تفاصيل التكرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_menu_start":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ فوري (الآن)", callback_data="rec_set_start_0"), InlineKeyboardButton("⏱️ 5 دقائق", callback_data="rec_set_start_300")],
                [InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="rec_set_start_1800"), InlineKeyboardButton("🕐 ساعة", callback_data="rec_set_start_3600")],
                [InlineKeyboardButton("🕒 5 ساعات", callback_data="rec_set_start_18000"), InlineKeyboardButton("🕕 6 ساعات", callback_data="rec_set_start_21600")],
                [InlineKeyboardButton("🕛 12 ساعة", callback_data="rec_set_start_43200"), InlineKeyboardButton("🗓️ 24 ساعة", callback_data="rec_set_start_86400")],
                [InlineKeyboardButton("✏️ إدخال يدوي (بالدقائق)", callback_data="rec_custom_start")],
                [InlineKeyboardButton("🔙 رجوع", callback_data="post_type_recurring")]
            ])
            await query.message.edit_text("⏰ **اختر موعد أول نشر:**", reply_markup=kb)

        elif data.startswith("rec_set_start_"):
            sec = int(data.replace("rec_set_start_", ""))
            temp_posts[user_id]['rec_start'] = sec
            await query.message.edit_text(
                "🔄 **إعداد جدول النشر المكرر:**\n───────────────────\nقم بضبط تفاصيل التكرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_custom_start":
            user_states[user_id] = "waiting_rec_custom_start"
            await query.message.edit_text("✏️ **أرسل وقت أول نشر بعد كم دقيقة من الآن (أرقام فقط):**\n*(مثال: `30` لبدء النشر بعد 30 دقيقة)*")

        elif data == "rec_menu_interval":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏱️ 5 دقائق", callback_data="rec_set_interval_300"), InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="rec_set_interval_1800")],
                [InlineKeyboardButton("🕐 ساعة", callback_data="rec_set_interval_3600"), InlineKeyboardButton("🕒 5 ساعات", callback_data="rec_set_interval_18000")],
                [InlineKeyboardButton("✏️ إدخال يدوي (بالدقائق)", callback_data="rec_custom_interval")],
                [InlineKeyboardButton("🔙 رجوع", callback_data="post_type_recurring")]
            ])
            await query.message.edit_text("⏱️ **اختر الوقت الفاصل بين كل تكرار والتالي:**", reply_markup=kb)

        elif data.startswith("rec_set_interval_"):
            sec = int(data.replace("rec_set_interval_", ""))
            temp_posts[user_id]['rec_interval'] = sec
            await query.message.edit_text(
                "🔄 **إعداد جدول النشر المكرر:**\n───────────────────\nقم بضبط تفاصيل التكرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_custom_interval":
            user_states[user_id] = "waiting_rec_custom_interval"
            await query.message.edit_text("✏️ **أرسل الفارق الزمني بين كل تكرار بالدقائق (أرقام فقط):**\n*(مثال: `60` للتكرار كل ساعة)*")

        elif data == "rec_menu_repeats":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("♾️ غير محدود", callback_data="rec_set_repeats_-1"), InlineKeyboardButton("1️⃣ مرة واحدة", callback_data="rec_set_repeats_1")],
                [InlineKeyboardButton("3️⃣ ثلاث مرات", callback_data="rec_set_repeats_3"), InlineKeyboardButton("5️⃣ 5 مرات", callback_data="rec_set_repeats_5")],
                [InlineKeyboardButton("🔟 10 مرات", callback_data="rec_set_repeats_10")],
                [InlineKeyboardButton("✏️ إدخال يدوي", callback_data="rec_custom_repeats")],
                [InlineKeyboardButton("🔙 رجوع", callback_data="post_type_recurring")]
            ])
            await query.message.edit_text("🔁 **اختر عدد مرات التكرار:**", reply_markup=kb)

        elif data.startswith("rec_set_repeats_"):
            rep = int(data.replace("rec_set_repeats_", ""))
            temp_posts[user_id]['rec_repeats'] = rep
            await query.message.edit_text(
                "🔄 **إعداد جدول النشر المكرر:**\n───────────────────\nقم بضبط تفاصيل التكرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_custom_repeats":
            user_states[user_id] = "waiting_rec_custom_repeats"
            await query.message.edit_text("✏️ **أرسل عدد المرات المطلوبة للتكرار (أرقام فقط):**")

        elif data == "rec_confirm_save":
            if user_id in temp_posts:
                post = temp_posts[user_id]
                start_dt = datetime.now() + timedelta(seconds=post['rec_start'])
                add_recurring_db(
                    chat_id=post['chat_id'],
                    message_id=post['message_id'],
                    media_group_id=post['media_group_id'],
                    channels_list=target_channels,
                    next_run_dt=start_dt,
                    interval_sec=post['rec_interval'],
                    repeats=post['rec_repeats']
                )
                del temp_posts[user_id]
                await query.message.edit_text("✅ **تم حفظ وجدولة المنشور المكرر بنجاح!**")

        elif data == "rec_back_to_post":
            if user_id in temp_posts:
                post = temp_posts[user_id]
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📥 إضافة للطابور العادي", callback_data="post_type_queue")],
                    [InlineKeyboardButton("🔄 إعداد منشور مكرر", callback_data="post_type_recurring")],
                    [InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
                ])
                await query.message.edit_text(
                    f"📌 **تم استلام المنشور.**\n───────────────────\n"
                    f"📢 **القنوات المستهدفة:** `{len(target_channels)}` قناة.\n"
                    f"اختر طريقة النشر المطلوبة:",
                    reply_markup=kb
                )

        elif data == "post_type_queue":
            if user_id in temp_posts:
                post = temp_posts[user_id]
                add_to_queue_db(post['chat_id'], post['message_id'], post['media_group_id'], target_channels)
                del temp_posts[user_id]
                await query.message.edit_text("✅ **تمت إضافة المنشور بنجاح إلى الطابور الرئيسي!**")

    except Exception as e:
        print(f"[!] خطأ Callback Query: {e}")

# ==================== 7. استقبال المنشورات ومدخلات النصوص ====================

@app.on_message(admin_filter)
async def handle_incoming_messages(client: Client, message: Message):
    global user_states, temp_posts, POST_INTERVAL
    user_id = message.from_user.id
    state = user_states.get(user_id)

    # 1. حالة إضافة قناة
    if state == "waiting_add_channel":
        ch = message.text.strip()
        if not ch.startswith("@") and not ch.startswith("-100"):
            await message.reply_text("⚠️ **يرجى إدخال معرّف صحيح يبدأ بـ `@` أو أيدي القناة.**")
            return
        add_channel_db(ch)
        user_states[user_id] = None
        await message.reply_text(f"✅ **تمت إضافة القناة `{ch}` بنجاح.**", reply_markup=get_main_reply_keyboard())
        return

    # 2. حالة الوقت المخصص للطابور الرئيسي
    elif state == "waiting_custom_time":
        if not message.text.isdigit():
            await message.reply_text("⚠️ **الرجاء إدخال رقم صحيح يمثل عدد الدقائق.**")
            return
        POST_INTERVAL = int(message.text) * 60
        user_states[user_id] = None
        await message.reply_text(f"✅ **تم ضبط الفارق الزمني للنشر إلى `{message.text}` دقيقة.**", reply_markup=get_main_reply_keyboard())
        return

    # 3. إدخال يدوي لوقت أول تكرار (إنشاء)
    elif state == "waiting_rec_custom_start":
        if not message.text.isdigit():
            await message.reply_text("⚠️ **يرجى إدخال رقم صحيح.**")
            return
        temp_posts[user_id]['rec_start'] = int(message.text) * 60
        user_states[user_id] = None
        await message.reply_text("🔄 **تم تحديث الوقت، اضغط تأكيد للإنهاء:**", reply_markup=build_recurring_main_kb(user_id))
        return

    # 4. إدخال يدوي للفاصل الزمني (إنشاء)
    elif state == "waiting_rec_custom_interval":
        if not message.text.isdigit():
            await message.reply_text("⚠️ **يرجى إدخال رقم صحيح.**")
            return
        temp_posts[user_id]['rec_interval'] = int(message.text) * 60
        user_states[user_id] = None
        await message.reply_text("🔄 **تم تحديث الفارق الزمني، اضغط تأكيد للإنهاء:**", reply_markup=build_recurring_main_kb(user_id))
        return

    # 5. إدخال يدوي لعدد المرات (إنشاء)
    elif state == "waiting_rec_custom_repeats":
        if not message.text.isdigit():
            await message.reply_text("⚠️ **يرجى إدخال رقم صحيح.**")
            return
        temp_posts[user_id]['rec_repeats'] = int(message.text)
        user_states[user_id] = None
        await message.reply_text("🔄 **تم تحديث عدد التكرارات، اضغط تأكيد للإنهاء:**", reply_markup=build_recurring_main_kb(user_id))
        return

    # 6. إدخالات التعديل المخصص
    elif state and state.startswith("waiting_custom_edit_start_"):
        r_id = int(state.replace("waiting_custom_edit_start_", ""))
        if not message.text.isdigit():
            await message.reply_text("⚠️ **يرجى إدخال رقم صحيح.**")
            return
        sec = int(message.text) * 60
        if user_id in edit_posts:
            edit_posts[user_id]['next_ts'] = datetime.now().timestamp() + sec
        user_states[user_id] = None
        await message.reply_text(f"✏️ **تعديل المنشور المكرر #{r_id}**", reply_markup=build_edit_recurring_kb(r_id, user_id))
        return

    elif state and state.startswith("waiting_custom_edit_int_"):
        r_id = int(state.replace("waiting_custom_edit_int_", ""))
        if not message.text.isdigit():
            await message.reply_text("⚠️ **يرجى إدخال رقم صحيح.**")
            return
        sec = int(message.text) * 60
        if user_id in edit_posts:
            edit_posts[user_id]['interval_sec'] = sec
        user_states[user_id] = None
        await message.reply_text(f"✏️ **تعديل المنشور المكرر #{r_id}**", reply_markup=build_edit_recurring_kb(r_id, user_id))
        return

    elif state and state.startswith("waiting_custom_edit_rep_"):
        r_id = int(state.replace("waiting_custom_edit_rep_", ""))
        if not message.text.isdigit():
            await message.reply_text("⚠️ **يرجى إدخال رقم صحيح.**")
            return
        rep = int(message.text)
        if user_id in edit_posts:
            edit_posts[user_id]['remaining'] = rep
        user_states[user_id] = None
        await message.reply_text(f"✏️ **تعديل المنشور المكرر #{r_id}**", reply_markup=build_edit_recurring_kb(r_id, user_id))
        return

    # 7. استقبال المنشورات الجديدة للنشر
    target_channels = get_channels()
    if not target_channels:
        await message.reply_text("⚠️ **يرجى إضافة قناة واحدة على الأقل قبل إضافة المنشورات!**")
        return

    if message.media_group_id:
        if user_id in temp_posts and temp_posts[user_id].get('media_group_id') == message.media_group_id:
            return
        temp_posts[user_id] = {
            'chat_id': message.chat.id,
            'message_id': message.id,
            'media_group_id': message.media_group_id
        }
    else:
        temp_posts[user_id] = {
            'chat_id': message.chat.id,
            'message_id': message.id,
            'media_group_id': None
        }

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 إضافة للطابور العادي", callback_data="post_type_queue")],
        [InlineKeyboardButton("🔄 إعداد منشور مكرر", callback_data="post_type_recurring")],
        [InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
    ])

    await message.reply_text(
        f"📌 **تم استلام المنشور.**\n───────────────────\n"
        f"📢 **القنوات المستهدفة:** `{len(target_channels)}` قناة.\n"
        f"اختر طريقة النشر المطلوبة:",
        reply_markup=kb
    )

# ==================== 8. تشغيل البوت ====================

async def main():
    init_db()
    await app.start()
    
    # ضبط القائمة الرسمية للأوامر في تلجرام
    await app.set_bot_commands([
        BotCommand("start", "بدء تشغيل البوت وفتح القائمة الرئيسية")
    ])

    print("=== [ Bot Started Successfully ] ===")
    
    # إطلاق محرك النشر التلقائي في الخلفية
    asyncio.create_task(publish_worker())
    
    await idle()
    await app.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
