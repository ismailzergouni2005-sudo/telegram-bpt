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

    # 1. بدء / إيقاف النشر (فقط الأزرار الخاصة بالتشغيل/الإيقاف)
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
                f"⏱️ **كل:** `{round(interval_sec/60, 1)}` دقيقة | 🔁 **المتبقي:** `{rem_str}`\n"
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
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_rec_{r_id}")]
            ])
            await query.message.edit_text("⏰ **اختر الموعد القادم الجديد:**", reply_markup=kb)

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
                [InlineKeyboardButton("🕕 6 ساعات", callback_data=f"set_edit_int_{r_id}_21600"), InlineKeyboardButton("🕛 12 ساعة", callback_data=f"set_edit_int_{r_id}_43200")],
                [InlineKeyboardButton("🗓️ 24 ساعة", callback_data=f"set_edit_int_{r_id}_86400")],
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_rec_{r_id}")]
            ])
            await query.message.edit_text("⏱️ **اختر الزمن الجديد بين التكرارات:**", reply_markup=kb)

        elif data.startswith("set_edit_int_"):
            parts = data.replace("set_edit_int_", "").split("_")
            r_id, sec = int(parts[0]), int(parts[1])
            if user_id in edit_posts:
                edit_posts[user_id]['interval_sec'] = sec
            await query.answer("تم تحديث الزمن بين التكرارات!")
            await query.message.edit_text(
                f"✏️ **تعديل المنشور المكرر #{r_id}**\n───────────────────\nاختر الإعداد المراد تعديله:",
                reply_markup=build_edit_recurring_kb(r_id, user_id)
            )

        elif data.startswith("edit_field_rep_"):
            r_id = int(data.replace("edit_field_rep_", ""))
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("♾️ غير محدود", callback_data=f"set_edit_rep_{r_id}_-1")],
                [InlineKeyboardButton("5 مرات", callback_data=f"set_edit_rep_{r_id}_5"), InlineKeyboardButton("10 مرات", callback_data=f"set_edit_rep_{r_id}_10")],
                [InlineKeyboardButton("20 مرة", callback_data=f"set_edit_rep_{r_id}_20"), InlineKeyboardButton("50 مرة", callback_data=f"set_edit_rep_{r_id}_50")],
                [InlineKeyboardButton("🔙 رجوع", callback_data=f"edit_rec_{r_id}")]
            ])
            await query.message.edit_text("🔁 **اختر عدد المرات الجديد:**", reply_markup=kb)

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
            p = edit_posts.get(user_id)
            if p:
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
                await query.message.edit_text(f"✅ **تم حفظ التعديلات على المنشور المكرر #{r_id} بنجاح!**")

        elif data == "back_to_recs":
            recs = get_recurring_db()
            if not recs:
                await query.message.edit_text("🔄 **لا توجد منشورات مكررة نشطة حالياً.**")
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
                    f"⏱️ **كل:** `{round(interval_sec/60, 1)}` دقيقة | 🔁 **المتبقي:** `{rem_str}`\n"
                    f"───────────────────\n"
                )
                buttons.append([
                    InlineKeyboardButton(f"✏️ تعديل #{r_id}", callback_data=f"edit_rec_{r_id}"),
                    InlineKeyboardButton(f"🗑️ إزالة #{r_id}", callback_data=f"delete_rec_{r_id}")
                ])

            buttons.append([InlineKeyboardButton("❌ إغلاق", callback_data="action_cancel")])
            await query.message.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

        elif data.startswith("preview_main_q_"):
            q_id = int(data.replace("preview_main_q_", ""))
            conn = sqlite3.connect("bot_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id, message_id FROM queue WHERE id = ?", (q_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                await app.copy_message(chat_id=user_id, from_chat_id=row[0], message_id=row[1])
                await query.answer("تم عرض المنشور فوق!")

        elif data.startswith("delete_main_q_"):
            q_id = int(data.replace("delete_main_q_", ""))
            pop_queue_db(q_id)
            await query.answer("تم حذف المنشور من الطابور!", show_alert=True)
            await query.message.edit_text("🗑️ **تم حذف المنشور بنجاح.**")

        elif data == "type_normal":
            post_data = temp_posts.get(user_id)
            if post_data:
                add_to_queue_db(post_data['chat_id'], post_data['msg_id'], post_data['media_group_id'], target_channels)
                await query.message.edit_text("📥 **تم إدراج المنشور بنجاح في الطابور الرئيسي!**")
                del temp_posts[user_id]

        elif data == "type_recurring" or data == "rec_menu_main":
            if user_id in temp_posts:
                temp_posts[user_id].setdefault('rec_start', 0)
                temp_posts[user_id].setdefault('rec_interval', 3600)
                temp_posts[user_id].setdefault('rec_repeats', -1)
            
            await query.message.edit_text(
                "⚙️ **لوحة ضبط النشر المكرر التلقائي**\n"
                "───────────────────\n"
                "اختر الإعدادات المناسبة بضغط الأزرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_menu_start":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ فوري (الآن)", callback_data="set_rec_start_0"), InlineKeyboardButton("⏱️ 5 دقائق", callback_data="set_rec_start_300")],
                [InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="set_rec_start_1800"), InlineKeyboardButton("🕐 ساعة", callback_data="set_rec_start_3600")],
                [InlineKeyboardButton("🕒 5 ساعات", callback_data="set_rec_start_18000"), InlineKeyboardButton("🕕 6 ساعات", callback_data="set_rec_start_21600")],
                [InlineKeyboardButton("🕛 12 ساعة", callback_data="set_rec_start_43200"), InlineKeyboardButton("🗓️ 24 ساعة", callback_data="set_rec_start_86400")],
                [InlineKeyboardButton("🔙 رجوع", callback_data="rec_menu_main")]
            ])
            await query.message.edit_text("⏰ **اختر موعد بدء النشر الأول:**", reply_markup=kb)

        elif data.startswith("set_rec_start_"):
            sec = int(data.replace("set_rec_start_", ""))
            if user_id in temp_posts:
                temp_posts[user_id]['rec_start'] = sec
            await query.answer("تم حفظ موعد البدء!")
            await query.message.edit_text(
                "⚙️ **لوحة ضبط النشر المكرر التلقائي**\n───────────────────\nاختر الإعدادات المناسبة بضغط الأزرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_menu_interval":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏱️ 5 دقائق", callback_data="set_rec_int_300"), InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="set_rec_int_1800")],
                [InlineKeyboardButton("🕐 ساعة", callback_data="set_rec_int_3600"), InlineKeyboardButton("🕒 5 ساعات", callback_data="set_rec_int_18000")],
                [InlineKeyboardButton("🕕 6 ساعات", callback_data="set_rec_int_21600"), InlineKeyboardButton("🕛 12 ساعة", callback_data="set_rec_int_43200")],
                [InlineKeyboardButton("🗓️ 24 ساعة", callback_data="set_rec_int_86400")],
                [InlineKeyboardButton("🔙 رجوع", callback_data="rec_menu_main")]
            ])
            await query.message.edit_text("⏱️ **اختر الزمن (الفارق) بين كل تكرار:**", reply_markup=kb)

        elif data.startswith("set_rec_int_"):
            sec = int(data.replace("set_rec_int_", ""))
            if user_id in temp_posts:
                temp_posts[user_id]['rec_interval'] = sec
            await query.answer("تم حفظ الزمن بين التكرارات!")
            await query.message.edit_text(
                "⚙️ **لوحة ضبط النشر المكرر التلقائي**\n───────────────────\nاختر الإعدادات المناسبة بضغط الأزرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_menu_repeats":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("♾️ غير محدود (تكرار دائم)", callback_data="set_rec_rep_-1")],
                [InlineKeyboardButton("5 مرات", callback_data="set_rec_rep_5"), InlineKeyboardButton("10 مرات", callback_data="set_rec_rep_10")],
                [InlineKeyboardButton("20 مرة", callback_data="set_rec_rep_20"), InlineKeyboardButton("50 مرة", callback_data="set_rec_rep_50")],
                [InlineKeyboardButton("🔙 رجوع", callback_data="rec_menu_main")]
            ])
            await query.message.edit_text("🔁 **اختر إجمالي عدد مرات التكرار:**", reply_markup=kb)

        elif data.startswith("set_rec_rep_"):
            rep = int(data.replace("set_rec_rep_", ""))
            if user_id in temp_posts:
                temp_posts[user_id]['rec_repeats'] = rep
            await query.answer("تم حفظ عدد التكرارات!")
            await query.message.edit_text(
                "⚙️ **لوحة ضبط النشر المكرر التلقائي**\n───────────────────\nاختر الإعدادات المناسبة بضغط الأزرار أدناه:",
                reply_markup=build_recurring_main_kb(user_id)
            )

        elif data == "rec_back_to_post":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🟢 نشر مجدول عادي", callback_data="type_normal")],
                [InlineKeyboardButton("🔄 نشر مكرر تلقائي", callback_data="type_recurring")],
                [InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
            ])
            await query.message.edit_text("📥 **تم استلام المنشور!** اختر نوع النشر المطلوب:", reply_markup=kb)

        elif data == "rec_confirm_save":
            post_data = temp_posts.get(user_id)
            if post_data:
                start_sec = post_data.get('rec_start', 0)
                interval_sec = post_data.get('rec_interval', 3600)
                repeats = post_data.get('rec_repeats', -1)

                start_dt = datetime.now() + timedelta(seconds=start_sec)

                add_recurring_db(
                    post_data['chat_id'],
                    post_data['msg_id'],
                    post_data['media_group_id'],
                    get_channels(),
                    start_dt,
                    interval_sec,
                    repeats
                )

                del temp_posts[user_id]
                user_states[user_id] = None

                rep_lbl = "غير محدود" if repeats == -1 else f"{repeats} مرة"
                await query.message.edit_text(
                    f"✅ **تم جدولة المنشور المكرر بنجاح!**\n───────────────────\n"
                    f"⏰ **النشر الأول:** `{start_dt.strftime('%Y-%m-%d %H:%M')}`\n"
                    f"⏱️ **الزمن بين التكرارات:** `{format_time_label(interval_sec)}`\n"
                    f"🔁 **عدد المرات:** `{rep_lbl}`"
                )

        elif data == "confirm_clear_queue":
            clear_queue_db()
            await query.message.edit_text("🗑️ **تم إفراغ الطابور الرئيسي بنجاح.**")

    except Exception as e:
        print(f"[!] خطأ تفاعل: {e}")

# ==================== 7. استقبال النصوص والمنشورات ====================

@app.on_message(admin_filter & ~filters.command(["start"]))
async def process_inputs(client: Client, message: Message):
    global user_states, POST_INTERVAL, temp_posts
    user_id = message.from_user.id
    text = message.text or ""
    state = user_states.get(user_id)

    if state == "waiting_add_channel" and message.text:
        ch = f"@{text.replace('https://t.me/', '').replace('t.me/', '').strip('@ ')}"
        add_channel_db(ch)
        await message.reply_text(f"✅ تم إضافة القناة `{ch}` بنجاح!", reply_markup=get_main_reply_keyboard())
        user_states[user_id] = None
        return

    if state == "waiting_custom_time" and message.text:
        try:
            POST_INTERVAL = int(text.strip()) * 60
            user_states[user_id] = None
            await message.reply_text(f"✅ **تم ضبط الفارق إلى `{int(text.strip())}` دقيقة.**", reply_markup=get_main_reply_keyboard())
            return
        except ValueError:
            await message.reply_text("❌ أرسل أرقاماً فقط.")
            return

    if not state and message.text not in BUTTON_TEXTS:
        temp_posts[user_id] = {
            'chat_id': message.chat.id,
            'msg_id': message.id,
            'media_group_id': str(message.media_group_id) if message.media_group_id else None,
            'rec_start': 0,
            'rec_interval': 3600,
            'rec_repeats': -1
        }

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 نشر مجدول عادي", callback_data="type_normal")],
            [InlineKeyboardButton("🔄 نشر مكرر تلقائي", callback_data="type_recurring")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="action_cancel")]
        ])

        await message.reply_text("📥 **تم استلام المنشور!** اختر نوع النشر المطلوب:", reply_markup=kb)

# ==================== 8. التشغيل الرئيسي ====================

async def main():
    init_db()
    await app.start()
    print("[✓] تم تشغيل البوت بنجاح!")
    
    try:
        await app.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    await app.set_bot_commands([
        BotCommand("start", "💎 القائمة الرئيسية")
    ])

    asyncio.create_task(publish_worker())
    await idle()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("تم الإيقاف.")
