import os
import asyncio
import sqlite3
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

# إنشاء Event Loop رئيسي قبل استيراد pyrogram لتفادي خطأ Python
try:
    asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

nest_asyncio.apply()

# ==================== خادم الويب المتوافق مع Render ====================
web_app = Flask('')

@web_app.route('/')
def home():
    return "Bot is Running 24/7!"

def run_web():
    # قراءة المنفذ ديناميكياً من بيئة Render
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

# تشغيل خادم الويب كـ daemon thread في الخلفية
threading.Thread(target=run_web, daemon=True).start()

# ==================== 1. الإعدادات والبيانات ====================
API_ID = 32087655
API_HASH = "0276a0250c2cfc8a1dde70b0f9f92fcd"
BOT_TOKEN = "8811469771:AAFYUx7hBFRCzD5cX6HsN0lGW71ZFnzDwP8"
OWNER_ID = 2071492262

# التحكم في النشر
POST_INTERVAL = 300            # الفارق الزمني الافتراضي بالثواني (300 ثانية = 5 دقائق)
is_paused = True               # متوقف افتراضياً حتى تعطي الإذن
last_post_time = None          # توقيت نشر آخر منشور
custom_footer = ""             # الحقوق/التوقيع التلقائي
user_states = {}            
temp_posts = {}               # لحفظ الرسائل المؤقتة قبل اختيار النوع

app = Client("my_scheduler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

BUTTON_TEXTS = [
    "▶️ بدء / استئناف النشر",
    "⏸️ إيقاف النشر مؤقتاً",
    "⏱️ تغيير الفارق الزمني",
    "📢 إدارة القنوات",
    "📊 حالة النشر والطابور",
    "🔄 المنشورات المكررة",
    "🎨 إعداد الحقوق والتوقيع",
    "🗑️ إفراغ الطابور بالكامل",
    "📈 إحصائيات النشر"
]

# ==================== 2. إدارة قاعدة البيانات (SQLite) ====================

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

    def ensure_columns(table, required_columns):
        cursor.execute(f"PRAGMA table_info({table})")
        existing_cols = {row[1] for row in cursor.fetchall()}
        for col_name, col_type in required_columns:
            if col_name not in existing_cols:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")

    ensure_columns("queue", [
        ("chat_id", "INTEGER"),
        ("message_id", "INTEGER"),
        ("media_group_id", "TEXT"),
        ("channels", "TEXT"),
    ])
    ensure_columns("recurring_posts", [
        ("chat_id", "INTEGER"),
        ("message_id", "INTEGER"),
        ("media_group_id", "TEXT"),
        ("channels", "TEXT"),
        ("next_run_timestamp", "REAL"),
        ("repeat_interval_seconds", "INTEGER"),
        ("remaining_repeats", "INTEGER"),
    ])

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

def move_queue_item_to_end(queue_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id, message_id, media_group_id, channels FROM queue WHERE id = ?", (queue_id,))
    row = cursor.fetchone()
    if row:
        chat_id, message_id, media_group_id, channels = row
        cursor.execute("DELETE FROM queue WHERE id = ?", (queue_id,))
        cursor.execute(
            "INSERT INTO queue (chat_id, message_id, media_group_id, channels) VALUES (?, ?, ?, ?)",
            (chat_id, message_id, media_group_id, channels)
        )
    conn.commit()
    conn.close()

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

# ==================== 3. لوحة الأزرار الرئيسية ====================

def get_main_reply_keyboard():
    pause_text = "▶️ بدء / استئناف النشر" if is_paused else "⏸️ إيقاف النشر مؤقتاً"
    keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton(pause_text), KeyboardButton("⏱️ تغيير الفارق الزمني")],
            [KeyboardButton("📢 إدارة القنوات"), KeyboardButton("📊 حالة النشر والطابور")],
            [KeyboardButton("🔄 المنشورات المكررة"), KeyboardButton("🎨 إعداد الحقوق والتوقيع")],
            [KeyboardButton("📈 إحصائيات النشر"), KeyboardButton("🗑️ إفراغ الطابور بالكامل")]
        ],
        resize_keyboard=True
    )
    return keyboard

# ==================== 4. محرك النشر والمحرك التكراري ====================

async def publish_item(chat_id, msg_id, media_group_id, channels_list):
    global custom_footer
    for ch in channels_list:
        ch = ch.strip()
        if not ch:
            continue
        try:
            if media_group_id and media_group_id != "None":
                await app.copy_media_group(chat_id=ch, from_chat_id=chat_id, message_id=msg_id)
            else:
                await app.copy_message(chat_id=ch, from_chat_id=chat_id, message_id=msg_id)

            if custom_footer:
                try:
                    await app.send_message(chat_id=ch, text=custom_footer)
                except Exception as footer_err:
                    print(f"[!] تعذر إرسال التوقيع في {ch}: {footer_err}")

            increment_stat(ch, success=True)
            print(f"[✓] تم النشر بنجاح في القناة: {ch}")
        except Exception as e:
            increment_stat(ch, success=False)
            print(f"[!] خطأ أثناء النشر في {ch}: {e}")

async def publish_worker():
    global last_post_time, is_paused
    while True:
        try:
            now = datetime.now()
            now_ts = now.timestamp()

            recurring_items = get_recurring_db()
            for r in recurring_items:
                r_id, chat_id, msg_id, media_group_id, chs_str, next_run_ts, interval_sec, remaining = r
                if now_ts >= next_run_ts:
                    channels = chs_str.split(",")
                    await publish_item(chat_id, msg_id, media_group_id, channels)
                    
                    new_remaining = remaining - 1 if remaining > 0 else -1
                    next_ts = now_ts + interval_sec
                    update_recurring_next_run(r_id, next_ts, new_remaining)

            if not is_paused:
                queue_items = get_queue_db()
                if queue_items:
                    should_publish = False
                    if last_post_time is None:
                        should_publish = True
                    else:
                        elapsed_seconds = (now - last_post_time).total_seconds()
                        if elapsed_seconds >= POST_INTERVAL:
                            should_publish = True

                    if should_publish:
                        item = queue_items[0]
                        q_id, chat_id, msg_id, media_group_id, chs_str = item
                        await publish_item(chat_id, msg_id, media_group_id, chs_str.split(","))
                        last_post_time = datetime.now()
                        pop_queue_db(q_id)

        except Exception as e:
            print(f"[!] خطأ في محرك النشر: {e}")
            
        await asyncio.sleep(3)

# ==================== 5. استقبال الأوامر والأزرار الرئيسية ====================

admin_filter = filters.private & filters.user(OWNER_ID)

@app.on_message(filters.command("start") & admin_filter)
async def start_cmd(client: Client, message: Message):
    await message.reply_text(
        "💎 **أهلاً بك في نظام النشر والتكرار المطور!**\n\n"
        "✨ **المزايا:**\n"
        "🟢 نشر عادي بطابور جدولة متسلسل.\n"
        "🔄 منشورات مكررة بتوقيت وتكرار مخصص.\n"
        "🎨 إضافة التوقيع والحقوق التلقائية.\n"
        "⚡ نشر فوري وتعديل كامل.\n\n"
        "👇 **اختر من الأزرار الملونة أدناه للبدء:**",
        reply_markup=get_main_reply_keyboard()
    )

@app.on_message(filters.command("help") & admin_filter)
async def help_cmd(client: Client, message: Message):
    await message.reply_text(
        "📖 **دليل استخدام البوت:**\n\n"
        "1️⃣ أضف قنواتك من `📢 إدارة القنوات`.\n"
        "2️⃣ أرسل أي منشور للبوت (نص/صورة/فيديو) ثم اختر نوعه.\n"
        "3️⃣ تحكم بسرعة النشر من `⏱️ تغيير الفارق الزمني`.\n"
        "4️⃣ راقب الطابور من `📊 حالة النشر والطابور`.\n"
        "5️⃣ فعّل توقيعاً تلقائياً من `🎨 إعداد الحقوق والتوقيع`.\n"
        "6️⃣ تابع الأداء من `📈 إحصائيات النشر`.\n"
        "7️⃣ استخدم `▶️/⏸️` للتحكم بتشغيل أو إيقاف المحرك.",
        reply_markup=get_main_reply_keyboard()
    )

@app.on_message(admin_filter & filters.text & filters.create(lambda _, __, m: m.text in BUTTON_TEXTS))
async def handle_reply_buttons(client: Client, message: Message):
    global is_paused, POST_INTERVAL, user_states, last_post_time, custom_footer
    user_id = message.from_user.id
    text = message.text.strip()

    if text in ["⏸️ إيقاف النشر مؤقتاً", "▶️ بدء / استئناف النشر"]:
        is_paused = not is_paused
        if is_paused:
            status_msg = "🔴 **تم إيقاف النشر التلقائي مؤقتاً.**"
        else:
            last_post_time = None
            status_msg = f"🟢 **تم تشغيل النشر بنجاح!**\nسيتم نشر أول منشور **فوراً** وفارق `{round(POST_INTERVAL/60, 1)}` دقيقة بين البقية."

        await message.reply_text(status_msg, reply_markup=get_main_reply_keyboard())

    elif text == "📢 إدارة القنوات":
        target_channels = get_channels()
        ch_text = "📢 **القنوات المسجلة للنشر:**\n\n"
        if target_channels:
            for i, c in enumerate(target_channels, 1):
                ch_text += f"{i}. 📌 `{c}`\n"
        else:
            ch_text += "⚠️ لا توجد أي قنوات مضافة حتى الآن!\n"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ إضافة قناة جديدة", callback_data="add_channel")],
            [InlineKeyboardButton("❌ حذف قناة", callback_data="remove_channel_menu")]
        ])
        await message.reply_text(ch_text, reply_markup=kb)

    elif text == "⏱️ تغيير الفارق الزمني":
        time_inline_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⚡ 5 دقائق", callback_data="set_time_5"),
                InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="set_time_30"),
                InlineKeyboardButton("🕐 1 ساعة", callback_data="set_time_60")
            ],
            [
                InlineKeyboardButton("🕒 3 ساعات", callback_data="set_time_180"),
                InlineKeyboardButton("🕔 5 ساعات", callback_data="set_time_300"),
                InlineKeyboardButton("🕕 12 ساعة", callback_data="set_time_720")
            ],
            [InlineKeyboardButton("✏️ إدخال عدد الدقائق يدوياً", callback_data="set_custom_time")]
        ])
        await message.reply_text(
            f"⏱️ **اختر الفارق الزمني المطلوبة:**\n*(الحالي: `{round(POST_INTERVAL/60, 1)}` دقيقة)*", 
            reply_markup=time_inline_keyboard
        )

    elif text == "🎨 إعداد الحقوق والتوقيع":
        footer_status = f"`{custom_footer}`" if custom_footer else "❌ *غير مفعّل*"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ تعديل الحقوق", callback_data="set_footer")],
            [InlineKeyboardButton("🗑️ إزالة الحقوق", callback_data="clear_footer")]
        ])
        await message.reply_text(
            f"🎨 **التوقيع/الحقوق الحالية أسفل الرسائل:**\n{footer_status}",
            reply_markup=kb
        )

    elif text == "📊 حالة النشر والطابور":
        queue = get_queue_db()
        queue_len = len(queue)
        status_str = "متوقف مؤقتاً 🔴" if is_paused else "يعمل بنجاح 🟢"

        buttons = []
        if queue_len > 0:
            row = []
            for idx, item in enumerate(queue, 1):
                q_id = item[0]
                row.append(InlineKeyboardButton(f"📌 {idx}", callback_data=f"view_post_{q_id}_{idx}"))
                if len(row) == 5:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)

        reply_kb = InlineKeyboardMarkup(buttons) if buttons else None

        msg_text = (
            f"📊 **تفاصيل وحالة النظام:**\n\n"
            f"▫️ المنشورات المعلقة: `{queue_len}`\n"
            f"▫️ حالة المحرك: **{status_str}**\n"
            f"▫️ الفارق الزمني: `{round(POST_INTERVAL / 60, 1)}` دقيقة\n"
        )
        if queue_len > 0:
            msg_text += "\n👇 **اختر رقم المنشور للتحكم به:**"

        await message.reply_text(msg_text, reply_markup=reply_kb)

    elif text == "🔄 المنشورات المكررة":
        recs = get_recurring_db()
        if not recs:
            await message.reply_text("🔄 **لا توجد أي منشورات مكررة حالياً.**\nقم بإرسال منشور واختيار `منشور مكرر` لإضافته.")
            return

        msg_text = "🔄 **قائمة المنشورات المكررة المجدولة:**\n\n"
        buttons = []
        for idx, r in enumerate(recs, 1):
            r_id, chat_id, msg_id, media_group_id, chs_str, next_run_ts, interval_sec, remaining = r
            dt_str = datetime.fromtimestamp(next_run_ts).strftime("%Y-%m-%d %H:%M")
            rep_str = f"{remaining} مرة" if remaining > 0 else "غير محدود ♾️"
            interval_min = round(interval_sec / 60, 1)
            
            msg_text += f"{idx}. ⏱️ النشر القادم: `{dt_str}`\n   🔄 التكرار كل: `{interval_min}` دقيقة | المتبقي: `{rep_str}`\n\n"
            buttons.append([InlineKeyboardButton(f"🗑️ حذف المنشور المكرر رقم {idx}", callback_data=f"del_rec_{r_id}")])

        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

    elif text == "🗑️ إفراغ الطابور بالكامل":
        queue_len = len(get_queue_db())
        if queue_len == 0:
            await message.reply_text("⚠️ **الطابور فارغ بالفعل.**", reply_markup=get_main_reply_keyboard())
            return
        confirm_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، إفراغ الطابور", callback_data="confirm_clear_queue")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="cancel_clear_queue")]
        ])
        await message.reply_text(
            f"⚠️ **هل أنت متأكد من حذف جميع المنشورات ({queue_len}) من الطابور؟**\nهذا الإجراء لا يمكن التراجع عنه.",
            reply_markup=confirm_kb
        )

    elif text == "📈 إحصائيات النشر":
        stats = get_stats()
        if not stats:
            await message.reply_text("📈 **لا توجد إحصائيات نشر حتى الآن.**")
            return
        msg_text = "📈 **إحصائيات النشر حسب القناة:**\n\n"
        total_success = 0
        total_fail = 0
        for ch, success, fail in stats:
            msg_text += f"📌 `{ch}` → ✅ نجح: `{success}` | ❌ فشل: `{fail}`\n"
            total_success += success
            total_fail += fail
        msg_text += f"\n📊 **الإجمالي:** ✅ `{total_success}` | ❌ `{total_fail}`"
        await message.reply_text(msg_text)

# ==================== 6. التحكم بالأزرار التفاعلية ====================

@app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery):
    global POST_INTERVAL, user_states, is_paused, last_post_time, custom_footer, temp_posts
    data = query.data
    user_id = query.from_user.id

    try:
        target_channels = get_channels()

        if data == "add_channel":
            user_states[user_id] = "waiting_add_channel"
            await query.message.edit_text("✏️ **أرسل الآن معرف القناة أو رابطها** (مثال: `@my_channel`).")

        elif data == "remove_channel_menu":
            if not target_channels:
                await query.answer("لا توجد قنوات لحذفها!", show_alert=True)
                return
            buttons = [[InlineKeyboardButton(f"❌ {c}", callback_data=f"del_ch_{c}")] for c in target_channels]
            await query.message.edit_text("🗑️ **اختر القناة المراد حذفها:**", reply_markup=InlineKeyboardMarkup(buttons))

        elif data.startswith("del_ch_"):
            ch_to_del = data.replace("del_ch_", "")
            if ch_to_del in target_channels:
                remove_channel_db(ch_to_del)
                await query.answer(f"تم حذف {ch_to_del}", show_alert=True)
                await query.message.edit_text(f"✅ تم حذف القناة `{ch_to_del}` بنجاح.")

        elif data.startswith("set_time_"):
            minutes = int(data.split("_")[-1])
            POST_INTERVAL = minutes * 60
            await query.answer(f"✅ تم التحديث!", show_alert=True)
            await query.message.edit_text(f"✅ **تم ضبط الوقت بين المنشورات إلى `{minutes}` دقيقة.**")

        elif data == "set_custom_time":
            user_states[user_id] = "waiting_custom_time"
            await query.message.edit_text("✏️ **اكتب الفارق الزمني بالدقائق:** (مثال: `120`).")

        elif data == "set_footer":
            user_states[user_id] = "waiting_footer"
            await query.message.edit_text("🎨 **أرسل التوقيع/النص الذي تريد ظهوره تلقائياً في الأسفل:**")

        elif data == "clear_footer":
            custom_footer = ""
            await query.answer("تمت الإزالة!", show_alert=True)
            await query.message.edit_text("✅ تم إزالة الحقوق والتوقيع التلقائي.")

        elif data == "type_normal":
            post_data = temp_posts.get(user_id)
            if post_data:
                add_to_queue_db(post_data['chat_id'], post_data['msg_id'], post_data['media_group_id'], target_channels)
                queue_len = len(get_queue_db())
                await query.message.edit_text(f"📥 **تم إدراج المنشور بنجاح في الطابور عادي!**\nترتيبه الحالي: `{queue_len}`")
                del temp_posts[user_id]
            else:
                await query.answer("⚠️ انتهت صلاحية هذا المنشور، أرسله مجدداً.", show_alert=True)
                await query.message.edit_text("❌ **تعذّر إدراج المنشور.**\nيرجى إرسال المنشور مرة أخرى ثم اختيار نوعه من جديد.")

        elif data == "type_recurring":
            post_data = temp_posts.get(user_id)
            if not post_data:
                await query.answer("⚠️ انتهت صلاحية هذا المنشور، أرسله مجدداً.", show_alert=True)
                await query.message.edit_text("❌ **تعذّر بدء إعداد التكرار.**\nيرجى إرسال المنشور مرة أخرى.")
            else:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🟢 نشر الآن", callback_data="rec_start_now")],
                    [InlineKeyboardButton("⏰ جدولة وتحديد وقت", callback_data="rec_open_schedule")]
                ])
                await query.message.edit_text(
                    "⏱️ **إعداد التكرار (الخطوة 1 من 3):**\n\nاختر طريقة تحديد بداية النشر من الأزرار أدناه:",
                    reply_markup=kb
                )

        elif data == "rec_start_now":
            if user_id in temp_posts:
                temp_posts[user_id]['next_run'] = datetime.now()
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="rec_int_30"), InlineKeyboardButton("🕐 1 ساعة", callback_data="rec_int_60")],
                    [InlineKeyboardButton("🕒 6 ساعات", callback_data="rec_int_360"), InlineKeyboardButton("12 ساعة", callback_data="rec_int_720")],
                    [InlineKeyboardButton("✏️ إدخال عدد الدقائق يدوياً", callback_data="rec_custom_interval")]
                ])
                await query.message.edit_text(
                    "⚡ **تم اختيار البدء فوراً.**\n\n"
                    "⏱️ **إعداد التكرار (الخطوة 2 من 3):**\nاختر أو اكتب الفارق الزمني بالدقائق بين كل تكرار والآخر:",
                    reply_markup=kb
                )

        elif data == "rec_open_schedule":
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🕒 وقت البدء الأول", callback_data="rec_set_start_time"),
                    InlineKeyboardButton("🔄 الوقت بين كل تكرار", callback_data="rec_set_interval_time")
                ],
                [InlineKeyboardButton("🔙 رجوع", callback_data="type_recurring")]
            ])
            await query.message.edit_text(
                "⚙️ **خيارات الجدولة:**\n\n"
                "حدد الإعداد الذي تريد إدخاله:",
                reply_markup=kb
            )

        elif data == "rec_set_start_time":
            user_states[user_id] = "rec_step_time"
            await query.message.edit_text(
                "🕒 **تحديد وقت البدء الأول (نظام 24 ساعة):**\n\n"
                "يرجى إرسال وقت البدء بتنسيق `الساعة:الدقيقة`\n\n"
                "💡 **أمثلة:**\n"
                "• `14:30` (الساعة 2:30 ظهراً)\n"
                "• `21:00` (الساعة 9:00 مساءً)\n"
                "• `09:15` (الساعة 9:15 صباحاً)"
            )

        elif data == "rec_set_interval_time":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="rec_int_30"), InlineKeyboardButton("🕐 1 ساعة", callback_data="rec_int_60")],
                [InlineKeyboardButton("🕒 6 ساعات", callback_data="rec_int_360"), InlineKeyboardButton("12 ساعة", callback_data="rec_int_720")],
                [InlineKeyboardButton("✏️ إدخال عدد الدقائق يدوياً من عندك", callback_data="rec_custom_interval")]
            ])
            await query.message.edit_text(
                "🔄 **الوقت بين كل تكرار وآخر:**\n\nاختر من القائمة أو اضغط زر الإدخال اليدوي لتحديد وقتك الخاص:",
                reply_markup=kb
            )

        elif data == "rec_custom_interval":
            user_states[user_id] = "rec_step_interval"
            await query.message.edit_text(
                "✏️ **أرسل الآن الفارق الزمني بالدقائق من عندك:**\n\n"
                "💡 **أمثلة:**\n"
                "• أرسل `15` لتكرار كل 15 دقيقة\n"
                "• أرسل `45` لتكرار كل 45 دقيقة\n"
                "• أرسل `90` لتكرار كل ساعة ونصف"
            )

        elif data.startswith("rec_int_"):
            minutes = int(data.split("_")[-1])
            if user_id in temp_posts:
                temp_posts[user_id]['interval_sec'] = minutes * 60
                user_states[user_id] = "rec_step_repeats"
            
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("5 مرات", callback_data="rec_rep_5"), InlineKeyboardButton("10 مرات", callback_data="rec_rep_10")],
                    [InlineKeyboardButton("♾️ تكرار لا نهائي", callback_data="rec_rep_-1")]
                ])
                await query.message.edit_text(
                    f"🔄 **إعداد التكرار (الخطوة 3 من 3):**\n\n"
                    f"الفارق المحدد: `{minutes}` دقيقة.\n"
                    f"كم عدد مرات تكرار المنشور؟ (اختر أو اكتب الرقم):", reply_markup=kb
                )

        elif data.startswith("rec_rep_"):
            repeats = int(data.split("_")[-1])
            post_data = temp_posts.get(user_id)
            if post_data:
                interval_sec = post_data.get('interval_sec', 3600)
                next_run = post_data.get('next_run', datetime.now())
            
                add_recurring_db(
                    post_data['chat_id'], 
                    post_data['msg_id'], 
                    post_data['media_group_id'], 
                    target_channels, 
                    next_run, 
                    interval_sec, 
                    repeats
                )
            
                rep_str = f"{repeats} مرة" if repeats > 0 else "غير محدود ♾️"
                await query.message.edit_text(
                    f"✅ **تم جدولة المنشور المكرر بنجاح!**\n\n"
                    f"📅 بدء النشر: `{next_run.strftime('%Y-%m-%d %H:%M')} (نظام 24h)`\n"
                    f"⏱️ التكرار كل: `{round(interval_sec/60, 1)}` دقيقة\n"
                    f"🔄 الكمية: `{rep_str}`"
                )
                del temp_posts[user_id]
                user_states[user_id] = None

        elif data.startswith("del_rec_"):
            r_id = int(data.replace("del_rec_", ""))
            delete_recurring_db(r_id)
            await query.answer("🗑️ تم الحذف!", show_alert=True)
            await query.message.edit_text("✅ **تم حذف المنشور المكرر بنجاح.**")

        elif data.startswith("view_post_"):
            parts = data.split("_")
            q_id = int(parts[2])
            post_idx = int(parts[3])

            queue = get_queue_db()
            target_item = next((item for item in queue if item[0] == q_id), None)

            if target_item:
                _, chat_id, msg_id, media_group_id, _ = target_item
                await query.answer(f"جاري العرض...")

                now = datetime.now()
                if is_paused:
                    status_timing = "🔴 **حالة النشر:** متوقف مؤقتاً\n"
                    delay_minutes = (post_idx - 1) * (POST_INTERVAL / 60)
                    status_timing += f"⏱️ **الموعد التقديري عند البدء:** بعد `{round(delay_minutes, 1)}` دقيقة."
                else:
                    status_timing = "🟢 **حالة النشر:** يعمل تلقائياً\n"
                    base_time = now if last_post_time is None else max(now, last_post_time + timedelta(seconds=POST_INTERVAL))
                    est_time = base_time + timedelta(seconds=(post_idx - 1) * POST_INTERVAL)
                    diff_seconds = int((est_time - now).total_seconds())
                
                    hours, remainder = divmod(diff_seconds, 3600)
                    minutes, _ = divmod(remainder, 60)
                
                    time_str = []
                    if hours > 0: time_str.append(f"{hours} ساعة")
                    if minutes > 0: time_str.append(f"{minutes} دقيقة")
                
                    diff_text = " و ".join(time_str) if time_str else "فوراً"
                    status_timing += f"⏱️ **موعد النشر المتوقع:** بعد `{diff_text}` *(الساعة {est_time.strftime('%H:%M')})*"

                if media_group_id and media_group_id != "None":
                    await app.copy_media_group(chat_id=query.message.chat.id, from_chat_id=chat_id, message_id=msg_id)
                else:
                    await app.copy_message(chat_id=query.message.chat.id, from_chat_id=chat_id, message_id=msg_id)

                action_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚡ نشر هذا المنشور الآن فوراً", callback_data=f"publish_now_{q_id}")],
                    [InlineKeyboardButton("⏭️ تأجيله إلى نهاية الطابور", callback_data=f"skip_next_{q_id}")],
                    [InlineKeyboardButton(f"🗑️ حذف المنشور رقم {post_idx}", callback_data=f"delete_single_post_{q_id}")]
                ])

                info_text = f"📌 **تفاصيل المنشور رقم `{post_idx}`:**\n\n{status_timing}"
                await app.send_message(chat_id=query.message.chat.id, text=info_text, reply_markup=action_keyboard)
            else:
                await query.answer("⚠️ المنشور غير موجود!", show_alert=True)

        elif data.startswith("publish_now_"):
            q_id = int(data.replace("publish_now_", ""))
            queue = get_queue_db()
            target_item = next((item for item in queue if item[0] == q_id), None)
            if target_item:
                _, chat_id, msg_id, media_group_id, chs_str = target_item
                await publish_item(chat_id, msg_id, media_group_id, chs_str.split(","))
                pop_queue_db(q_id)
                await query.answer("⚡ تم النشر بنجاح!", show_alert=True)
                await query.message.edit_text("✅ **تم نشر المنشور فوراً وتحديث القائمة.**")

        elif data.startswith("delete_single_post_"):
            q_id = int(data.replace("delete_single_post_", ""))
            pop_queue_db(q_id)
            await query.answer("🗑️ تم الحذف بنجاح!", show_alert=True)
            await query.message.edit_text("✅ **تم حذف المنشور من الطابور.**")

        elif data == "confirm_clear_queue":
            clear_queue_db()
            last_post_time = None
            await query.answer("🗑️ تم إفراغ الطابور!", show_alert=True)
            await query.message.edit_text("🗑️ **تم مسح جميع المنشورات من الطابور بنجاح.**")

        elif data == "cancel_clear_queue":
            await query.answer("تم الإلغاء", show_alert=False)
            await query.message.edit_text("✅ **تم إلغاء عملية الإفراغ، الطابور كما هو.**")

        elif data.startswith("skip_next_"):
            q_id = int(data.replace("skip_next_", ""))
            move_queue_item_to_end(q_id)
            await query.answer("⏭️ تم تأجيل المنشور إلى آخر الطابور!", show_alert=True)
            await query.message.edit_text("✅ **تم تأجيل المنشور إلى نهاية الطابور بنجاح.**")

    except Exception as e:
        print(f"[!] خطأ في callback_handler ({data}): {e}")

# ==================== 7. استقبال البيانات والخطوات ====================

@app.on_message(admin_filter & ~filters.command(["start"]))
async def auto_collect_all_types(client: Client, message: Message):
    global user_states, POST_INTERVAL, custom_footer, temp_posts
    user_id = message.from_user.id
    text = message.text or ""
    state = user_states.get(user_id)

    if state == "waiting_add_channel" and message.text:
        clean_text = text.replace("https://t.me/", "").replace("t.me/", "").strip("@ ")
        ch = f"@{clean_text}"
        add_channel_db(ch)
        await message.reply_text(f"✅ تم إضافة القناة `{ch}` بنجاح!", reply_markup=get_main_reply_keyboard())
        user_states[user_id] = None
        return

    if state == "waiting_custom_time" and message.text:
        try:
            minutes = int(text.strip())
            POST_INTERVAL = minutes * 60
            user_states[user_id] = None
            await message.reply_text(f"✅ **تم الضبط إلى `{minutes}` دقيقة.**", reply_markup=get_main_reply_keyboard())
            return
        except ValueError:
            await message.reply_text("❌ أرسل أرقاماً فقط.")
            return

    if state == "waiting_footer" and message.text:
        custom_footer = message.text.strip()
        user_states[user_id] = None
        await message.reply_text(f"✅ **تم حفظ الحقوق:**\n`{custom_footer}`", reply_markup=get_main_reply_keyboard())
        return

    if state == "rec_step_time" and message.text:
        now = datetime.now()
        try:
            h, m = map(int, text.strip().split(":"))
            start_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if start_time < now:
                start_time += timedelta(days=1)
        except Exception:
            await message.reply_text("❌ **صيغة الوقت غير صحيحة!**\nيرجى إرسال التوقيت بنظام 24 ساعة فقط (مثال: `14:30` أو `09:15`).")
            return

        if user_id in temp_posts:
            temp_posts[user_id]['next_run'] = start_time

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="rec_int_30"), InlineKeyboardButton("🕐 1 ساعة", callback_data="rec_int_60")],
                [InlineKeyboardButton("🕒 6 ساعات", callback_data="rec_int_360"), InlineKeyboardButton("12 ساعة", callback_data="rec_int_720")],
                [InlineKeyboardButton("✏️ إدخال عدد الدقائق يدوياً من عندك", callback_data="rec_custom_interval")]
            ])
            await message.reply_text(
                f"✅ تم تحديد وقت البدء: `{start_time.strftime('%H:%M')}`\n\n"
                "⏱️ **إعداد التكرار (الخطوة 2 من 3):**\n"
                "اختر أو اكتب الفارق الزمني بالدقائق بين كل منشور:", 
                reply_markup=kb
            )
            user_states[user_id] = None
        return

    if state == "rec_step_interval" and message.text:
        try:
            minutes = int(text.strip())
            if user_id in temp_posts:
                temp_posts[user_id]['interval_sec'] = minutes * 60
                user_states[user_id] = "rec_step_repeats"

                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("5 مرات", callback_data="rec_rep_5"), InlineKeyboardButton("10 مرات", callback_data="rec_rep_10")],
                    [InlineKeyboardButton("♾️ تكرار لا نهائي", callback_data="rec_rep_-1")]
                ])
                await message.reply_text(
                    f"🔄 **إعداد التكرار (الخطوة 3 من 3):**\n\n"
                    f"الفارق المحدد: `{minutes}` دقيقة.\n"
                    f"كم عدد مرات تكرار المنشور؟ (اختر أو اكتب الرقم):", reply_markup=kb
                )
            return
        except ValueError:
            await message.reply_text("❌ يرجى كتابة أرقام فقط (مثال: 15 أو 45).")
            return

    if state == "rec_step_repeats" and message.text:
        try:
            repeats = int(text.strip())
            post_data = temp_posts.get(user_id)
            if post_data:
                interval_sec = post_data.get('interval_sec', 3600)
                next_run = post_data.get('next_run', datetime.now())
                target_channels = get_channels()

                add_recurring_db(
                    post_data['chat_id'], 
                    post_data['msg_id'], 
                    post_data['media_group_id'], 
                    target_channels, 
                    next_run, 
                    interval_sec, 
                    repeats
                )

                rep_str = f"{repeats} مرة" if repeats > 0 else "غير محدود ♾️"
                await message.reply_text(
                    f"✅ **تم جدولة المنشور المكرر بنجاح!**\n\n"
                    f"📅 بدء النشر: `{next_run.strftime('%Y-%m-%d %H:%M')}`\n"
                    f"⏱️ التكرار كل: `{round(interval_sec/60, 1)}` دقيقة\n"
                    f"🔄 الكمية: `{rep_str}`",
                    reply_markup=get_main_reply_keyboard()
                )
                del temp_posts[user_id]
                user_states[user_id] = None
                return
        except ValueError:
            await message.reply_text("❌ اكتب رقماً صحيحاً (أو -1 لتكرار غير محدود).")
            return

    # استقبال أي منشور جديد من الآدمن
    if not state and message.text not in BUTTON_TEXTS:
        temp_posts[user_id] = {
            'chat_id': message.chat.id,
            'msg_id': message.id,
            'media_group_id': str(message.media_group_id) if message.media_group_id else None
        }

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 منشور عادي (جدولة)", callback_data="type_normal")],
            [InlineKeyboardButton("🔄 منشور مكرر", callback_data="type_recurring")]
        ])

        await message.reply_text("📥 **تم استلام المنشور!**\nاختر كيف تريد نشر هذا المنشور:", reply_markup=kb)

# ==================== 8. نقطة التشغيل الرئيسية (Main) ====================

async def main():
    init_db()
    
    # 1. تشغيل عميل Pyrogram
    await app.start()
    print("[✓] تم تشغيل بوت تيليجرام بنجاح!")
    
    # 2. حرق الـ Webhook القديم لضمان عمل البوت في وضع Long Polling بدون مشاكل
    try:
        await app.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # 3. إعداد الأوامر المتاحة للبوت
    await app.set_bot_commands([
        BotCommand("start", "💎 تشغيل البوت وعرض القائمة الرئيسية"),
        BotCommand("help", "📖 دليل استخدام البوت والتعليمات")
    ])

    # 4. تشغيل محرك النشر التلقائي في الخلفية
    asyncio.create_task(publish_worker())

    # 5. الاستماع الدائم للأحداث والأوامر
    await idle()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("تم إيقاف البوت.")
