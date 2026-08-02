import os
import asyncio
import sqlite3
import zipfile
import shutil
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
custom_footer = ""             
user_states = {}            
temp_posts = {}               

app = Client("my_scheduler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

BUTTON_TEXTS = [
    "▶️ بدء / استئناف النشر",
    "⏸️ إيقاف النشر مؤقتاً",
    "⏱️ تغيير الفارق الزمني",
    "📢 إدارة القنوات",
    "📊 حالة النشر والطابور",
    "🔄 المنشورات المكررة",
    "📁 إدارة الملفات المرفوعة",
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            photo_count INTEGER,
            file_path TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            photo_path TEXT,
            order_index INTEGER
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

# ==================== إدارة ملفات وطوابير الملفات ====================

def save_uploaded_file(file_name, photos_list, folder_path):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO uploaded_files (file_name, photo_count, file_path) VALUES (?, ?, ?)",
                   (file_name, len(photos_list), folder_path))
    file_id = cursor.lastrowid
    
    for idx, p_path in enumerate(photos_list, 1):
        cursor.execute("INSERT INTO file_queue (file_id, photo_path, order_index) VALUES (?, ?, ?)",
                       (file_id, p_path, idx))
        
    conn.commit()
    conn.close()
    return file_id

def get_uploaded_files():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_name, photo_count, file_path FROM uploaded_files")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_file_queue(file_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, photo_path, order_index FROM file_queue WHERE file_id = ? ORDER BY order_index ASC", (file_id,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def delete_file_queue_item(item_id, file_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT photo_path FROM file_queue WHERE id = ?", (item_id,))
    row = cursor.fetchone()
    if row and os.path.exists(row[0]):
        try:
            os.remove(row[0])
        except Exception:
            pass
    cursor.execute("DELETE FROM file_queue WHERE id = ?", (item_id,))
    cursor.execute("UPDATE uploaded_files SET photo_count = photo_count - 1 WHERE id = ?", (file_id,))
    conn.commit()
    conn.close()

def delete_uploaded_file_db(file_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT file_path FROM uploaded_files WHERE id = ?", (file_id,))
    row = cursor.fetchone()
    if row and os.path.exists(row[0]):
        shutil.rmtree(row[0], ignore_errors=True)
    cursor.execute("DELETE FROM uploaded_files WHERE id = ?", (file_id,))
    cursor.execute("DELETE FROM file_queue WHERE file_id = ?", (file_id,))
    conn.commit()
    conn.close()

# ==================== 3. لوحة الأزرار الرئيسية ====================

def get_main_reply_keyboard():
    pause_text = "▶️ بدء / استئناف النشر" if is_paused else "⏸️ إيقاف النشر مؤقتاً"
    keyboard = ReplyKeyboardMarkup(
        [
            [KeyboardButton(pause_text), KeyboardButton("⏱️ تغيير الفارق الزمني")],
            [KeyboardButton("📢 إدارة القنوات"), KeyboardButton("📊 حالة النشر والطابور")],
            [KeyboardButton("🔄 المنشورات المكررة"), KeyboardButton("📁 إدارة الملفات المرفوعة")],
            [KeyboardButton("🎨 إعداد الحقوق والتوقيع"), KeyboardButton("🗑️ إفراغ الطابور بالكامل")],
            [KeyboardButton("📈 إحصائيات النشر")]
        ],
        resize_keyboard=True
    )
    return keyboard

# ==================== 4. محرك النشر ====================

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
                    await app.send_message(chat_id=ch, text=custom_footer, disable_web_page_preview=True)
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
        "💎 **أهلاً بك في نظام النشر المطور!**\n\n"
        "✨ **المزايا:**\n"
        "🟢 نشر عادي وطابور جدولة.\n"
        "🔄 منشورات مكررة بتوقيت وتكرار مخصص.\n"
        "📁 رفع ملفات ZIP وتخصيص طابور مستقل لكل ملف.\n"
        "🎨 إضافة التوقيع والنصوص التشعبية.\n\n"
        "👇 **اختر من الأزرار الملونة أدناه للبدء:**",
        reply_markup=get_main_reply_keyboard()
    )

@app.on_message(admin_filter & filters.text & filters.create(lambda _, __, m: m.text in BUTTON_TEXTS))
async def handle_reply_buttons(client: Client, message: Message):
    global is_paused, POST_INTERVAL, user_states, last_post_time, custom_footer
    user_id = message.from_user.id
    text = message.text.strip()

    if text in ["⏸️ إيقاف النشر مؤقتاً", "▶️ بدء / استئناف النشر"]:
        is_paused = not is_paused
        status_msg = "🔴 **تم إيقاف النشر التلقائي مؤقتاً.**" if is_paused else f"🟢 **تم تشغيل النشر بنجاح!**\nسيتم النشر بفارق `{round(POST_INTERVAL/60, 1)}` دقيقة."
        await message.reply_text(status_msg, reply_markup=get_main_reply_keyboard())

    elif text == "📢 إدارة القنوات":
        target_channels = get_channels()
        ch_text = "📢 **القنوات المسجلة للنشر:**\n\n"
        if target_channels:
            for i, c in enumerate(target_channels, 1):
                ch_text += f"{i}. 📌 `{c}`\n"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ إضافة قناة جديدة", callback_data="add_channel")],
                [InlineKeyboardButton("❌ حذف قناة", callback_data="remove_channel_menu")],
                [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
            ])
        else:
            ch_text += "⚠️ لا توجد قنوات!"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("➕ إضافة قناة جديدة", callback_data="add_channel")], [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]])

        await message.reply_text(ch_text, reply_markup=kb)

    elif text == "📁 إدارة الملفات المرفوعة":
        files = get_uploaded_files()
        if not files:
            await message.reply_text("📁 **لا توجد ملفات مرفوعة حالياً.**\nأرسل ملف مضغوط بصيغة `.zip` يحتوي على صور لرفعه للبوت.")
            return

        msg_text = "📁 **قائمة الملفات وطوابيرها المخصصة:**\n\n"
        buttons = []
        for fid, fname, fcount, _ in files:
            msg_text += f"📦 **الملف:** `{fname}`\n🖼️ عدد صور الطابور الخاص: `{fcount}` صورة\n\n"
            buttons.append([InlineKeyboardButton(f"⚙️ طابور وإدارة: {fname}", callback_data=f"manage_file_{fid}")])

        buttons.append([InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")])
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

    elif text == "⏱️ تغيير الفارق الزمني":
        time_inline_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ 5 دقائق", callback_data="set_time_5"), InlineKeyboardButton("⏱️ 30 دقيقة", callback_data="set_time_30"), InlineKeyboardButton("🕐 1 ساعة", callback_data="set_time_60")],
            [InlineKeyboardButton("🕒 3 ساعات", callback_data="set_time_180"), InlineKeyboardButton("🕔 5 ساعات", callback_data="set_time_300"), InlineKeyboardButton("🕕 12 ساعة", callback_data="set_time_720")],
            [InlineKeyboardButton("✏️ إدخال عدد الدقائق يدوياً", callback_data="set_custom_time")],
            [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
        ])
        await message.reply_text(f"⏱️ **اختر الفارق الزمني:**\n*(الحالي: `{round(POST_INTERVAL/60, 1)}` دقيقة)*", reply_markup=time_inline_keyboard)

    elif text == "🎨 إعداد الحقوق والتوقيع":
        footer_status = f"`{custom_footer}`" if custom_footer else "❌ *غير مفعّل*"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ تعديل الحقوق", callback_data="set_footer")],
            [InlineKeyboardButton("🗑️ إزالة الحقوق", callback_data="clear_footer")],
            [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
        ])
        await message.reply_text(f"🎨 **التوقيع الحالي:**\n{footer_status}", reply_markup=kb)

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

        buttons.append([InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")])

        msg_text = f"📊 **تفاصيل النظام:**\n\n▫️ المعلقة: `{queue_len}`\n▫️ المحرك: **{status_str}**\n▫️ الفارق الزمني: `{round(POST_INTERVAL / 60, 1)}` دقيقة\n"
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

    elif text == "🗑️ إفراغ الطابور بالكامل":
        queue_len = len(get_queue_db())
        if queue_len == 0:
            await message.reply_text("⚠️ **الطابور الفارغ بالفعل.**")
            return
        confirm_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، إفراغ الطابور", callback_data="confirm_clear_queue")],
            [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
        ])
        await message.reply_text(f"⚠️ **هل أنت متأكد من مسح جميع المنشورات ({queue_len}) من الطابور الرئيسي؟**", reply_markup=confirm_kb)

    elif text == "📈 إحصائيات النشر":
        stats = get_stats()
        if not stats:
            await message.reply_text("📈 **لا توجد إحصائيات بعد.**")
            return
        msg_text = "📈 **إحصائيات النشر:**\n\n"
        for ch, success, fail in stats:
            msg_text += f"📌 `{ch}` → ✅ `{success}` | ❌ `{fail}`\n"
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]]))

# ==================== 6. التحكم بالأزرار التفاعلية وطوابير الملفات ====================

@app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery):
    global POST_INTERVAL, user_states, is_paused, last_post_time, custom_footer, temp_posts
    data = query.data
    user_id = query.from_user.id

    try:
        target_channels = get_channels()

        if data == "action_cancel":
            user_states[user_id] = None
            await query.answer("تم الإلغاء!")
            await query.message.edit_text("❌ **تم إلغاء العملية.**")

        elif data.startswith("manage_file_"):
            fid = int(data.replace("manage_file_", ""))
            files = get_uploaded_files()
            fitem = next((f for f in files if f[0] == fid), None)
            if fitem:
                _, fname, fcount, _ = fitem
                fqueue = get_file_queue(fid)
                
                kb = [
                    [InlineKeyboardButton("📋 عرض وتصفح طابور الملف", callback_data=f"view_fq_{fid}")],
                    [InlineKeyboardButton("📥 إدراج كافة طابور الملف للطابور الرئيسي", callback_data=f"enqueue_file_{fid}")],
                    [InlineKeyboardButton("🗑️ حذف هذا الملف وطابوره", callback_data=f"delete_file_{fid}")],
                    [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
                ]
                await query.message.edit_text(
                    f"📁 **إدارة الملف:** `{fname}`\n"
                    f"📊 **عدد صور الطابور الخاص:** `{len(fqueue)}` صورة\n\n"
                    f"اختر ما تريد القيام به:",
                    reply_markup=InlineKeyboardMarkup(kb)
                )

        elif data.startswith("view_fq_"):
            fid = int(data.replace("view_fq_", ""))
            fqueue = get_file_queue(fid)
            files = get_uploaded_files()
            fitem = next((f for f in files if f[0] == fid), None)
            fname = fitem[1] if fitem else ""

            if not fqueue:
                await query.answer("⚠️ طابور الملف فارغ!", show_alert=True)
                return

            msg_text = f"📋 **طابور الصور الخواص بالملف (`{fname}`):**\n\n"
            buttons = []
            for item_id, ppath, idx in fqueue:
                pname = os.path.basename(ppath)
                msg_text += f"🖼️ `{idx}`. {pname}\n"
                buttons.append([
                    InlineKeyboardButton(f"📥 إدراج الصورة {idx}", callback_data=f"enq_item_{item_id}_{fid}"),
                    InlineKeyboardButton(f"❌ حذف {idx}", callback_data=f"del_fq_item_{item_id}_{fid}")
                ])

            buttons.append([InlineKeyboardButton("🔵 رجوع للملف", callback_data=f"manage_file_{fid}")])
            await query.message.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

        elif data.startswith("enq_item_"):
            parts = data.split("_")
            item_id = int(parts[2])
            fid = int(parts[3])
            
            conn = sqlite3.connect("bot_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT photo_path FROM file_queue WHERE id = ?", (item_id,))
            row = cursor.fetchone()
            conn.close()

            if row and os.path.exists(row[0]):
                sent_msg = await app.send_photo(chat_id=user_id, photo=row[0])
                add_to_queue_db(sent_msg.chat.id, sent_msg.id, None, target_channels)
                await query.answer("تم إدراج الصورة في طابور النشر الرئيسي!", show_alert=True)

        elif data.startswith("del_fq_item_"):
            parts = data.split("_")
            item_id = int(parts[3])
            fid = int(parts[4])

            delete_file_queue_item(item_id, fid)
            await query.answer("تم حذف الصورة من طابور الملف!", show_alert=True)
            
            # إعادة تحذيث القائمة
            fqueue = get_file_queue(fid)
            if fqueue:
                await query.message.edit_text(f"✅ **تم تحديث طابور الملف.** اضغط لعرض القائمة من جديد.", 
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 عرض الطابور", callback_data=f"view_fq_{fid}")]]))
            else:
                await query.message.edit_text("⚠️ **أصبح طابور هذا الملف فارغاً الآن.**")

        elif data.startswith("enqueue_file_"):
            fid = int(data.replace("enqueue_file_", ""))
            fqueue = get_file_queue(fid)
            if fqueue:
                for _, photo_path, _ in fqueue:
                    if os.path.exists(photo_path):
                        sent_msg = await app.send_photo(chat_id=user_id, photo=photo_path)
                        add_to_queue_db(sent_msg.chat.id, sent_msg.id, None, target_channels)
                await query.answer("تم إدراج كامل طابور الملف للنشر!", show_alert=True)
                await query.message.edit_text("✅ **تم نقل كامل منشورات طابور الملف إلى طابور النشر التلقائي الرئيسي.**")

        elif data.startswith("delete_file_"):
            fid = int(data.replace("delete_file_", ""))
            delete_uploaded_file_db(fid)
            await query.answer("تم حذف الملف وطابوره!", show_alert=True)
            await query.message.edit_text("🗑️ **تم حذف الملف بكافة طابوره وصوره نهائياً.**")

        elif data == "type_normal":
            post_data = temp_posts.get(user_id)
            if post_data:
                add_to_queue_db(post_data['chat_id'], post_data['msg_id'], post_data['media_group_id'], target_channels)
                await query.message.edit_text("📥 **تم إدراج المنشور بنجاح في الطابور الرئيسي!**")
                del temp_posts[user_id]

        elif data == "confirm_clear_queue":
            clear_queue_db()
            await query.message.edit_text("🗑️ **تم إفراغ الطابور الرئيسي.**")

    except Exception as e:
        print(f"[!] خطأ Callback: {e}")

# ==================== 7. استقبال الملفات المضغوطة ZIP والتفاعل ====================

@app.on_message(admin_filter & filters.document)
async def handle_zip_file(client: Client, message: Message):
    if message.document.file_name and message.document.file_name.lower().endswith(".zip"):
        msg = await message.reply_text("📥 **جاري تنزيل الملف وحفظ الصور وتعيين طابور خاص بالملف... يرجى الانتظار**")
        try:
            download_path = await message.download()
            extract_folder = f"uploaded_files/{int(datetime.now().timestamp())}"
            os.makedirs(extract_folder, exist_ok=True)

            with zipfile.ZipFile(download_path, 'r') as zip_ref:
                zip_ref.extractall(extract_folder)

            photos = [os.path.join(extract_folder, f) for f in os.listdir(extract_folder) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
            photos.sort()

            if not photos:
                await msg.edit_text("⚠️ **الملف المضغوط لا يحتوي على صور تدعم الصيغ المعيارية (JPG, PNG, WEBP).**")
                shutil.rmtree(extract_folder, ignore_errors=True)
            else:
                fid = save_uploaded_file(message.document.file_name, photos, extract_folder)
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 عرض وطابور الملف", callback_data=f"manage_file_{fid}")],
                    [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
                ])
                await msg.edit_text(
                    f"✅ **تم حفظ الملف وتشكيل طابور خاص به بنجاح!**\n\n"
                    f"📦 **اسم الملف:** `{message.document.file_name}`\n"
                    f"🖼️ **عدد صور الطابور الخاص:** `{len(photos)}` صورة\n\n"
                    f"يمكنك الآن التوجه لقسم `📁 إدارة الملفات المرفوعة` للتحكم بطابور هذا الملف.",
                    reply_markup=kb
                )
            
            if os.path.exists(download_path):
                os.remove(download_path)

        except Exception as e:
            await msg.edit_text(f"❌ **حدث خطأ أثناء فك الملف:** {e}")

@app.on_message(admin_filter & ~filters.command(["start"]))
async def auto_collect_all_types(client: Client, message: Message):
    global user_states, POST_INTERVAL, custom_footer, temp_posts
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
            await message.reply_text(f"✅ **تم الضبط إلى `{int(text.strip())}` دقيقة.**", reply_markup=get_main_reply_keyboard())
            return
        except ValueError:
            await message.reply_text("❌ أرسل أرقاماً فقط.")
            return

    if state == "waiting_footer" and message.text:
        custom_footer = message.text.strip()
        user_states[user_id] = None
        await message.reply_text(f"✅ **تم حفظ التوقيع.**", reply_markup=get_main_reply_keyboard())
        return

    if not state and message.text not in BUTTON_TEXTS:
        temp_posts[user_id] = {
            'chat_id': message.chat.id,
            'msg_id': message.id,
            'media_group_id': str(message.media_group_id) if message.media_group_id else None
        }

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 منشور عادي (جدولة)", callback_data="type_normal")],
            [InlineKeyboardButton("🔄 منشور مكرر", callback_data="type_recurring")],
            [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
        ])

        await message.reply_text("📥 **تم استلام المنشور!** اختر طريقة النشر:", reply_markup=kb)

# ==================== 8. نقطة التشغيل الرئيسية ====================

async def main():
    init_db()
    await app.start()
    print("[✓] تم تشغيل البوت بنجاح!")
    
    try:
        await app.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    await app.set_bot_commands([
        BotCommand("start", "💎 القائمة الرئيسية"),
        BotCommand("help", "📖 التعليمات")
    ])

    asyncio.create_task(publish_worker())
    await idle()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("تم الإيقاف.")
