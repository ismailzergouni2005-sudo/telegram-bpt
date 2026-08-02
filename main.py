import os
import asyncio
import sqlite3
import zipfile
import shutil
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

# إنشاء Event Loop رئيسي لتفادي مشاكل Asyncio
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

IMAGE_EXTENSIONS = (
    '.jpg', '.jpeg', '.png', '.webp', '.bmp', 
    '.gif', '.tiff', '.tif', '.heic', '.heif', 
    '.svg', '.ico', '.avif'
)

def is_image_file(file_path):
    if file_path.lower().endswith(IMAGE_EXTENSIONS):
        return True
    mime, _ = mimetypes.guess_type(file_path)
    return mime and mime.startswith('image/')

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
            file_path TEXT,
            start_time_ts REAL DEFAULT 0,
            interval_seconds INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            photo_path TEXT,
            caption TEXT DEFAULT '',
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

def get_queue_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, chat_id, message_id, media_group_id, channels FROM queue ORDER BY id ASC")
    rows = cursor.fetchall()
    conn.close()
    return rows

def pop_queue_db(queue_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM queue WHERE id = ?", (queue_id,))
    conn.commit()
    conn.close()

def clear_queue_db():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM queue")
    conn.commit()
    conn.close()

# ==================== إدارة الملفات والطوابير ====================

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

def update_file_schedule(file_id, start_ts, interval_sec):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE uploaded_files SET start_time_ts = ?, interval_seconds = ? WHERE id = ?",
                   (start_ts, interval_sec, file_id))
    conn.commit()
    conn.close()

def get_uploaded_files():
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_name, photo_count, file_path, start_time_ts, interval_seconds FROM uploaded_files")
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_file_queue(file_id):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, photo_path, caption, order_index FROM file_queue WHERE file_id = ? ORDER BY order_index ASC", (file_id,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def update_item_caption(item_id, new_caption):
    conn = sqlite3.connect("bot_data.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE file_queue SET caption = ? WHERE id = ?", (new_caption, item_id))
    conn.commit()
    conn.close()

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

async def publish_item(chat_id, msg_id, media_group_id, channels_list, custom_text=None, photo_path=None):
    global custom_footer
    for ch in channels_list:
        ch = ch.strip()
        if not ch:
            continue
        try:
            if photo_path and os.path.exists(photo_path):
                cap = (custom_text or "") + (f"\n\n{custom_footer}" if custom_footer else "")
                await app.send_photo(chat_id=ch, photo=photo_path, caption=cap)
            else:
                if media_group_id and media_group_id != "None":
                    await app.copy_media_group(chat_id=ch, from_chat_id=chat_id, message_id=msg_id)
                else:
                    await app.copy_message(chat_id=ch, from_chat_id=chat_id, message_id=msg_id)

                if custom_footer:
                    await app.send_message(chat_id=ch, text=custom_footer, disable_web_page_preview=True)

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
            target_channels = get_channels()

            # 1. معالجة طوابير الملفات المجدولة بـ (ساعة البدء والتكرار)
            files = get_uploaded_files()
            for fid, fname, fcount, fpath, start_ts, interval_sec in files:
                if start_ts > 0 and now_ts >= start_ts and fcount > 0:
                    fqueue = get_file_queue(fid)
                    if fqueue:
                        item_id, photo_p, caption, idx = fqueue[0]
                        if os.path.exists(photo_p):
                            await publish_item(OWNER_ID, 0, None, target_channels, custom_text=caption, photo_path=photo_p)
                        
                        delete_file_queue_item(item_id, fid)
                        
                        # تحديث موعد الصورة القادمة في طابور الملف
                        if len(fqueue) - 1 > 0:
                            next_ts = now_ts + (interval_sec if interval_sec > 0 else 60)
                            update_file_schedule(fid, next_ts, interval_sec)
                        else:
                            update_file_schedule(fid, 0, 0)

            # 2. معالجة الطابور الرئيسي
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

# ==================== 5. الأوامر الرئيسية والتفاعل ====================

admin_filter = filters.private & filters.user(OWNER_ID)

@app.on_message(filters.command("start") & admin_filter)
async def start_cmd(client: Client, message: Message):
    await message.reply_text(
        "💎 **أهلاً بك في نظام النشر المطور مع الجدولة والمعاينة!**\n\n"
        "✨ **الميزات:**\n"
        "▫️ جدولة طوابير الملفات وتحديد (ساعة البدء والفرز الزمني).\n"
        "▫️ عرض المعاينة للصور والمنشورات مباشرة داخل البوت.\n"
        "▫️ تعديل الكابشن/النص المرفق للمنشورات بسهولة.\n\n"
        "👇 **اختر من اللوحة أدناه للبدء:**",
        reply_markup=get_main_reply_keyboard()
    )

@app.on_message(admin_filter & filters.text & filters.create(lambda _, __, m: m.text in BUTTON_TEXTS))
async def handle_reply_buttons(client: Client, message: Message):
    global is_paused, POST_INTERVAL, user_states, last_post_time, custom_footer
    user_id = message.from_user.id
    text = message.text.strip()

    if text in ["⏸️ إيقاف النشر مؤقتاً", "▶️ بدء / استئناف النشر"]:
        is_paused = not is_paused
        status_msg = "🔴 **تم إيقاف النشر مؤقتاً.**" if is_paused else f"🟢 **تم تشغيل النشر بنجاح!**"
        await message.reply_text(status_msg, reply_markup=get_main_reply_keyboard())

    elif text == "📁 إدارة الملفات المرفوعة":
        files = get_uploaded_files()
        if not files:
            await message.reply_text("📁 **لا توجد ملفات.** أرسل `.zip` يحتوي على الصور لرفعه.")
            return

        msg_text = "📁 **قائمة الملفات المرفوعة:**\n\n"
        buttons = []
        for fid, fname, fcount, _, start_ts, interval in files:
            status = f"⏳ جدولة: كل `{round(interval/60, 1)}` دقيقة" if start_ts > 0 else "⚪ غير مجدول"
            msg_text += f"📦 **{fname}** | 🖼️ صور: `{fcount}`\n▫️ الحالة: {status}\n\n"
            buttons.append([InlineKeyboardButton(f"⚙️ إدارة وطابور: {fname}", callback_data=f"manage_file_{fid}")])

        buttons.append([InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")])
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

    elif text == "📊 حالة النشر والطابور":
        queue = get_queue_db()
        queue_len = len(queue)
        msg_text = f"📊 **الطابور الرئيسي الحالي:** `{queue_len}` منشورات\n\n"
        buttons = []
        
        for idx, item in enumerate(queue, 1):
            q_id = item[0]
            buttons.append([
                InlineKeyboardButton(f"👁️ معاينة منشور #{idx}", callback_data=f"preview_main_q_{q_id}"),
                InlineKeyboardButton(f"❌ حذف", callback_data=f"delete_main_q_{q_id}")
            ])

        buttons.append([InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")])
        await message.reply_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

# ==================== 6. التحكم والزر المعاينة والجدولة ====================

@app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery):
    global POST_INTERVAL, user_states, is_paused, last_post_time, custom_footer, temp_posts
    data = query.data
    user_id = query.from_user.id

    try:
        if data == "action_cancel":
            user_states[user_id] = None
            await query.message.edit_text("❌ **تم إلغاء العملية.**")

        elif data.startswith("manage_file_"):
            fid = int(data.replace("manage_file_", ""))
            files = get_uploaded_files()
            fitem = next((f for f in files if f[0] == fid), None)
            if fitem:
                _, fname, fcount, _, start_ts, interval = fitem
                
                sch_info = "❌ **غير مجدول**"
                if start_ts > 0:
                    dt = datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M')
                    sch_info = f"✅ **مجدول للبدء:** `{dt}`\n⏱️ **الفارق الزمني:** `{round(interval/60,1)}` دقيقة"

                kb = [
                    [InlineKeyboardButton("📋 عرض وتعديل الصور", callback_data=f"view_fq_{fid}")],
                    [InlineKeyboardButton("⏰ ضبط ساعة البدء والتكرار", callback_data=f"set_file_time_{fid}")],
                    [InlineKeyboardButton("📥 إدراج الكل بالطابور الرئيسي", callback_data=f"enqueue_file_{fid}")],
                    [InlineKeyboardButton("🗑️ حذف الملف", callback_data=f"delete_file_{fid}")],
                    [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
                ]
                await query.message.edit_text(
                    f"📁 **إدارة الملف:** `{fname}`\n"
                    f"📊 **الصور:** `{fcount}` صورة\n"
                    f"▫️ **جدولة التوقيت:** {sch_info}\n\nاختر إجراءً:",
                    reply_markup=InlineKeyboardMarkup(kb)
                )

        elif data.startswith("set_file_time_"):
            fid = int(data.replace("set_file_time_", ""))
            user_states[user_id] = f"wait_file_start_time_{fid}"
            await query.message.edit_text(
                "⏰ **تحديد ساعة البدء والتكرار:**\n\n"
                "أرسل التوقيت والفارق الزمني بالشكل التالي:\n"
                "`ساعة:دقيقة فارق_الدقائق`\n\n"
                "📌 **مثال:** `14:30 15`\n"
                "*(تعني البدء اليوم الساعة 02:30 مساءً، ونشر صورة كل 15 دقيقة)*"
            )

        elif data.startswith("view_fq_"):
            fid = int(data.replace("view_fq_", ""))
            fqueue = get_file_queue(fid)
            if not fqueue:
                await query.answer("⚠️ طابور الملف فارغ!", show_alert=True)
                return

            buttons = []
            msg_text = f"📋 **صور طابور الملف:**\n\n"
            for item_id, ppath, caption, idx in fqueue:
                pname = os.path.basename(ppath)
                msg_text += f"🖼️ `{idx}`. {pname} | 📝 `{caption or 'بدون كابشن'}`\n"
                buttons.append([
                    InlineKeyboardButton(f"👁️ معاينة {idx}", callback_data=f"show_img_{item_id}"),
                    InlineKeyboardButton(f"✏️ تعديل وصف {idx}", callback_data=f"edit_cap_{item_id}_{fid}"),
                    InlineKeyboardButton(f"❌ حذف", callback_data=f"del_fq_item_{item_id}_{fid}")
                ])

            buttons.append([InlineKeyboardButton("🔵 رجوع للملف", callback_data=f"manage_file_{fid}")])
            await query.message.edit_text(msg_text, reply_markup=InlineKeyboardMarkup(buttons))

        elif data.startswith("show_img_"):
            item_id = int(data.replace("show_img_", ""))
            conn = sqlite3.connect("bot_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT photo_path, caption FROM file_queue WHERE id = ?", (item_id,))
            row = cursor.fetchone()
            conn.close()

            if row and os.path.exists(row[0]):
                await app.send_photo(chat_id=user_id, photo=row[0], caption=f"👁️ **معاينة الصورة:**\n\n{row[1]}")
                await query.answer("تم عرض الصورة بنجاح!")

        elif data.startswith("edit_cap_"):
            parts = data.split("_")
            item_id = int(parts[2])
            fid = int(parts[3])
            user_states[user_id] = f"wait_edit_caption_{item_id}_{fid}"
            await query.message.edit_text("📝 **أرسل النص/الوصف الجديد الذي تريد إرفاقه مع هذه الصورة:**")

        elif data.startswith("del_fq_item_"):
            parts = data.split("_")
            item_id = int(parts[3])
            fid = int(parts[4])
            delete_file_queue_item(item_id, fid)
            await query.answer("تم حذف الصورة من الطابور!", show_alert=True)
            await query.message.edit_text("✅ **تم تحديث الطابور.**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📋 عرض الطابور", callback_data=f"view_fq_{fid}")]]))

        elif data.startswith("preview_main_q_"):
            q_id = int(data.replace("preview_main_q_", ""))
            conn = sqlite3.connect("bot_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id, message_id FROM queue WHERE id = ?", (q_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                await app.copy_message(chat_id=user_id, from_chat_id=row[0], message_id=row[1])
                await query.answer("تم عرض المنشور بكامل تفاصيله!")

        elif data.startswith("delete_main_q_"):
            q_id = int(data.replace("delete_main_q_", ""))
            pop_queue_db(q_id)
            await query.answer("تم حذف المنشور من الطابور!", show_alert=True)
            await query.message.edit_text("🗑️ **تم حذف المنشور بنجاح.**")

        elif data == "type_normal":
            post_data = temp_posts.get(user_id)
            if post_data:
                add_to_queue_db(post_data['chat_id'], post_data['msg_id'], post_data['media_group_id'], get_channels())
                await query.message.edit_text("📥 **تم إضافة المنشور في الطابور الرئيسي!**")
                del temp_posts[user_id]

    except Exception as e:
        print(f"[!] خطأ تفاعل: {e}")

# ==================== 7. رفع واستقبال ملفات ZIP والرسائل ====================

@app.on_message(admin_filter & filters.document)
async def handle_zip_file(client: Client, message: Message):
    if message.document.file_name and message.document.file_name.lower().endswith(".zip"):
        msg = await message.reply_text("📥 **جاري تنزيل وفحص الصور داخل ZIP...**")
        try:
            download_path = await message.download()
            extract_folder = f"uploaded_files/{int(datetime.now().timestamp())}"
            os.makedirs(extract_folder, exist_ok=True)

            with zipfile.ZipFile(download_path, 'r') as zip_ref:
                zip_ref.extractall(extract_folder)

            photos = []
            for root, _, files in os.walk(extract_folder):
                for f in files:
                    full_p = os.path.join(root, f)
                    if is_image_file(full_p):
                        photos.append(full_p)

            photos.sort()

            if not photos:
                await msg.edit_text("⚠️ **الملف لا يحتوي على صور مدعومة.**")
                shutil.rmtree(extract_folder, ignore_errors=True)
            else:
                fid = save_uploaded_file(message.document.file_name, photos, extract_folder)
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚙️ إعدادات وطابور الملف", callback_data=f"manage_file_{fid}")],
                    [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
                ])
                await msg.edit_text(
                    f"✅ **تم رفع الملف وحفظ الصور بنجاح!**\n\n"
                    f"📦 **اسم الملف:** `{message.document.file_name}`\n"
                    f"🖼️ **عدد الصور:** `{len(photos)}` صورة\n\n"
                    f"يمكنك الآن جدولة ساعة البدء وعرض الصور وتعديلها بسهولة.",
                    reply_markup=kb
                )
            
            if os.path.exists(download_path):
                os.remove(download_path)

        except Exception as e:
            await msg.edit_text(f"❌ **حدث خطأ:** {e}")

@app.on_message(admin_filter & ~filters.command(["start"]))
async def process_inputs(client: Client, message: Message):
    global user_states, POST_INTERVAL, custom_footer, temp_posts
    user_id = message.from_user.id
    text = message.text or ""
    state = user_states.get(user_id)

    if state and state.startswith("wait_file_start_time_"):
        fid = int(state.replace("wait_file_start_time_", ""))
        try:
            time_part, interval_part = text.strip().split()
            hour, minute = map(int, time_part.split(":"))
            interval_min = int(interval_part)

            now = datetime.now()
            start_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if start_dt < now:
                start_dt += timedelta(days=1)

            update_file_schedule(fid, start_dt.timestamp(), interval_min * 60)
            user_states[user_id] = None
            await message.reply_text(
                f"✅ **تم جدولة الملف بنجاح!**\n\n"
                f"⏰ **ساعة البدء:** `{start_dt.strftime('%Y-%m-%d %H:%M')}`\n"
                f"⏱️ **التكرار بين الصور:** كل `{interval_min}` دقيقة.",
                reply_markup=get_main_reply_keyboard()
            )
            return
        except Exception:
            await message.reply_text("❌ **صيغة غير صحيحة.** أرسل بالشكل: `14:30 15` (ساعة:دقيقة ثم الدقائق بين التكرار)")
            return

    if state and state.startswith("wait_edit_caption_"):
        parts = state.split("_")
        item_id = int(parts[3])
        fid = int(parts[4])
        update_item_caption(item_id, text.strip())
        user_states[user_id] = None
        await message.reply_text("✅ **تم تحديث وصف/نص الصورة بنجاح!**", reply_markup=get_main_reply_keyboard())
        return

    if not state and message.text not in BUTTON_TEXTS:
        temp_posts[user_id] = {
            'chat_id': message.chat.id,
            'msg_id': message.id,
            'media_group_id': str(message.media_group_id) if message.media_group_id else None
        }

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 منشور عادي (جدولة)", callback_data="type_normal")],
            [InlineKeyboardButton("🔴 إلغاء", callback_data="action_cancel")]
        ])

        await message.reply_text("📥 **تم استلام المنشور!** اختر إجراءً:", reply_markup=kb)

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
