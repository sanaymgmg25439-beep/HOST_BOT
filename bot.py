import json
import os
import platform
import re
import subprocess
import sys
import threading
import telebot
from telebot import types

# Bot Configuration
TOKEN = "8694882116:AAHuRn3fqWVCffF5Q-iTcsXMMwd1ViCUgQo"
ADMIN_ID = 8214230295  # သတ်မှတ်ထားသော Admin ID

bot = telebot.TeleBot(TOKEN)
active_processes = {}
ACTIVE_STATE_FILE = "active_scripts.json"
SCRIPTS_DIR = "running_scripts"

# ဖိုင်တွဲ မရှိပါက ဖန်တီးရန်
if not os.path.exists(SCRIPTS_DIR):
  os.makedirs(SCRIPTS_DIR)

STD_LIBS = {
    "os",
    "sys",
    "time",
    "math",
    "random",
    "json",
    "re",
    "subprocess",
    "threading",
    "multiprocessing",
    "asyncio",
    "datetime",
    "collections",
    "itertools",
    "functools",
    "pathlib",
    "urllib",
    "http",
    "socket",
    "sqlite3",
    "hashlib",
    "base64",
    "string",
    "logging",
    "typing",
    "traceback",
    "shutil",
    "uuid",
    "io",
    "abc",
    "contextlib",
    "operator",
    "weakref",
}

PACKAGE_MAPPING = {
    "telegram": "python-telegram-bot",
    "bs4": "beautifulsoup4",
    "cv2": "opencv-python",
    "PIL": "Pillow",
    "dotenv": "python-dotenv",
}


def extract_imports(file_path):
  imports = set()
  pattern = re.compile(
      r"^(?:\s*import\s+([a-zA-Z0-9_]+)|\s*from\s+([a-zA-Z0-9_]+)\s+import)",
      re.MULTILINE,
  )
  try:
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
      content = f.read()
      matches = pattern.findall(content)
      for match in matches:
        mod = match[0] or match[1]
        if mod and mod not in STD_LIBS:
          imports.add(mod)
  except Exception as e:
    print(f"Error reading file: {e}")
  return list(imports)


def clean_ansi(text):
  if not text:
    return ""
  ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
  return ansi_escape.sub("", text)


def save_state(file_name):
  state = []
  if os.path.exists(ACTIVE_STATE_FILE):
    try:
      with open(ACTIVE_STATE_FILE, "r") as f:
        state = json.load(f)
    except:
      pass
  if file_name not in state:
    state.append(file_name)
    with open(ACTIVE_STATE_FILE, "w") as f:
      json.dump(state, f)
  git_commit_push(f"Add running script: {file_name}")


def remove_state(file_name):
  if os.path.exists(ACTIVE_STATE_FILE):
    try:
      with open(ACTIVE_STATE_FILE, "r") as f:
        state = json.load(f)
      if file_name in state:
        state.remove(file_name)
        with open(ACTIVE_STATE_FILE, "w") as f:
          json.dump(state, f)
      git_commit_push(f"Remove stopped script: {file_name}")
    except:
      pass


def git_commit_push(message):
  try:
    subprocess.run(
        ["git", "add", ACTIVE_STATE_FILE, SCRIPTS_DIR],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    subprocess.run(
        ["git", "commit", "-m", message],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    subprocess.run(
        ["git", "push"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
  except Exception as e:
    print(f"Git sync error: {e}")


def run_script_process(file_path, file_name, chat_id):
  env = os.environ.copy()
  env["TERM"] = "xterm-256color"
  env["PYTHONUNBUFFERED"] = "1"

  cmd = [sys.executable, "-u", file_path]
  process = subprocess.Popen(
      cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
  )
  active_processes[chat_id] = process
  save_state(file_name)

  def monitor():
    stdout, stderr = process.communicate()
    if chat_id in active_processes:
      del active_processes[chat_id]
    remove_state(file_name)

    out = clean_ansi(stdout)
    err = clean_ansi(stderr)
    resp = f"🏁 **COMPLETED / STOPPED:** `{file_name}`\n\n"
    if out:
      resp += f"📤 **Output:**\n```\n{out[:3000]}\n```\n"
    if err:
      resp += f"⚠️ **Logs/Errors:**\n```\n{err[:3000]}\n```\n"

    try:
      bot.send_message(chat_id, resp, parse_mode="Markdown")
    except:
      pass

  threading.Thread(target=monitor, daemon=True).start()


# Bot စတင်ချိန်တွင် ယခင် Run နေခဲ့သော ဖိုင်များကို အလိုအလျောက် ပြန်စပေးရန်
def auto_resume_scripts():
  if not os.path.exists(ACTIVE_STATE_FILE):
    return
  try:
    with open(ACTIVE_STATE_FILE, "r") as f:
      state = json.load(f)
    for file_name in state:
      file_path = os.path.join(SCRIPTS_DIR, file_name)
      if os.path.exists(file_path):
        print(f"🔄 Auto-resuming script: {file_name}")
        run_script_process(file_path, file_name, ADMIN_ID)
  except Exception as e:
    print(f"Auto resume error: {e}")


@bot.message_handler(commands=["start", "help"])
def send_welcome(message):
  if message.from_user.id != ADMIN_ID:
    bot.reply_to(message, "⛔ ခွင့်ပြုချက်မရှိပါ။")
    return
  welcome_text = (
      "┏ 🌟 **WYP PERSISTENT HOST BOT** 🌟\n"
      "┣━━━━━━━━━━━━━━━━━━━━━━\n"
      "┃ 👑 **Status:** Auto-Resume & Git Sync Enabled\n"
      "┗━━━━━━━━━━━━━━━━━━━━━━\n\n"
      "📂 `.py` ဖိုင်ကို တိုက်ရိုက်ပို့ပါက GitHub သို့ သိမ်းဆည်းပြီး"
      " Restart ကျသွားလျှင်ပါ အလိုအလျောက် ပြန်လည် Run ပေးပါမည်။"
  )
  bot.send_message(message.chat.id, welcome_text, parse_mode="Markdown")


@bot.message_handler(content_types=["document"])
def handle_python_file(message):
  chat_id = message.chat.id
  if message.from_user.id != ADMIN_ID:
    bot.reply_to(message, "⛔ ခွင့်ပြုချက်မရှိပါ။")
    return

  file_name = message.document.file_name
  if not file_name or not file_name.endswith(".py"):
    bot.reply_to(message, "⚠️ ကျေးဇူးပြု၍ `.py` ဖိုင်ကိုသာ ပို့ပေးပါ။")
    return

  msg = bot.reply_to(message, f"📥 Downloading & Syncing `{file_name}`...")
  local_path = os.path.join(SCRIPTS_DIR, file_name)

  try:
    file_info = bot.get_file(message.document.file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    with open(local_path, "wb") as f:
      f.write(downloaded_file)

    bot.edit_message_text(
        "⚙️ Checking modules...", chat_id=chat_id, message_id=msg.message_id
    )

    # Modules များကို အလိုအလျောက် တပ်ဆင်ခြင်း
    raw_modules = extract_imports(local_path)
    for mod in raw_modules:
      pkg = PACKAGE_MAPPING.get(mod, mod)
      try:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                pkg,
                "--prefer-binary",
                "--no-cache-dir",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
      except:
        pass

    run_script_process(local_path, file_name, chat_id)

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "🛑 အလုပ်ရပ်မည် (Stop Script)", callback_data=f"stop_{file_name}"
        )
    )

    bot.edit_message_text(
        f"🟢 **RUNNING & SYNCED**\n📄 File: `{file_name}`\n⏱️ Status:"
        " GitHub သို့ သိမ်းဆည်းပြီး အလုပ်လုပ်နေပါပြီ...",
        chat_id=chat_id,
        message_id=msg.message_id,
        reply_markup=markup,
        parse_mode="Markdown",
    )

  except Exception as e:
    bot.send_message(chat_id, f"❌ Critical Error: {e}")


@bot.callback_query_handler(func=lambda call: call.data.startswith("stop_"))
def stop_script(call):
  if call.from_user.id != ADMIN_ID:
    bot.answer_callback_query(call.id, "⛔ ခွင့်ပြုချက်မရှိပါ။", show_alert=True)
    return

  file_name = call.data.replace("stop_", "", 1)
  chat_id = call.message.chat.id

  # Process ကို ရှာပြီး ရပ်တန့်ရန်
  stopped = False
  for cid, proc in list(active_processes.items()):
    if cid == chat_id:
      try:
        proc.terminate()
        proc.wait(timeout=3)
      except:
        proc.kill()
      del active_processes[cid]
      stopped = True

  remove_state(file_name)
  local_path = os.path.join(SCRIPTS_DIR, file_name)
  if os.path.exists(local_path):
    try:
      os.remove(local_path)
    except:
      pass

  bot.answer_callback_query(call.id, "✅ Script ကို ရပ်လိုက်ပါပြီ။")
  try:
    bot.edit_message_text(
        "🛑 **SCRIPT STOPPED & REMOVED**",
        chat_id=chat_id,
        message_id=call.message.message_id,
    )
  except:
    pass


if __name__ == "__main__":
  print("WYP Persistent Host Bot is starting...")
  # Bot စတင်တာနဲ့ ယခင် Run နေကျ ဖိုင်များကို ပြန်လည်စတင်ရန်
  threading.Thread(target=auto_resume_scripts, daemon=True).start()
  bot.infinity_polling()
