# run_bot_background.py
import asyncio
import logging
import sys
import os

# This assumes the main orchestrator logic is in 'advanced_test_bot.py'
# and it has a primary entry function, e.g., 'main_orchestrator'.
from advanced_test_bot import main_orchestrator

# --- Instructions for Setup and Running as a Daemon ---
#
# 1. Install Dependencies:
#    - Create a virtual environment: `python3 -m venv .venv`
#    - Activate it: `source .venv/bin/activate`
#    - Install all required packages: `pip install -r requirements.txt`
#
# 2. Configure Environment:
#    - Copy the example environment file: `cp .env.example .env`
#    - Edit the new `.env` file and fill in your actual API keys and secrets.
#
# 3. First Run (Interactive):
#    - Run the bot once in the foreground to log into your Telegram account:
#    > python3 run_bot_background.py
#    - Telethon will prompt for your phone number, password, and a login code.
#    - After logging in, a 'telegram_session.session' file will be created.
#    - You can stop the bot with Ctrl+C after you see it's monitoring channels.
#
# 4. Running as a Daemon (in the background):
#    - To run the bot persistently on a server, use a tool like 'nohup' or 'screen'.
#
#    - Using 'nohup':
#      This command runs the bot, ignores hangup signals, and redirects all
#      output (stdout and stderr) to a log file named 'bot_daemon.log'.
#      The '&' at the end runs the process in the background.
#
#      > nohup python3 run_bot_background.py > bot_daemon.log 2>&1 &
#
#    - To find the process later:
#      > ps aux | grep run_bot_background.py
#
#    - To stop the bot, find its Process ID (PID) and use the kill command:
#      > kill <PID>

if __name__ == '__main__':
    # Check for .env file to ensure user has configured the bot
    if not os.path.exists('.env'):
        print("ERROR: '.env' file not found.", file=sys.stderr)
        print("Please copy '.env.example' to '.env' and fill in your API keys.", file=sys.stderr)
        sys.exit(1)

    try:
        # Run the main asynchronous entry point of the bot
        asyncio.run(main_orchestrator())
    except KeyboardInterrupt:
        # This will be triggered if you run it in the foreground and press Ctrl+C
        logging.info("Shutdown requested by user.")
    except Exception as e:
        # Log any critical error that might cause the main orchestrator to crash
        logging.critical(f"The main orchestrator has crashed: {e}", exc_info=True)
