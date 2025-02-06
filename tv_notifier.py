import logging
from datetime import date, timedelta
import time
import requests
import icalendar
import sqlite3
from schedule import every, run_pending
from telegram import Bot, Update
from telegram.ext import CommandHandler, Application, ContextTypes
import traceback
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    filename='tv_notifier.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ICAL_URL = os.getenv("ICAL_URL")
ICAL_FILE = "tvmaze_followed.ics"
DB_FILE = "tv_notifications.db"

def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS events
                         (uid TEXT PRIMARY KEY, 
                          summary TEXT,
                          start_date DATE)''')
        logging.info("Database initialized successfully")
        conn.close()
    except Exception as e:
        logging.error(f"Database initialization failed: {str(e)}")
        traceback.print_exc()

def update_schedule():
    try:
        # Download iCal file
        response = requests.get(ICAL_URL, timeout=30)
        response.raise_for_status()

        with open(ICAL_FILE, "wb") as f:
            f.write(response.content)
        logging.info("iCal file downloaded successfully")
        
        # Parse iCal and update database
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        with open(ICAL_FILE, "rb") as f:
            cal = icalendar.Calendar.from_ical(f.read())
            for event in cal.walk("vevent"):
                uid = event["uid"]
                start_date = event["dtstart"].dt.date().isoformat()
                summary = event["summary"]
                
                try:
                    cursor.execute(
                        """INSERT OR IGNORE INTO events 
                           VALUES (?, ?, ?)""", 
                        (uid, summary, start_date))
                    conn.commit()
                except sqlite3.Error as e:
                    logging.warning(f"Failed to insert event {uid}: {str(e)}")

        conn.close()
        logging.info("Database updated with new events")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download iCal file: {str(e)}")
    except Exception as e:
        logging.error(f"Error updating schedule: {str(e)}")
        traceback.print_exc()

def send_notifications():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Get events premiering tomorrow
        tomorrow = date.today() + timedelta(days=1)
        cursor.execute(
            """SELECT uid, summary 
               FROM events 
               WHERE start_date = ?""", 
            (tomorrow.isoformat(),)
        )
        upcoming_events = cursor.fetchall()

        if upcoming_events:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            for uid, summary in upcoming_events:
                show_parts = summary.split(": ")
                if len(show_parts) == 2:
                    show_name = show_parts[0]
                    episode = show_parts[1]
                    message_text = f"Reminder: {show_name}\n🔴 Episode: {episode} airs tomorrow!"
                else:
                    message_text = f"Reminder: {summary} airs tomorrow!"
                
                try:
                    bot.send_message(
                        chat_id="your_chat_id",
                        text=message_text
                    )
                    logging.info(f"Notification sent for {uid}")
                except Exception as e:
                    logging.error(f"Failed to send notification for {uid}: {str(e)}")

        conn.close()
        logging.info("Notifications sent successfully")
    except sqlite3.Error as e:
        logging.error(f"Database error during notifications: {str(e)}")
    except Exception as e:
        logging.error(f"Error sending notifications: {str(e)}")
        traceback.print_exc()

async def send_weekly_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Get today's date
        today = date.today()
        logging.info(f"Today's date: {today}")
        
        # Calculate the end of the week (Sunday)
        sunday = today + timedelta(days=(6 - today.weekday()) % 7)
        logging.info(f"Sunday date: {sunday}")

        # Get all events between today and Sunday
        cursor.execute(
            """SELECT uid, summary, start_date 
               FROM events 
               WHERE start_date BETWEEN ? AND ?""",
            (today.isoformat(), sunday.isoformat())
        )
        
        events = cursor.fetchall()
        logging.info(f"Found {len(events)} events in database")

        if not events:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="No upcoming shows scheduled for this week."
            )
            logging.info("No events found for this week")
            return

        # Organize events by date and show title
        weekly_schedule = {}
        for uid, summary, start_date_str in events:
            try:
                start_date = date.fromisoformat(start_date_str)
                if start_date < today:
                    continue  # Skip events that have already aired

                # Extract show title and episode info
                title, episode_info = summary.split(": ")
                season_episode = episode_info.split("x")
                if len(season_episode) != 2:
                    continue  # Skip invalid format

                season = season_episode[0]
                episode = season_episode[1]

                # Group by date and show title
                if start_date not in weekly_schedule:
                    weekly_schedule[start_date] = {}
                if title not in weekly_schedule[start_date]:
                    weekly_schedule[start_date][title] = []
                weekly_schedule[start_date][title].append(episode)

            except Exception as e:
                logging.error(f"Error processing event {uid}: {str(e)}")
                continue

        if not weekly_schedule:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="No upcoming shows scheduled for this week."
            )
            logging.info("No future events found")
            return

        # Create message with formatted schedule
        message = "📅 This week's TV schedule (from today to Sunday):\n\n"
        for day in sorted(weekly_schedule.keys()):
            day_str = day.strftime("%A, %B %d")
            message += f"👉 {day_str}:\n"
            for show_title, episodes in weekly_schedule[day].items():
                message += f"- {show_title} (Season {season})\n"
                for episode in episodes:
                    message += f"  • Episode {episode.lstrip('0')}\n"
            message += "\n"

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message
        )
        logging.info("Weekly schedule sent successfully")

    except sqlite3.Error as e:
        logging.error(f"Database error during weekly schedule: {str(e)}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Error fetching weekly schedule. Please try again later."
        )
    except Exception as e:
        logging.error(f"Error sending weekly schedule: {str(e)}")
        traceback.print_exc()
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Error processing your request. Please try again later."
        )
    finally:
        conn.close()


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        current_time = date.today().strftime("%Y-%m-%d %H:%M:%S")
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🤖 TV Notifier Bot is active and running!\n"
                 f"Current time: {current_time}\n\n"
                 f"Available commands:\n"
                 f"• /weekly - View this week's TV schedule\n"
                 f"• /start - Check bot status"
        )
        logging.info("Start command received")
    except Exception as e:
        logging.error(f"Error in start command: {str(e)}")
        traceback.print_exc()


def main():
    try:
        # Initialize components
        init_db()
        update_schedule()

        # Create the Application and pass it your bot token
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Add handler for the weekly command
        application.add_handler(CommandHandler('weekly', send_weekly_schedule))
        application.add_handler(CommandHandler('start', start_command))
        # Schedule jobs
        every(30).days.do(update_schedule)
        every().day.at("08:00").do(send_notifications)

        # Run the bot
        application.run_polling()

        # Run scheduler in parallel
        while True:
            run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Service stopped by user")
    except Exception as e:
        logging.error(f"Main loop error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
