import os
import dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters
from utils.data_processing import upload_json_to_mongodb, upload_csv_to_mysql


dotenv.load_dotenv()


async def start(update: Update, context: CallbackContext) -> None:
    # Define the custom keyboard with quick reply options
    keyboard = [
        ["Upload Data", "Query Data"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text('Hi! Welcome to TraderJohns\'s Bot. You can upload your data, create your database, and interact with your data!', reply_markup=reply_markup)


# Handle "Upload Data" selection and ask for the data type
async def handle_upload_data(update: Update, context: CallbackContext) -> None:
    # Ask the user to select CSV or JSON
    keyboard = [["CSV", "JSON"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        "What kind of data would you like to upload? Please choose CSV or JSON.",
        reply_markup=reply_markup
    )


# Handle CSV selection and prompt for file upload
async def handle_csv_selection(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("Please upload your CSV file.")


# Handle JSON selection and prompt for file upload
async def handle_json_selection(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("Please upload your JSON file.")


async def process_json_file(update: Update, context: CallbackContext) -> None:
    try:
        # Get the file object from Telegram
        file = await context.bot.get_file(update.message.document.file_id)
        file_name = update.message.document.file_name
        file_path = f'/tmp/{file_name}'

        # Download the file to the local path
        await file.download_to_drive(file_path)

        # Call the utility function to upload JSON to MongoDB
        upload_json_to_mongodb(file_path, file_name.split('.')[0])

        await update.message.reply_text("JSON file has been processed and stored in MongoDB.")
    except Exception as e:
        await update.message.reply_text("An error occurred, please try again.")


async def handle_csv_selection(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("Please upload your CSV file.")


async def process_csv_file(update: Update, context: CallbackContext) -> None:
    try:
        # Download the CSV file
        file = await context.bot.get_file(update.message.document.file_id)
        file_name = update.message.document.file_name
        file_path = f'/tmp/{file_name}'

        # Download the file to the specified path
        await file.download_to_drive(file_path)

        # Call the utility function to upload CSV to MySQL
        table_name = file_name.split('.')[0]  # Use the file name (without extension) as the table name
        upload_csv_to_mysql(file_path, table_name=table_name)

        await update.message.reply_text(f"CSV file has been processed and stored in the '{table_name}' table in MySQL.")
    except Exception as e:
        await update.message.reply_text("An error occurred while processing the CSV file. Please try again.")


def main() -> None:
    # Initialize the bot token from environment variables
    token = os.getenv('BOT_TOKEN')
    application = Application.builder().token(token).build()

    # Only add the start command handler
    application.add_handler(CommandHandler("start", start))
    # Handlers for menu selections
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(Upload Data)$"), handle_upload_data))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(CSV)$"), handle_csv_selection))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(JSON)$"), handle_json_selection))

    # File upload handler for CSV
    application.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), process_csv_file))
    
    # File upload handler for JSON
    application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), process_json_file))



    # Start the bot
    application.run_polling()

if __name__ == '__main__':
    main()