import os
import dotenv
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackContext, MessageHandler, filters
from utils.data_processing import upload_json_to_mongodb, upload_csv_to_mysql
from utils.query_data import get_mysql_tables, get_mongodb_collections, format_nested_fields

dotenv.load_dotenv()


async def start(update: Update, context: CallbackContext) -> None:
    # Define the custom keyboard with quick reply options
    keyboard = [["Upload Data", "Query Data"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    # Construct the welcome message with descriptions
    welcome_message = (
        "👋 Welcome to TraderJohn's Bot!\n\n"
        "This bot helps you upload data, create databases, and interact with your data efficiently. "
        "Here's what you can do:\n\n"
        
        "📂 **Upload Data**\n"
        "    - Upload your data to MongoDB or MySQL.\n"
        "    - **Steps**:\n"
        "      1. Select 'Upload Data'.\n"
        "      2. Choose the file type: CSV or JSON.\n"
        "      3. Upload your file, and it will be processed and stored in the appropriate database.\n\n"
        
        "📊 **Query Data**\n"
        "    - View all your uploaded data.\n"
        "    - Lists all:\n"
        "      - **Collections (JSON)** in MongoDB\n"
        "      - **Tables (CSV)** in MySQL\n"
        "    - Displays their schemas (field names and types).\n"
        "    - Select 'Query Data' to see the details.\n\n"
        
        "📄 **File Upload Options**\n"
        "    - **CSV Files**:\n"
        "      - Stored in MySQL as tables.\n"
        "      - Automatically creates table schemas based on the CSV structure.\n"
        "    - **JSON Files**:\n"
        "      - Stored in MongoDB as collections.\n"
        "      - Automatically infers schemas from the JSON structure.\n\n"
        
        "🔄 **Back to Menu**\n"
        "    - Return to the main menu from any stage.\n"
        "    - Select 'Back to Menu' to navigate back and choose another action.\n\n"
        
        "🔧 **Error Handling**\n"
        "    - If any error occurs (e.g., invalid file format or database connection issues), "
        "you'll receive an error message with instructions to retry.\n\n"
        
        "Use the buttons below to get started!"
    )

    await update.message.reply_text(welcome_message, reply_markup=reply_markup)


def build_reply_markup(options: list) -> ReplyKeyboardMarkup:
    options.append(["Back to Menu"])  # Add the "Back to Menu" option
    return ReplyKeyboardMarkup(options, one_time_keyboard=True, resize_keyboard=True)


# Handle "Upload Data" selection and ask for the data type
async def handle_upload_data(update: Update, context: CallbackContext) -> None:
    # Ask the user to select CSV or JSON
    keyboard = [["CSV", "JSON"]]
    reply_markup = build_reply_markup(keyboard)

    await update.message.reply_text(
        "What kind of data would you like to upload? Please choose CSV or JSON.",
        reply_markup=reply_markup
    )


# Handle CSV selection and prompt for file upload
async def handle_csv_selection(update: Update, context: CallbackContext) -> None:
    reply_markup = build_reply_markup([])
    await update.message.reply_text("Please upload your CSV file.", reply_markup=reply_markup)


# Handle JSON selection and prompt for file upload
async def handle_json_selection(update: Update, context: CallbackContext) -> None:
    reply_markup = build_reply_markup([])
    await update.message.reply_text("Please upload your JSON file.", reply_markup=reply_markup)


async def process_json_file(update: Update, context: CallbackContext) -> None:
    reply_markup = build_reply_markup([])
    try:
        # Get the file object from Telegram
        file = await context.bot.get_file(update.message.document.file_id)
        file_name = update.message.document.file_name
        file_path = f'/tmp/{file_name}'

        # Download the file to the local path
        await file.download_to_drive(file_path)

        # Call the utility function to upload JSON to MongoDB
        upload_json_to_mongodb(file_path, file_name.split('.')[0])

        await update.message.reply_text("JSON file has been processed and stored in MongoDB.", reply_markup=reply_markup)
    except Exception as e:
        await update.message.reply_text("An error occurred, please try again.", reply_markup=reply_markup)


async def handle_csv_selection(update: Update, context: CallbackContext) -> None:
    reply_markup = build_reply_markup([])
    await update.message.reply_text("Please upload your CSV file.", reply_markup=reply_markup)


async def process_csv_file(update: Update, context: CallbackContext) -> None:
    reply_markup = build_reply_markup([])
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

        await update.message.reply_text(f"CSV file has been processed and stored in the '{table_name}' table in MySQL.", reply_markup=reply_markup)
    except Exception as e:
        await update.message.reply_text("An error occurred while processing the CSV file. Please try again.", reply_markup=reply_markup)


async def show_tables(update: Update, context: CallbackContext) -> None:
    # Fetch collections from MongoDB and tables from MySQL
    mongodb_details = get_mongodb_collections()
    mysql_details = get_mysql_tables()
    response = "Here are the available tables/collections:\n\n"

    # Add MongoDB collections
    response += "📂 **MongoDB Collections (JSON file format):**\n\n"
    for collection, fields in mongodb_details.items():
        response += f"- {collection}:\n\n"
        if fields:
            response += format_nested_fields(fields)  # Use the helper function for nested structures
        else:
            response += "  (No fields or empty collection)\n\n"
        response += "\n"
    response += "\n\n"

    # Add MySQL tables and their columns
    response += "📊 **MySQL Tables (CDV file format):**\n\n"
    for table, columns in mysql_details.items():
        response += f"- {table}:\n\n"
        for column_name, data_type in columns:
            response += f"  • {column_name}: {data_type}\n"
        response += "\n"
    response += "\n\n"

    keyboard = [["Start Querying"]]
    reply_markup = build_reply_markup(keyboard)
    # Send the response to the user
    await update.message.reply_text(response, parse_mode="Markdown", reply_markup=reply_markup)


def main() -> None:
    # Initialize the bot token from environment variables
    token = os.getenv('BOT_TOKEN')
    application = Application.builder().token(token).build()

    # Only add the start command handler
    application.add_handler(CommandHandler("start", start))

    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(Back to Menu)$"), start))

    # Uploading data
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(Upload Data)$"), handle_upload_data))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(CSV)$"), handle_csv_selection))
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(JSON)$"), handle_json_selection))
    # File upload handler for CSV
    application.add_handler(MessageHandler(filters.Document.MimeType("text/csv"), process_csv_file))
    # File upload handler for JSON
    application.add_handler(MessageHandler(filters.Document.MimeType("application/json"), process_json_file))

    # Showing tables
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(Query Data)$"), show_tables))

    # Start the bot
    application.run_polling()

if __name__ == '__main__':
    main()