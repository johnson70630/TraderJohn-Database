import os
import json
import logging
import re
from datetime import datetime
import dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackContext, ConversationHandler, filters

from utils.data_processing import upload_json_to_mongodb, upload_csv_to_mysql
from utils.query_data import test_database_connections, get_mysql_tables, get_mongodb_collections
from utils.query_generator import QueryGenerator
from utils.execute_query import QueryExecutor
from utils.format import format_nested_fields

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv()

# Initialize query generator and executor
query_generator = QueryGenerator()
query_executor = QueryExecutor(
    mysql_config={
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    },
    mongodb_url=os.getenv('MONGODB_URL'),
    mongodb_name=os.getenv('MONGODB_NAME')
)

# States for ConversationHandler
CHOOSING, UPLOAD_FILE, QUERY_DATA = range(3)

async def start(update: Update, context: CallbackContext) -> int:
    """Start the conversation and show main menu."""
    keyboard = [
        ["Upload Data", "Query Data"],
        ["Sample Queries", "Help", "Exit"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    welcome_message = (
        "👋 Welcome to TraderJohn's Database Bot!\n\n"
        "This bot helps you manage and query your data efficiently. Here's what you can do:\n\n"
        
        "📂 **Upload Data**\n"
        "• Upload CSV files → MySQL\n"
        "• Upload JSON files → MongoDB\n\n"
        
        "📊 **Query Data**\n"
        "• View all tables and collections\n"
        "• Query using natural language\n"
        "• Get data insights\n\n"
        
        "🤔 **Natural Language Query Examples**:\n"
        "• 'Show all orders where price > 100'\n"
        "• 'Find top 5 customers with highest total_spent'\n"
        "• 'Calculate average price from products'\n"
        "• 'Count orders by customer_id'\n\n"
        
        "Please select an option to continue:"
    )
    
    await update.message.reply_text(welcome_message, reply_markup=reply_markup, parse_mode="Markdown")
    return CHOOSING

async def help_command(update: Update, context: CallbackContext) -> int:
    """Show help information."""
    help_text = (
        "🔍 **Query Examples**:\n\n"
        "1. Basic Queries:\n"
        "   • 'Show all users'\n"
        "   • 'Display products'\n\n"
        
        "2. Filtered Queries:\n"
        "   • 'Find orders where total > 1000'\n"
        "   • 'Show users where country is USA'\n\n"
        
        "3. Aggregations:\n"
        "   • 'Calculate average order total'\n"
        "   • 'Count orders by status'\n"
        "   • 'Sum sales by product'\n\n"
        
        "4. Sorting:\n"
        "   • 'Show top 10 customers by spend'\n"
        "   • 'List products ordered by price desc'\n\n"
        
        "5. Data Upload:\n"
        "   • CSV files go to MySQL tables\n"
        "   • JSON files go to MongoDB collections\n\n"
        
        "Use /start to return to main menu"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")
    return CHOOSING

async def handle_upload_data(update: Update, context: CallbackContext) -> int:
    """Handle the upload data option."""
    keyboard = [
        ["CSV", "JSON"],
        ["Back to Menu"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    await update.message.reply_text(
        "Please choose the type of file you want to upload:",
        reply_markup=reply_markup
    )
    return UPLOAD_FILE

async def handle_file_type_selection(update: Update, context: CallbackContext) -> int:
    """Handle file type selection for upload."""
    file_type = update.message.text.upper()
    context.user_data['file_type'] = file_type
    
    keyboard = [["Back to Menu"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    
    if file_type == "CSV":
        await update.message.reply_text(
            "Please upload your CSV file. It will be stored in MySQL.",
            reply_markup=reply_markup
        )
    elif file_type == "JSON":
        await update.message.reply_text(
            "Please upload your JSON file. It will be stored in MongoDB.",
            reply_markup=reply_markup
        )
    return UPLOAD_FILE

async def handle_file_upload(update: Update, context: CallbackContext) -> int:
    """Handle file upload and processing."""
    try:
        file = await context.bot.get_file(update.message.document.file_id)
        file_name = update.message.document.file_name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_path = f'/tmp/{timestamp}_{file_name}'
        
        # Download the file
        await file.download_to_drive(temp_path)
        
        # Process based on file type
        file_type = context.user_data.get('file_type', '').upper()
        if file_type == "CSV":
            table_name = file_name.split('.')[0]
            upload_csv_to_mysql(temp_path, table_name)
            await update.message.reply_text(f"CSV data has been uploaded to MySQL table '{table_name}'.")
        elif file_type == "JSON":
            collection_name = file_name.split('.')[0]
            upload_json_to_mongodb(temp_path, collection_name)
            await update.message.reply_text(f"JSON data has been uploaded to MongoDB collection '{collection_name}'.")
        
        # Clean up temporary file
        os.remove(temp_path)
        return CHOOSING
        
    except Exception as e:
        await update.message.reply_text(f"Error processing file: {str(e)}")
        return CHOOSING

async def show_data_overview(update: Update, context: CallbackContext) -> int:
    """Show overview of available data and accept natural language queries."""
    try:
        # Get available data sources
        mongodb_details = {}
        mysql_details = {}

        # Get MongoDB collections
        try:
            mongodb_details = get_mongodb_collections()
            logger.info(f"MongoDB collections found: {list(mongodb_details.keys())}")
        except Exception as e:
            logger.error(f"MongoDB error: {str(e)}")
            mongodb_details = {}

        # Get MySQL tables
        try:
            mysql_details = get_mysql_tables()
            logger.info(f"MySQL tables found: {list(mysql_details.keys())}")
        except Exception as e:
            logger.error(f"MySQL error: {str(e)}")
            mysql_details = {}

        # Store in user context
        context.user_data.clear()  # Clear previous data
        context.user_data['mongodb_collections'] = list(mongodb_details.keys())
        context.user_data['mysql_tables'] = list(mysql_details.keys())
        
        keyboard = [
            ["Query MongoDB", "Query MySQL"],
            ["Back to Menu"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        response = "📊 **Available Data Sources**:\n\n"
        
        # MongoDB collections section
        response += "**MongoDB Collections:**\n"
        if mongodb_details:
            for collection, fields in mongodb_details.items():
                response += f"\n🔹 {collection}:\n"
                if fields:
                    response += format_nested_fields(fields)
                else:
                    response += "  (Empty collection)\n"
        else:
            response += "(No collections available)\n"
        
        # MySQL tables section
        response += "\n**MySQL Tables:**\n"
        if mysql_details:
            for table, columns in mysql_details.items():
                response += f"\n🔹 {table}:\n"
                for column_name, data_type in columns:
                    response += f"  • {column_name}: {data_type}\n"
        else:
            response += "(No tables available)\n"
        
        response += "\n🔍 **To query data, first select the database type you want to query from the keyboard below.**"
        
        await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="Markdown")
        return QUERY_DATA
        
    except Exception as e:
        logger.error(f"Error in show_data_overview: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "An error occurred while retrieving data sources. Please try again.",
            reply_markup=ReplyKeyboardMarkup([["Back to Menu"]], resize_keyboard=True)
        )
        return CHOOSING
    

async def handle_database_selection(update: Update, context: CallbackContext) -> int:
    """Handle database type selection for querying."""
    try:
        selection = update.message.text.strip()
        logger.info(f"Database selection: {selection}")

        if selection == "Back to Menu":
            return await start(update, context)

        # Verify context data exists
        if 'mongodb_collections' not in context.user_data or 'mysql_tables' not in context.user_data:
            logger.error("Missing database info in context")
            # Try to refresh the data
            mongodb_details = get_mongodb_collections()
            mysql_details = get_mysql_tables()
            context.user_data['mongodb_collections'] = list(mongodb_details.keys())
            context.user_data['mysql_tables'] = list(mysql_details.keys())

        if selection == "Query MongoDB":
            collections = context.user_data.get('mongodb_collections', [])
            if not collections:
                await update.message.reply_text("No MongoDB collections available.")
                return QUERY_DATA

            response = (
                "📚 Available MongoDB Collections:\n\n" +
                "\n".join([f"• {coll}" for coll in collections]) +
                "\n\n🔍 Query Examples:\n" +
                "• Type collection name to see all documents\n" +
                "• `find in [collection] where field > value`\n" +
                "• `count documents in [collection] grouped by field`"
            )

        elif selection == "Query MySQL":
            tables = context.user_data.get('mysql_tables', [])
            if not tables:
                await update.message.reply_text("No MySQL tables available.")
                return QUERY_DATA

            response = (
                "📚 Available MySQL Tables:\n\n" +
                "\n".join([f"• {table}" for table in tables]) +
                "\n\n🔍 Query Examples:\n" +
                "• Type table name to see first 10 rows\n" +
                "• `select * from [table] where [condition]`\n" +
                "• `find top 5 rows in [table] ordered by [column]`"
            )

        context.user_data['selected_db'] = selection
        keyboard = [["Back to Menu"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=False, resize_keyboard=True)
        
        try:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to send message with Markdown: {str(e)}")
            # Try sending without Markdown formatting
            await update.message.reply_text(response, reply_markup=reply_markup)
            
        return QUERY_DATA

    except Exception as e:
        logger.error(f"Database selection error: {str(e)}", exc_info=True)
        await update.message.reply_text(
            "An error occurred. Please select 'Query Data' from the main menu.",
            reply_markup=ReplyKeyboardMarkup([["Back to Menu"]], resize_keyboard=True)
        )
        return CHOOSING
    
async def handle_query(update: Update, context: CallbackContext) -> int:
    """Handle natural language queries."""
    try:
        user_input = update.message.text.strip()
        logger.info(f"Processing query: {user_input}")
        
        if user_input.lower() == 'back to menu':
            return await start(update, context)
        
        selected_db = context.user_data.get('selected_db')
        if not selected_db:
            await update.message.reply_text(
                "Please select a database type first.",
                reply_markup=ReplyKeyboardMarkup([["Query MongoDB", "Query MySQL"], ["Back to Menu"]], resize_keyboard=True)
            )
            return QUERY_DATA

        # Handle MongoDB Query
        if selected_db == "Query MongoDB":
            collections = context.user_data.get('mongodb_collections', set())
            
            # Parse query patterns
            query_patterns = {
                'find_all': r'^find\s+all\s+(\w+)$',
                'show_all_field': r'^show\s+all\s+(\w+)\s+(?:in|from)\s+(\w+)$',
                'collection_name': r'^(\w+)$'
            }
            
            # Try to match each pattern
            mongo_query = None
            target = None
            field = None
            
            for pattern_name, pattern in query_patterns.items():
                match = re.match(pattern, user_input.lower())
                if match:
                    if pattern_name == 'find_all':
                        target = match.group(1)
                        mongo_query = {'find': {}}
                    elif pattern_name == 'show_all_field':
                        field = match.group(1)
                        target = match.group(2)
                        mongo_query = {
                            'find': {},
                            'projection': {field: 1, '_id': 0}
                        }
                    elif pattern_name == 'collection_name':
                        target = match.group(1)
                        mongo_query = {'find': {}}
                    break
            
            # Validate collection name
            if not target or target not in collections:
                await update.message.reply_text(
                    f"Please specify a valid collection name.\n\n"
                    f"Available collections:\n" +
                    "\n".join([f"• {coll}" for coll in collections])
                )
                return QUERY_DATA
            
            # Execute MongoDB query
            try:
                # Send query
                await update.message.reply_text(
                    f"MongoDB Query:\n```json\n{json.dumps(mongo_query, indent=2)}\n```",
                    parse_mode="Markdown"
                )
                
                # Execute query
                results = query_executor.execute_mongodb_query(target, mongo_query)
                results_list = list(results)[:10]
                
                if results_list:
                    # Send results in smaller chunks
                    for i, doc in enumerate(results_list, 1):
                        msg = json.dumps(doc, indent=2, default=str).replace('{', '\\{').replace('}', '\\}')
                        await update.message.reply_text(
                            f"Document {i}:\n```\n{msg}\n```",
                            parse_mode="Markdown"
                        )
                    
                    if len(results_list) == 10:
                        await update.message.reply_text("(Showing first 10 documents)")
                else:
                    await update.message.reply_text("No documents found.")
            
            except Exception as e:
                logger.error(f"MongoDB execution error: {str(e)}")
                await update.message.reply_text(f"Error executing query: {str(e)}")
            
            return QUERY_DATA

        # Handle MySQL Query
        else:  # MySQL
            tables = context.user_data.get('mysql_tables', set())
            
            # Parse query patterns
            query_patterns = {
                'find_all': r'^find\s+all\s+(\w+)$',
                'show_all_field': r'^show\s+all\s+(\w+)\s+(?:in|from)\s+(\w+)$',
                'table_name': r'^(\w+)$'
            }
            
            # Try to match each pattern
            sql_query = None
            target = None
            
            for pattern_name, pattern in query_patterns.items():
                match = re.match(pattern, user_input.lower())
                if match:
                    if pattern_name == 'find_all':
                        target = match.group(1)
                        sql_query = f"SELECT * FROM {target} LIMIT 10"
                    elif pattern_name == 'show_all_field':
                        field = match.group(1)
                        target = match.group(2)
                        sql_query = f"SELECT {field} FROM {target} LIMIT 10"
                    elif pattern_name == 'table_name':
                        target = match.group(1)
                        sql_query = f"SELECT * FROM {target} LIMIT 10"
                    break
            
            # Validate table name
            if not target or target not in tables:
                await update.message.reply_text(
                    f"Please specify a valid table name.\n\n"
                    f"Available tables:\n" +
                    "\n".join([f"• {table}" for table in tables])
                )
                return QUERY_DATA
            
            # Execute MySQL query
            try:
                # Send query
                await update.message.reply_text(
                    f"SQL Query:\n```sql\n{sql_query}\n```",
                    parse_mode="Markdown"
                )
                
                # Execute query
                results = query_executor.execute_sql_query(sql_query)
                
                if not results:
                    await update.message.reply_text("No results found.")
                    return QUERY_DATA
                
                # Format in vertical chunks of 5 columns
                columns = list(results[0].keys())
                
                # Group columns into sets of 5
                for i in range(0, len(columns), 5):
                    col_group = columns[i:i+5]
                    
                    # Create mini table for this column group
                    header = " | ".join(f"{col:15}" for col in col_group)
                    separator = "-" * len(header)
                    
                    rows = []
                    for row in results:
                        row_values = [f"{str(row[col]):15}" for col in col_group]
                        rows.append(" | ".join(row_values))
                    
                    group_table = f"Columns {i+1}-{i+len(col_group)}:\n"
                    group_table += header + "\n" + separator + "\n"
                    group_table += "\n".join(rows)
                    
                    # Send each column group separately
                    await update.message.reply_text(
                        f"```\n{group_table}\n```",
                        parse_mode="Markdown"
                    )
                
                await update.message.reply_text(f"\nTotal rows: {len(results)}")
                
            except Exception as e:
                logger.error(f"MySQL execution error: {str(e)}")
                await update.message.reply_text(f"Error executing query: {str(e)}")
            
            return QUERY_DATA
        
    except Exception as e:
        logger.error(f"Query error: {str(e)}", exc_info=True)
        await update.message.reply_text(f"Error processing query: {str(e)}")
        return QUERY_DATA
    
async def cancel(update: Update, context: CallbackContext) -> int:
    """Cancel conversation."""
    await update.message.reply_text(
        "Goodbye! Type /start to begin again.",
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END

async def show_sample_queries(update: Update, context: CallbackContext) -> int:
    """Show sample queries and their outputs with natural language descriptions."""
    sample_queries = [
        # MySQL Examples
        {
            "type": "MySQL",
            "description": "Retrieve all movies released after the year 2000.",
            "query": "SELECT movie, year_released FROM pixar_movies WHERE year_released > 2000;",
            "output": "Movie           | Year Released\nFinding Nemo   | 2003\nThe Incredibles | 2004"
        },
        {
            "type": "MySQL",
            "description": "Find the top 5 movies with the highest Rotten Tomatoes ratings.",
            "query": "SELECT movie, rotten_tomatoes_rating FROM pixar_movies ORDER BY rotten_tomatoes_rating DESC LIMIT 5;",
            "output": "Movie           | Rotten Tomatoes Rating\nToy Story       | 100%\nFinding Nemo    | 99%"
        },
        {
            "type": "MySQL",
            "description": "Calculate the average IMDb rating of all Pixar movies.",
            "query": "SELECT AVG(imdb_rating) AS average_rating FROM pixar_movies;",
            "output": "Average IMDb Rating\n8.1"
        },
        {
            "type": "MySQL",
            "description": "List all movies directed by 'John Lasseter'.",
            "query": "SELECT movie FROM pixar_movies WHERE director = 'John Lasseter';",
            "output": "Movie\nToy Story\nA Bug's Life"
        },
        # MongoDB Examples
        {
            "type": "MongoDB",
            "description": "Retrieve all orders where the price is greater than 100.",
            "query": "db.orders.find({ price: { $gt: 100 } });",
            "output": "[\n  { OrderID: 12345, Price: 120 },\n  { OrderID: 12346, Price: 150 }\n]"
        },
        {
            "type": "MongoDB",
            "description": "Calculate the total spending for each customer.",
            "query": "db.customers.aggregate([ { $group: { _id: '$customer_id', totalSpent: { $sum: '$total_spent' } } } ]);",
            "output": "[\n  { _id: 'C001', totalSpent: 5000 },\n  { _id: 'C002', totalSpent: 4500 }\n]"
        },
        {
            "type": "MongoDB",
            "description": "Find the average price of all products.",
            "query": "db.products.aggregate([ { $group: { _id: null, avgPrice: { $avg: '$price' } } } ]);",
            "output": "[\n  { _id: null, avgPrice: 75.5 }\n]"
        },
        {
            "type": "MongoDB",
            "description": "Count the number of orders placed by each customer.",
            "query": "db.orders.aggregate([ { $group: { _id: '$customer_id', orderCount: { $sum: 1 } } } ]);",
            "output": "[\n  { _id: 'C001', orderCount: 5 },\n  { _id: 'C002', orderCount: 3 }\n]"
        },
    ]

    response = "💡 **Sample Queries and Outputs**:\n\n"
    keyboard = [["Back to Menu"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text(response, reply_markup=reply_markup, parse_mode="Markdown")

    for i, example in enumerate(sample_queries, 1):
        # Send the description
        await update.message.reply_text(
            f"**{i}. [{example['type']}]**\n\n"
            f"**Description:** `{example['description']}`\n\n",
            parse_mode="Markdown"
        )
        # Send the query
        await update.message.reply_text(
            f"**Query:**\n```{example['query']}```",
            parse_mode="Markdown"
        )
        # Send the output
        await update.message.reply_text(
            f"**Output:**\n```\n{example['output']}\n```",
            parse_mode="Markdown"
        )

    return CHOOSING

def main() -> None:
    """Run the bot."""
    # Create the Application and pass it your bot's token
    application = Application.builder().token(os.getenv('BOT_TOKEN')).build()
    
    # Set up conversation handler with explicit handler for database selection
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            CHOOSING: [
                MessageHandler(filters.Regex('^Upload Data$'), handle_upload_data),
                MessageHandler(filters.Regex('^Query Data$'), show_data_overview),
                MessageHandler(filters.Regex('^Sample Queries$'), show_sample_queries),
                MessageHandler(filters.Regex('^Help$'), help_command),
                MessageHandler(filters.Regex('^Exit$'), cancel),
                MessageHandler(filters.Regex('^Back to Menu$'), start)
            ],
            UPLOAD_FILE: [
                MessageHandler(filters.Regex('^(CSV|JSON)$'), handle_file_type_selection),
                MessageHandler(filters.Regex('^Back to Menu$'), start),
                MessageHandler(filters.Document.ALL, handle_file_upload),
            ],
            QUERY_DATA: [
                # Explicitly handle both MongoDB and MySQL selection
                MessageHandler(filters.Regex('^Query MongoDB$'), handle_database_selection),
                MessageHandler(filters.Regex('^Query MySQL$'), handle_database_selection),
                MessageHandler(filters.Regex('^Back to Menu$'), start),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query),
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('help', help_command),
        ],
    )
    
    application.add_handler(conv_handler)
    application.run_polling()

if __name__ == '__main__':
    test_database_connections()
    main()