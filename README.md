# TraderJohn-Database

This project is a Telegram bot that allows users to upload data files (CSV or JSON), convert them to MySQL or MongoDB databases, and query the databases. The bot provides sample queries and allows users to explore the databases.

## Features

- Upload CSV or JSON files and convert them to MySQL or MongoDB databases.
- Explore available databases and their tables/collections.
- Generate sample queries for the selected dataset.
- Execute user-defined queries on the databases.

## Prerequisites

- Python 3.8+
- Telegram Bot Token
- MySQL Server
- MongoDB Server

## Installation

1. **Clone the repository:**
```sh
git clone https://github.com/johnson70630/TraderJohn-Database.git
```

2. **Install dependencies:**
```sh
pip install poetry
make install
```

3. **Set up environment variables:**

Change `.env.template` filename to `.env`, modify the file in the project root directory, and add the following variables:
```sh
BOT_TOKEN=your_telegram_bot_token
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_HOST=your_mysql_host
DB_NAME=your_mysql_database
MONGODB_URL=your_mongodb_url
MONGODB_NAME=your_mongodb_name
```
(create a bot from botFather in telegram and get the bot token)

## Usage

1. **Run the bot**
```sh
make run
```

2. **Interact with the bot**

- Start the bot by sending the `/start` command.
- Use the provided options to upload data, explore databases, and query data.

### Project Structure

```
TraderJohn-Database/
├── [main.py]                   # Main bot script
├── project_proposal            # The proposal of this project
├── sample_data                 # Sample data uploaded to databases
├── utils/                      # Code implemented in main.py
│   ├── __init__.py                 
│   ├── query_generator.py      
│   ├── data_processig.py  
│   ├── ...
├── .env                        # Environment variables
├── [pyproject.toml]            # Poetry configuration file
├── [README.md]                 # Project documentation
```
### Bot Commands (Quick Reply Botton)
- `/start`: Start the bot and show the main menu.
- `Upload Data`: Upload CSV or JSON files to convert to MySQL or MongoDB databases.
- `Query Data`: Explore available databases and execute queries.
- `Sample Queries`: Show sample queries for the selected dataset.
- `Back to menu`: Return to the main menu.
- `Exit`: Exit the bot.

**Sample Queries**
```
SQL:

1. cars
2. youtube_channel_real_performance_analytics
3. show enginetype from cars group by enginetype having average price > 10000
4. find price, citympg and peakrpm in cars
5. show cars which price larger than 20000
6. count total enginetype in cars grouped by enginetype

MongoDB:

1. find in iris_data which petalWidth larger than 1.5
2. iris_data
3. crime_rate
4. count documents in iris_data grouped by species
5. iris_data, show species grouped by species having petalWidth larger than 0.5
6. iris_data, show species grouped by species having petalWidth larger than 1.5
```


### License
This project is licensed under the MIT License. See the [LICENSE](/LICENSE) file for details.

```
This [README.md] file provides a comprehensive overview of the project, including features, prerequisites, installation instructions, usage guidelines, project structure, bot commands, contributing guidelines, and license information. Adjust the content as needed to fit your specific project details.
```