import random
import json

# Sample datasets
def get_sample_data():
    mysql_sample_data = """
    +----------+----------------+------------+-------------+------------+
    | OrderID  | CustomerName   | OrderDate  | TotalAmount | Status     |
    +----------+----------------+------------+-------------+------------+
    | 1        | John Doe       | 2023-11-01 | 100.50      | Completed  |
    | 2        | Jane Smith     | 2023-11-02 | 200.75      | Pending    |
    | 3        | Emily Davis    | 2023-11-03 | 150.25      | Completed  |
    | 4        | Chris Brown    | 2023-11-04 | 300.00      | Cancelled  |
    +----------+----------------+------------+-------------+------------+

    +------------+----------------+-----------+
    | CustomerID | CustomerName   | Country   |
    +------------+----------------+-----------+
    | 1          | John Doe       | USA       |
    | 2          | Jane Smith     | UK        |
    | 3          | Emily Davis    | Canada    |
    | 4          | Chris Brown    | Australia |
    +------------+----------------+-----------+
    """

    mongodb_sample_data = json.dumps({
        "Orders": [
            {"OrderID": 1, "CustomerName": "John Doe", "OrderDate": "2023-11-01", "TotalAmount": 100.50, "Status": "Completed"},
            {"OrderID": 2, "CustomerName": "Jane Smith", "OrderDate": "2023-11-02", "TotalAmount": 200.75, "Status": "Pending"},
            {"OrderID": 3, "CustomerName": "Emily Davis", "OrderDate": "2023-11-03", "TotalAmount": 150.25, "Status": "Completed"},
            {"OrderID": 4, "CustomerName": "Chris Brown", "OrderDate": "2023-11-04", "TotalAmount": 300.00, "Status": "Cancelled"}
        ],
        "Customers": [
            {"CustomerID": 1, "CustomerName": "John Doe", "Country": "USA"},
            {"CustomerID": 2, "CustomerName": "Jane Smith", "Country": "UK"},
            {"CustomerID": 3, "CustomerName": "Emily Davis", "Country": "Canada"},
            {"CustomerID": 4, "CustomerName": "Chris Brown", "Country": "Australia"}
        ]
    }, indent=4)

    return {
        "MySQL": mysql_sample_data,
        "MongoDB": mongodb_sample_data
    }

# Sample queries
def get_sample_queries(type=None):
    queries = [
        {
            "type": "Group By",
            "natural_language": "Group orders by their status and count the number of orders in each status.",
            "mysql": "SELECT Status, COUNT(*) AS OrderCount FROM Orders GROUP BY Status;",
            "mysql_result":"""
            +------------+------------+
            | Status     | OrderCount |
            +------------+------------+
            | Cancelled  | 1          |
            | Completed  | 2          |
            | Pending    | 1          |
            +------------+------------+
            """,
            "mongodb": """
            db.Orders.aggregate([
                { $group: { _id: "$Status", OrderCount: { $sum: 1 } } }
            ]);
            """,
            "mongodb_result": """
            [
                { "_id": "Cancelled", "OrderCount": 1 },
                { "_id": "Completed", "OrderCount": 2 },
                { "_id": "Pending", "OrderCount": 1 }
            ]
            """
        },
        {
            "type": "Join",
            "natural_language": "Join orders with customers to get the customer's country for each order.",
            "mysql": """
            SELECT o.OrderID, o.CustomerName, c.Country, o.TotalAmount
            FROM Orders o
            JOIN Customers c ON o.CustomerName = c.CustomerName;
            """,
            "mysql_result": """
            +----------+----------------+-----------+-------------+
            | OrderID  | CustomerName   | Country   | TotalAmount |
            +----------+----------------+-----------+-------------+
            | 1        | John Doe       | USA       | 100.50      |
            | 2        | Jane Smith     | UK        | 200.75      |
            | 3        | Emily Davis    | Canada    | 150.25      |
            | 4        | Chris Brown    | Australia | 300.00      |
            +----------+----------------+-----------+-------------+
            """,
            "mongodb": """
            db.Orders.aggregate([
                {
                    $lookup: {
                        from: "Customers",
                        localField: "CustomerName",
                        foreignField: "CustomerName",
                        as: "CustomerDetails"
                    }
                },
                { $unwind: "$CustomerDetails" },
                {
                    $project: {
                        OrderID: 1,
                        CustomerName: 1,
                        Country: "$CustomerDetails.Country",
                        TotalAmount: 1
                    }
                }
            ]);
            """,
            "mongodb_result": """
            [
                { "OrderID": 1, "CustomerName": "John Doe", "Country": "USA", "TotalAmount": 100.50 },
                { "OrderID": 2, "CustomerName": "Jane Smith", "Country": "UK", "TotalAmount": 200.75 },
                { "OrderID": 3, "CustomerName": "Emily Davis", "Country": "Canada", "TotalAmount": 150.25 },
                { "OrderID": 4, "CustomerName": "Chris Brown", "Country": "Australia", "TotalAmount": 300.00 }
            ]
            """
        },
        {
            "type": "Sum",
            "natural_language": "Calculate the total sales (sum of TotalAmount) for completed orders.",
            "mysql": "SELECT SUM(TotalAmount) AS TotalSales FROM Orders WHERE Status = 'Completed';",
            "mysql_result": """
            +-------------+
            | TotalSales  |
            +-------------+
            | 250.75      |
            +-------------+
            """,
            "mongodb": """
            db.Orders.aggregate([
                { $match: { Status: "Completed" } },
                { $group: { _id: null, TotalSales: { $sum: "$TotalAmount" } } }
            ]);
            """,
            "mongodb_result": """
            [
                { "_id": null, "TotalSales": 250.75 }
            ]
            """
        },
        {
            "type": "Order By",
            "natural_language": "Retrieve all orders and sort them by total amount in descending order.",
            "mysql": "SELECT * FROM Orders ORDER BY TotalAmount DESC;",
            "mysql_result": """
            +----------+----------------+------------+-------------+------------+
            | OrderID  | CustomerName   | OrderDate  | TotalAmount | Status     |
            +----------+----------------+------------+-------------+------------+
            | 4        | Chris Brown    | 2023-11-04 | 300.00      | Cancelled  |
            | 2        | Jane Smith     | 2023-11-02 | 200.75      | Pending    |
            | 3        | Emily Davis    | 2023-11-03 | 150.25      | Completed  |
            | 1        | John Doe       | 2023-11-01 | 100.50      | Completed  |
            +----------+----------------+------------+-------------+------------+
            """,
            "mongodb": """
            db.Orders.find().sort({ TotalAmount: -1 });
            """,
            "mongodb_result": """
            [
                { "OrderID": 4, "CustomerName": "Chris Brown", "OrderDate": "2023-11-04", "TotalAmount": 300.00, "Status": "Cancelled" },
                { "OrderID": 2, "CustomerName": "Jane Smith", "OrderDate": "2023-11-02", "TotalAmount": 200.75, "Status": "Pending" },
                { "OrderID": 3, "CustomerName": "Emily Davis", "OrderDate": "2023-11-03", "TotalAmount": 150.25, "Status": "Completed" },
                { "OrderID": 1, "CustomerName": "John Doe", "OrderDate": "2023-11-01", "TotalAmount": 100.50, "Status": "Completed" }
            ]
            """
        },
        {
            "type": "Where",
            "natural_language": "Find orders where the total amount is greater than 150.",
            "mysql": "SELECT * FROM Orders WHERE TotalAmount > 150;",
            "mysql_result": """
            +----------+----------------+------------+-------------+------------+
            | OrderID  | CustomerName   | OrderDate  | TotalAmount | Status     |
            +----------+----------------+------------+-------------+------------+
            | 2        | Jane Smith     | 2023-11-02 | 200.75      | Pending    |
            | 4        | Chris Brown    | 2023-11-04 | 300.00      | Cancelled  |
            +----------+----------------+------------+-------------+------------+
            """,
            "mongodb": """
            db.Orders.find({ TotalAmount: { $gt: 150 } });
            """,
            "mongodb_result": """
            [
                { "OrderID": 2, "CustomerName": "Jane Smith", "OrderDate": "2023-11-02", "TotalAmount": 200.75, "Status": "Pending" },
                { "OrderID": 4, "CustomerName": "Chris Brown", "OrderDate": "2023-11-04", "TotalAmount": 300.00, "Status": "Cancelled" }
            ]
            """
        }
    ]
    return random.choice(queries) if type is None else next((q for q in queries if q["type"] == type), None)