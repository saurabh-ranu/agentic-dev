#!/usr/bin/env python3
"""
Script to create a sample SQLite database with demo data for testing.
"""
import sqlite3
import os

def create_sample_database():
    db_path = "demo.db"
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create new database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create employees table
    cursor.execute('''
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT,
            salary REAL,
            hire_date TEXT,
            is_active BOOLEAN
        )
    ''')
    
    # Insert sample data
    sample_employees = [
        (1, 'John Doe', 30, 'Engineering', 75000.0, '2020-01-15', True),
        (2, 'Jane Smith', 28, 'Marketing', 65000.0, '2020-03-20', True),
        (3, 'Bob Johnson', 35, 'Engineering', 85000.0, '2019-11-10', True),
        (4, 'Alice Brown', 29, 'Sales', 60000.0, '2021-02-14', False),
        (5, 'Charlie Wilson', 32, 'Engineering', 80000.0, '2020-06-01', True),
        (6, 'Diana Davis', 27, 'Marketing', 62000.0, '2021-08-15', True),
        (7, 'Eve Miller', 31, 'Sales', 58000.0, '2020-12-03', True),
        (8, 'Frank Garcia', 33, 'Engineering', 78000.0, '2019-09-22', True),
        (9, 'Grace Lee', 26, 'HR', 55000.0, '2021-05-10', True),
        (10, 'Henry Taylor', 34, 'Sales', 72000.0, '2020-04-18', False)
    ]
    
    cursor.executemany('''
        INSERT INTO employees (id, name, age, department, salary, hire_date, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', sample_employees)
    
    # Create products table
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price REAL,
            stock_quantity INTEGER,
            created_date TEXT
        )
    ''')
    
    # Insert sample products
    sample_products = [
        (1, 'Laptop Pro', 'Electronics', 1299.99, 50, '2023-01-01'),
        (2, 'Wireless Mouse', 'Electronics', 29.99, 200, '2023-01-15'),
        (3, 'Office Chair', 'Furniture', 299.99, 30, '2023-02-01'),
        (4, 'Coffee Mug', 'Kitchen', 12.99, 150, '2023-02-15'),
        (5, 'Notebook Set', 'Office Supplies', 24.99, 100, '2023-03-01'),
        (6, 'Desk Lamp', 'Furniture', 89.99, 75, '2023-03-15'),
        (7, 'Bluetooth Headphones', 'Electronics', 199.99, 80, '2023-04-01'),
        (8, 'Water Bottle', 'Kitchen', 19.99, 120, '2023-04-15')
    ]
    
    cursor.executemany('''
        INSERT INTO products (id, name, category, price, stock_quantity, created_date)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', sample_products)
    
    # Create sales table
    cursor.execute('''
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            employee_id INTEGER,
            quantity INTEGER,
            sale_date TEXT,
            total_amount REAL,
            FOREIGN KEY (product_id) REFERENCES products (id),
            FOREIGN KEY (employee_id) REFERENCES employees (id)
        )
    ''')
    
    # Insert sample sales
    sample_sales = [
        (1, 1, 2, 1, '2023-05-01', 1299.99),
        (2, 2, 1, 3, '2023-05-02', 89.97),
        (3, 3, 4, 1, '2023-05-03', 299.99),
        (4, 4, 2, 5, '2023-05-04', 64.95),
        (5, 1, 6, 2, '2023-05-05', 2599.98),
        (6, 7, 1, 1, '2023-05-06', 199.99),
        (7, 5, 3, 4, '2023-05-07', 99.96),
        (8, 6, 4, 1, '2023-05-08', 89.99)
    ]
    
    cursor.executemany('''
        INSERT INTO sales (id, product_id, employee_id, quantity, sale_date, total_amount)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', sample_sales)
    
    # Commit and close
    conn.commit()
    conn.close()
    
    print(f"Sample database created successfully at {db_path}")
    print("Tables created: employees, products, sales")
    print("Sample data inserted for testing")

if __name__ == "__main__":
    create_sample_database()
