import sqlite3
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class ValidationRules:
    @staticmethod
    def validate_user(user: Dict[str, Any]) -> Optional[str]:
        if not isinstance(user.get('id'), int) or user['id'] <= 0:
            return f"Invalid user ID: {user.get("id")}. Must be a positive integer."
        
        name = user.get('name', '').strip()
        if not name or not re.match(r'^[A-Za-z\s]+$', name):
            return f"Invalid name: {name}. Must contain only letters and spaces."
        
        email = user.get('email', '').strip()
        email_regex = r'^[a-zA-Z0-9. %+-]+@[a-zA-Z-0-9.-]+\.[a-zA-Z]{2,}$'
        if not email or not re.match(email_regex, email):
            return f'Invalid email: {email}. Must be a valid email address.'
        
        return None
    
    @staticmethod
    def validate_product(product: Dict[str, Any]) -> Optional[str]:
        if not isinstance(product.get('id'), int) or product['id'] <= 0:
            return f"Invalid product ID: {product.get('id')}. Must be a positive integer"
        
        name = product.get('name', '').strip()
        if not name or not re.match(r'^[A-Za-z0-9\s]+$', name):
            return f"Invalid product name: {name}. Must contain only letters and sapaces."
        
        price = product.get('price')
        try:
            price_float = float(price)
            if price_float < 0:
                return f"Invalid price: {price}. Price must be non-negative."
        except (TypeError, ValueError):
            return f"Invalid price: {price}. Must be a number."
        
        return None
    
    @staticmethod
    def validate_order(order: Dict[str, Any]) -> Optional[str]:
        if not isinstance(order.get('id'), int) or order['id'] <= 0:
            return f"Invalid order ID: {order.get('id')}. Must be a positive integer"
        
        user_id = order.get('user_id')
        if not isinstance(user_id, int) or user_id <= 0:
            return f"Invalid user ID: {user_id}. Must be a positive integer"
        
        product_id = order.get('user_id')
        if not isinstance(product_id, int) or product_id <= 0:
            return f"Invalid product ID: {product_id}. Must be a positive integer"
        
        quantity = order.get('quantity')
        if not isinstance(quantity, int):
            return f"Invalid quanitty: {quantity}. Must be an integer."
        
        return None
    
class DatabaseConfig:
    users_db = 'users.db'
    products_db = 'products.db'
    orders_db = 'orders.db'
    
class DatabaseManager:
    _lock = threading.Lock()
    
    @staticmethod
    def _create_tables():
        with sqlite3.connect(DatabaseConfig.users_db) as users_conn:
            users_conn.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT, email TEXT)''')
            
        with sqlite3.connect(DatabaseConfig.products_db) as products_conn:
            products_conn.execute('''CREATE TABLE IF NOT EXISTS products (id INTEGER, name TEXT, price REAL)''')
            
        with sqlite3.connect(DatabaseConfig.orders_db) as orders_conn:
            orders_conn.execute('''CREATE TABLE IF NOT EXISTS orders (id INTEGER, user_id INTEGER, product_id INTEGER, quantity INTEGER)''')
            
    @classmethod
    def _insert_record(cls, db_path: str, table: str, record: Dict[str, Any],
                       validation_func: callable) -> Dict[str, Any]:
        
        validation_error = validation_func(record)
        if validation_error:
            return {
                'record': record,
                'status': 'validation_error',
                'error': validation_error
            }
            
        try:
            with cls._lock, sqlite3.connect(db_path) as conn:
                placeholders = ', '.join(['?' for _ in record])
                columns = ', '.join(record.keys())
                query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
                
                cursor = conn.cursor()
                cursor.execute(query, list(record.values()))
                conn.commit()
                
                return {
                    'record': record,
                    'status': 'success',
                    'rowid': cursor.lastrowid
                }
        except sqlite3.Error as e:
            return {
                'record': record,
                'status': 'insertion_error',
                'error': str(e)
            }
    
    @classmethod
    def simulate_insertions(cls) -> Dict[str, List[Dict]]:
        cls._create_tables()
        
        users_data = [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'},
            {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com'},
            {'id': 4, 'name': 'David', 'email': 'david@example.com'},
            {'id': 5, 'name': 'Eve', 'email': 'eve@example.com'},
            {'id': 6, 'name': 'Frank', 'email': 'frank@example.com'},
            {'id': 7, 'name': 'Grace', 'email': 'grace@example.com'},
            {'id': 8, 'name': 'Alice', 'email': 'alice@example.com'},
            {'id': 9, 'name': 'Henry', 'email': 'henry@example.com'},
            {'id': 10, 'name': '', 'email': 'jane@example.com'}
        ]
        
        products_data = [
            {'id': 1, 'name': 'Laptop', 'price': 1000.00},
            {'id': 2, 'name': 'Smartphone', 'price': 700.00},
            {'id': 3, 'name': 'Headphones', 'price': 150.00},
            {'id': 4, 'name': 'Monitor', 'price': 300.00},
            {'id': 5, 'name': 'Keyboard', 'price': 50.00},
            {'id': 6, 'name': 'Mouse', 'price': 30.00},
            {'id': 7, 'name': 'Laptop', 'price': 1000.00},
            {'id': 8, 'name': 'Smartwatch', 'price': 250.00},
            {'id': 9, 'name': 'Gaming Chair', 'price': 500.00},
            {'id': 10, 'name': 'Earbuds', 'price': -50.00}
        ]
        
        orders_data = [
            {'id': 1, 'user_id': 1, 'product_id': 1, 'quantity': 2},
            {'id': 2, 'user_id': 2, 'product_id': 2, 'quantity': 1},
            {'id': 3, 'user_id': 3, 'product_id': 3, 'quantity': 5},
            {'id': 4, 'user_id': 4, 'product_id': 4, 'quantity': 1},
            {'id': 5, 'user_id': 5, 'product_id': 5, 'quantity': 3},
            {'id': 6, 'user_id': 6, 'product_id': 6, 'quantity': 4},
            {'id': 7, 'user_id': 7, 'product_id': 7, 'quantity': 2},
            {'id': 8, 'user_id': 8, 'product_id': 8, 'quantity': 0},
            {'id': 9, 'user_id': 9, 'product_id': 1, 'quantity': -1},
            {'id': 10, 'user_id': 10, 'product_id': 11, 'quantity': 2}
        ]
        
        results = {
            'users': [],
            'products': [],
            'orders': []
        }
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            users_futures = [
                executor.submit(
                    cls._insert_record,
                    DatabaseConfig.users_db,
                    'users',
                    record,
                    ValidationRules.validate_user
                )
                for record in users_data
            ]
            results['users'] = [future.result() for future in as_completed(users_futures)]
            
            products_futures = [
                executor.submit(
                    cls._insert_record,
                    DatabaseConfig.products_db,
                    'products',
                    record,
                    ValidationRules.validate_product
                )
                for record in products_data
            ]
            results['products'] = [future.result() for future in as_completed(products_futures)]
            
            orders_futures = [
                executor.submit(
                    cls._insert_record,
                    DatabaseConfig.orders_db,
                    'orders',
                    record,
                    ValidationRules.validate_order
                )
                for record in orders_data
            ]
            results['orders'] = [future.result() for future in as_completed(orders_futures)]
            
        return results
    
def main() :
    try:
        results = DatabaseManager.simulate_insertions()
        
        print("Users Insertion Results:")
        for result in results['users']:
            print(f"Record: {result.get('record', 'N/A')}, "
                  f"status: {result.get('status', 'N/A')}, "
                  f"Details: {result.get('error', 'Success')}")
            
        print("\nProducts Insertion Results:")
        for result in results['products']:
            print(f"Record: {result.get('record', 'N/A')}, "
                  f"status: {result.get('status', 'N/A')}, "
                  f"Details: {result.get('error', 'Success')}")
            
        print("\nOrders Insertion Results:")
        for result in results['orders']:
            print(f"Record: {result.get('record', 'N/A')}, "
                  f"status: {result.get('status', 'N/A')}, "
                  f"Details: {result.get('error', 'Success')}")
            
    except Exception as e:
        print(f"An error occured: {e}")
        
if __name__ == '__main__':
    main()
        
        
            
    
            
       
        