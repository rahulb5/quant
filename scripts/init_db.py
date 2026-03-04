"""
scripts/init_db.py

Creates the database file and applies all migrations.
Run with: python scripts/init_db.py
"""

from src.db.client import db

db.open()
db.close()
