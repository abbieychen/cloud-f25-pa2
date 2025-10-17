from flask import Flask, render_template, jsonify, request
import sqlite3
import json
from datetime import datetime, timedelta
import logging
import os

# Configuration from environment variables with defaults
BROKER_LIST = os.getenv('BROKER_LIST', 'localhost:9092')
TOPIC = os.getenv('TOPIC', 'weather')
RATE = float(os.getenv('RATE', '1.0'))
TOPIC_PREFIX = os.getenv('TOPIC_PREFIX', 'sensor')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

app = Flask(__name__)
app.config['DATABASE'] = '/data/sensor_data.db'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize database if needed"""
    try:
        conn = get_db_connection()
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sensor_data'")
        table_exists = cursor.fetchone() is not None
        conn.close()
        
        if not table_exists:
            logger.warning("Database table not found. Please ensure the subscriber is running.")
        else:
            logger.info("Database initialized successfully")
            
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/sensor-data')
def get_sensor_data():
    """API endpoint to get sensor data"""
    try:
        topic_filter = request.args.get('topic', 'all')
        time_range = request.args.get('time_range', '1h')  # 1h, 6h, 24h, 7d
        limit = int(request.args.get('limit', 1000))
        
        # Calculate time filter
        time_filters = {
            '1h': timedelta(hours=1),
            '6h': timedelta(hours=6),
            '24h': timedelta(hours=24),
            '7d': timedelta(days=7)
        }
        
        time_delta = time_filters.get(time_range, timedelta(hours=1))
        cutoff_time = (datetime.now() - time_delta).isoformat()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if topic_filter == 'all':
            cursor.execute('''
                SELECT topic, sensor_id, data_json, timestamp, received_at
                FROM sensor_data 
                WHERE timestamp > ?
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (cutoff_time, limit))
        else:
            cursor.execute('''
                SELECT topic, sensor_id, data_json, timestamp, received_at
                FROM sensor_data 
                WHERE topic = ? AND timestamp > ?
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (topic_filter, cutoff_time, limit))
        
        data = cursor.fetchall()
        conn.close()
        
        # Process data
        sensor_data = []
        for row in data:
            try:
                data_json = json.loads(row['data_json'])
                sensor_data.append({
                    'topic': row['topic'],
                    'sensor_id': row['sensor_id'],
                    'data': data_json,
                    'timestamp': row['timestamp'],
                    'received_at': row['received_at']
                })
            except json.JSONDecodeError:
                continue
        
        return jsonify({
            'success': True,
            'data': sensor_data,
            'count': len(sensor_data)
        })
        
    except Exception as e:
        logger.error(f"Error fetching sensor data: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/topics')
def get_topics():
    """API endpoint to get available topics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT topic FROM sensor_data ORDER BY topic
        ''')
        
        topics = [row['topic'] for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({
            'success': True,
            'topics': topics
        })
        
    except Exception as e:
        logger.error(f"Error fetching topics: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats')
def get_stats():
    """API endpoint to get statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total messages
        cursor.execute('SELECT COUNT(*) as total FROM sensor_data')
        total_messages = cursor.fetchone()['total']
        
        # Messages by topic
        cursor.execute('''
            SELECT topic, COUNT(*) as count 
            FROM sensor_data 
            GROUP BY topic 
            ORDER BY count DESC
        ''')
        topics_count = {row['topic']: row['count'] for row in cursor.fetchall()}
        
        # Recent activity
        cursor.execute('''
            SELECT COUNT(*) as recent_count 
            FROM sensor_data 
            WHERE received_at > datetime('now', '-1 hour')
        ''')
        recent_messages = cursor.fetchone()['recent_count']
        
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_messages': total_messages,
                'recent_messages': recent_messages,
                'topics_count': topics_count
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    # Initialize database on startup
    init_database()
    
    app.run(host='0.0.0.0', port=5000, debug=False)