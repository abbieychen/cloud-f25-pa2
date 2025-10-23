from flask import Flask, render_template, jsonify
import sqlite3
import json
import os
from datetime import datetime

app = Flask(__name__)

def get_db_connection():
    """Get database connection"""
    db_path = os.getenv('DB_PATH', '/data/sensor_data.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/data')
def get_all_data():
    """Get all sensor data from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, topic, sensor_id, timestamp, data_json, created_at 
            FROM sensor_data 
            ORDER BY created_at DESC 
            LIMIT 1000
        ''')
        
        rows = cursor.fetchall()
        data = []
        
        for row in rows:
            sensor_data = {
                'id': row['id'],
                'topic': row['topic'],
                'sensor_id': row['sensor_id'],
                'timestamp': row['timestamp'],
                'created_at': row['created_at'],
                'data': json.loads(row['data_json'])
            }
            data.append(sensor_data)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'data': data,
            'count': len(data)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/data/<topic>')
def get_data_by_topic(topic):
    """Get sensor data for a specific topic"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, topic, sensor_id, timestamp, data_json, created_at 
            FROM sensor_data 
            WHERE topic = ?
            ORDER BY created_at DESC 
            LIMIT 500
        ''', (topic,))
        
        rows = cursor.fetchall()
        data = []
        
        for row in rows:
            sensor_data = {
                'id': row['id'],
                'topic': row['topic'],
                'sensor_id': row['sensor_id'],
                'timestamp': row['timestamp'],
                'created_at': row['created_at'],
                'data': json.loads(row['data_json'])
            }
            data.append(sensor_data)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'topic': topic,
            'data': data,
            'count': len(data)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/topics')
def get_topics():
    """Get list of all available topics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT topic FROM sensor_data')
        topics = [row['topic'] for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'topics': topics
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats')
def get_stats():
    """Get statistics about the sensor data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total records count
        cursor.execute('SELECT COUNT(*) as total FROM sensor_data')
        total_count = cursor.fetchone()['total']
        
        # Get count by topic
        cursor.execute('SELECT topic, COUNT(*) as count FROM sensor_data GROUP BY topic')
        topic_counts = {row['topic']: row['count'] for row in cursor.fetchall()}
        
        # Get latest timestamp
        cursor.execute('SELECT MAX(created_at) as latest FROM sensor_data')
        latest = cursor.fetchone()['latest']
        
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_records': total_count,
                'records_by_topic': topic_counts,
                'latest_update': latest
            }
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)