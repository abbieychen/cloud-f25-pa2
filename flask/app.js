let sensorChart = null;

async function loadData() {
    try {
        // Load stats
        const statsResponse = await fetch('/api/stats');
        const statsData = await statsResponse.json();
        
        if (statsData.success) {
            updateStats(statsData.stats);
        }

        // Load topics
        const topicsResponse = await fetch('/api/topics');
        const topicsData = await topicsResponse.json();
        
        if (topicsData.success) {
            updateTopicSelect(topicsData.topics);
        }

        // Load sensor data
        const topic = document.getElementById('topicSelect').value;
        const timeRange = document.getElementById('timeRangeSelect').value;
        
        const dataResponse = await fetch(`/api/sensor-data?topic=${topic}&time_range=${timeRange}&limit=1000`);
        const sensorData = await dataResponse.json();
        
        if (sensorData.success) {
            updateChart(sensorData.data);
            updateDataTable(sensorData.data);
        } else {
            showError('Failed to load sensor data: ' + sensorData.error);
        }

        document.getElementById('lastUpdate').textContent = 'Last updated: ' + new Date().toLocaleString();
        
    } catch (error) {
        showError('Error loading data: ' + error.message);
    }
}

function updateStats(stats) {
    document.getElementById('totalMessages').textContent = stats.total_messages.toLocaleString();
    document.getElementById('recentMessages').textContent = stats.recent_messages.toLocaleString();
    document.getElementById('activeTopics').textContent = Object.keys(stats.topics_count).length;
    document.getElementById('systemStatus').textContent = 'Healthy';
}

function updateTopicSelect(topics) {
    const select = document.getElementById('topicSelect');
    const currentValue = select.value;
    
    // Clear existing options except "all"
    select.innerHTML = '<option value="all">All Topics</option>';
    
    topics.forEach(topic => {
        const option = document.createElement('option');
        option.value = topic;
        option.textContent = topic;
        select.appendChild(option);
    });
    
    // Restore previous selection if possible
    if (topics.includes(currentValue)) {
        select.value = currentValue;
    }
}

function updateChart(data) {
    const ctx = document.getElementById('sensorChart').getContext('2d');
    
    if (sensorChart) {
        sensorChart.destroy();
    }
    
    // Group data by topic and time
    const groupedData = {};
    data.forEach(item => {
        if (!groupedData[item.topic]) {
            groupedData[item.topic] = [];
        }
        groupedData[item.topic].push(item);
    });
    
    const datasets = Object.keys(groupedData).map((topic, index) => {
        const topicData = groupedData[topic];
        const colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40'];
        
        return {
            label: topic,
            data: topicData.map(item => ({
                x: new Date(item.timestamp),
                y: Object.values(item.data).filter(val => typeof val === 'number').length
            })),
            borderColor: colors[index % colors.length],
            backgroundColor: colors[index % colors.length] + '20',
            tension: 0.1
        };
    });
    
    sensorChart = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'minute'
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Data Points'
                    }
                }
            }
        }
    });
}

function updateDataTable(data) {
    const tbody = document.getElementById('dataTableBody');
    tbody.innerHTML = '';
    
    data.slice(0, 50).forEach(item => {
        const row = document.createElement('tr');
        
        const timestamp = new Date(item.timestamp).toLocaleString();
        const dataPreview = JSON.stringify(item.data).substring(0, 100) + '...';
        
        row.innerHTML = `
            <td>${timestamp}</td>
            <td>${item.topic}</td>
            <td>${item.sensor_id}</td>
            <td title="${JSON.stringify(item.data)}">${dataPreview}</td>
        `;
        
        tbody.appendChild(row);
    });
}

function showError(message) {
    // Simple error display - you might want to implement a better notification system
    console.error(message);
    alert('Error: ' + message);
}

// Load data on page load
document.addEventListener('DOMContentLoaded', function() {
    loadData();
    // Refresh data every 30 seconds
    setInterval(loadData, 30000);
});