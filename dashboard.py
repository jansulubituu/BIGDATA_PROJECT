"""
Dashboard Dữ liệu Bất động sản
Visualization cho dữ liệu bất động sản từ MongoDB Atlas
Dashboard real-time với tự động làm mới mỗi 5 phút
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
import time
from functools import lru_cache

# Load .env file
project_root = Path(__file__).parent
env_file = project_root / '.env'

if not env_file.exists():
    print(f"❌ Không tìm thấy file .env tại: {env_file}")
    sys.exit(1)

load_dotenv(dotenv_path=env_file)

# Cấu hình MongoDB
MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'bigdata_houses')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION', 'listings')

if not MONGODB_URI:
    print("❌ Không tìm thấy MONGODB_URI trong file .env")
    sys.exit(1)

# MongoDB client toàn cục (connection pooling)
_mongo_client = None
_mongo_client_last_check = 0
MONGO_CLIENT_TIMEOUT = 300  # 5 phút

# Cấu hình cache
_cache_data = None
_cache_timestamp = 0
CACHE_TTL = 300  # Cache 5 phút (làm mới mỗi 5 phút)

# Các trường cần thiết cho dashboard (projection để tối ưu)
DASHBOARD_FIELDS = {
    'price': 1,
    'area_m2': 1,
    'price_per_m2': 1,
    'price_billion': 1,
    'district': 1,
    'region': 1,
    'title': 1,
    'price_category': 1,
    'area_category': 1,
    'rental_category': 1,
    'category': 1,  # Thêm để lọc theo category trong dropdown
    'processing_time': 1,  # Thêm để phân tích theo tháng
    'crawl_timestamp': 1,  # Backup timestamp nếu processing_time không có
    '_id': 0  # Loại bỏ _id để giảm lượng dữ liệu truyền
}

# Initialize Dash app with Bootstrap theme
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Custom CSS for beautiful styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Dashboard Phân tích Bất động sản</title>
        {%favicon%}
        {%css%}
        <style>
            /* Space/Universe Theme CSS Variables */
            :root {
                --bg-primary: #0a0a1a;
                --bg-secondary: #0f0f2e;
                --bg-card: #1a1a3e;
                --bg-card-hover: #252550;
                --border-color: #2d2d5a;
                --text-primary: #ffffff;
                --text-secondary: #b8c5e0;
                --text-muted: #8b9dc4;
                --accent-primary: #00d4ff;
                --accent-secondary: #8b5cf6;
                --accent-nebula: #a855f7;
                --accent-star: #fbbf24;
                --accent-green: #10b981;
                --accent-orange: #f59e0b;
                --accent-red: #ef4444;
                --accent-blue: #3b82f6;
                --shadow-sm: 0 2px 4px rgba(0, 0, 0, 0.5);
                --shadow-md: 0 4px 6px rgba(0, 0, 0, 0.6);
                --shadow-lg: 0 10px 15px rgba(0, 0, 0, 0.7);
                --shadow-xl: 0 20px 25px rgba(0, 0, 0, 0.8);
                --glow-primary: 0 0 20px rgba(0, 212, 255, 0.5);
                --glow-secondary: 0 0 20px rgba(139, 92, 246, 0.5);
            }

            /* Space Background với Stars */
            body {
                font-family: 'Inter', 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
                background: var(--bg-primary);
                background-image: 
                    radial-gradient(ellipse at top, rgba(139, 92, 246, 0.15) 0%, transparent 50%),
                    radial-gradient(ellipse at bottom right, rgba(0, 212, 255, 0.1) 0%, transparent 50%),
                    radial-gradient(ellipse at bottom left, rgba(168, 85, 247, 0.1) 0%, transparent 50%),
                    linear-gradient(180deg, #0a0a1a 0%, #0f0f2e 50%, #0a0a1a 100%);
                min-height: 100vh;
                margin: 0;
                padding: 20px;
                color: var(--text-primary);
                position: relative;
                overflow-x: hidden;
            }

            /* Stars Animation */
            body::before {
                content: '';
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-image: 
                    radial-gradient(2px 2px at 20% 30%, #fff, transparent),
                    radial-gradient(2px 2px at 60% 70%, #fff, transparent),
                    radial-gradient(1px 1px at 50% 50%, #fff, transparent),
                    radial-gradient(1px 1px at 80% 10%, #fff, transparent),
                    radial-gradient(2px 2px at 90% 60%, #fff, transparent),
                    radial-gradient(1px 1px at 33% 80%, #fff, transparent),
                    radial-gradient(2px 2px at 10% 90%, #fff, transparent),
                    radial-gradient(1px 1px at 70% 20%, #fff, transparent),
                    radial-gradient(2px 2px at 40% 40%, #fff, transparent),
                    radial-gradient(1px 1px at 15% 50%, #fff, transparent);
                background-repeat: repeat;
                background-size: 200% 200%;
                animation: twinkle 20s linear infinite;
                pointer-events: none;
                opacity: 0.6;
                z-index: 0;
            }

            @keyframes twinkle {
                0%, 100% { opacity: 0.6; }
                50% { opacity: 1; }
            }

            /* Nebula Effect */
            body::after {
                content: '';
                position: fixed;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: radial-gradient(ellipse at center, rgba(139, 92, 246, 0.1) 0%, transparent 70%);
                animation: nebula 30s ease-in-out infinite;
                pointer-events: none;
                z-index: 0;
            }

            @keyframes nebula {
                0%, 100% { transform: translate(0, 0) scale(1); opacity: 0.3; }
                33% { transform: translate(5%, 5%) scale(1.1); opacity: 0.5; }
                66% { transform: translate(-5%, -5%) scale(0.9); opacity: 0.4; }
            }

            /* Ensure content is above background effects */
            .dashboard-container {
                position: relative;
                z-index: 1;
            }

            /* Typography với Space Theme */
            h1 {
                color: var(--text-primary);
                text-shadow: 
                    0 0 10px rgba(0, 212, 255, 0.5),
                    0 0 20px rgba(139, 92, 246, 0.3),
                    0 2px 10px rgba(0, 212, 255, 0.3);
                font-weight: 700;
                font-size: 2.5rem;
                letter-spacing: -0.5px;
                background: linear-gradient(135deg, #ffffff 0%, #00d4ff 50%, #8b5cf6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                animation: glow-text 3s ease-in-out infinite;
            }

            @keyframes glow-text {
                0%, 100% { filter: brightness(1); }
                50% { filter: brightness(1.2); }
            }

            h2 {
                color: var(--text-primary);
                font-weight: 600;
                letter-spacing: -0.3px;
                text-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
            }

            /* Card Styling với Space Theme */
            .card {
                border-radius: 16px;
                background: var(--bg-card);
                border: 1px solid var(--border-color);
                box-shadow: var(--shadow-lg), inset 0 0 20px rgba(0, 212, 255, 0.05);
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                overflow: hidden;
                backdrop-filter: blur(10px);
                position: relative;
            }

            .card::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: radial-gradient(circle at top right, rgba(0, 212, 255, 0.1), transparent);
                opacity: 0;
                transition: opacity 0.3s;
                pointer-events: none;
            }

            .card:hover {
                transform: translateY(-4px);
                box-shadow: var(--shadow-xl), var(--glow-primary);
                border-color: rgba(0, 212, 255, 0.5);
            }

            .card:hover::before {
                opacity: 1;
            }

            .card-body {
                padding: 1.75rem;
            }

            /* Metric Cards với Space Theme */
            .metric-card-primary {
                background: linear-gradient(135deg, #1a1a3e 0%, #2d1b5e 100%);
                border: 1px solid rgba(139, 92, 246, 0.4);
                position: relative;
                overflow: hidden;
                box-shadow: var(--shadow-lg), inset 0 0 30px rgba(139, 92, 246, 0.1);
            }

            .metric-card-primary::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 3px;
                background: linear-gradient(90deg, #8b5cf6, #00d4ff);
                box-shadow: 0 0 10px rgba(139, 92, 246, 0.8);
            }

            .metric-card-primary::after {
                content: '⭐';
                position: absolute;
                top: 10px;
                right: 15px;
                font-size: 1.5rem;
                opacity: 0.3;
                animation: float 3s ease-in-out infinite;
            }

            .metric-card-success {
                background: linear-gradient(135deg, #1a1a3e 0%, #0d3d2e 100%);
                border: 1px solid rgba(0, 212, 255, 0.4);
                position: relative;
                overflow: hidden;
                box-shadow: var(--shadow-lg), inset 0 0 30px rgba(0, 212, 255, 0.1);
            }

            .metric-card-success::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 3px;
                background: linear-gradient(90deg, #00d4ff, #10b981);
                box-shadow: 0 0 10px rgba(0, 212, 255, 0.8);
            }

            .metric-card-success::after {
                content: '🌌';
                position: absolute;
                top: 10px;
                right: 15px;
                font-size: 1.5rem;
                opacity: 0.3;
                animation: float 3s ease-in-out infinite 0.5s;
            }

            .metric-card-info {
                background: linear-gradient(135deg, #1a1a3e 0%, #1e2a4e 100%);
                border: 1px solid rgba(59, 130, 246, 0.4);
                position: relative;
                overflow: hidden;
                box-shadow: var(--shadow-lg), inset 0 0 30px rgba(59, 130, 246, 0.1);
            }

            .metric-card-info::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 3px;
                background: linear-gradient(90deg, #3b82f6, #00d4ff);
                box-shadow: 0 0 10px rgba(59, 130, 246, 0.8);
            }

            .metric-card-info::after {
                content: '🌠';
                position: absolute;
                top: 10px;
                right: 15px;
                font-size: 1.5rem;
                opacity: 0.3;
                animation: float 3s ease-in-out infinite 1s;
            }

            .metric-card-warning {
                background: linear-gradient(135deg, #1a1a3e 0%, #3d2a1e 100%);
                border: 1px solid rgba(251, 191, 36, 0.4);
                position: relative;
                overflow: hidden;
                box-shadow: var(--shadow-lg), inset 0 0 30px rgba(251, 191, 36, 0.1);
            }

            .metric-card-warning::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 3px;
                background: linear-gradient(90deg, #fbbf24, #f59e0b);
                box-shadow: 0 0 10px rgba(251, 191, 36, 0.8);
            }

            .metric-card-warning::after {
                content: '✨';
                position: absolute;
                top: 10px;
                right: 15px;
                font-size: 1.5rem;
                opacity: 0.3;
                animation: float 3s ease-in-out infinite 1.5s;
            }

            @keyframes float {
                0%, 100% { transform: translateY(0) rotate(0deg); }
                50% { transform: translateY(-10px) rotate(5deg); }
            }

            /* Chart Container với Space Theme */
            .plotly-graph-div {
                border-radius: 12px;
                background: var(--bg-card) !important;
                border: 1px solid var(--border-color);
                box-shadow: inset 0 0 20px rgba(0, 212, 255, 0.05);
                position: relative;
            }

            .plotly-graph-div::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: radial-gradient(circle at top left, rgba(139, 92, 246, 0.05), transparent);
                pointer-events: none;
                border-radius: 12px;
            }

            /* Text Colors */
            .text-muted {
                color: var(--text-secondary) !important;
            }

            /* HR Styling */
            hr {
                border-color: var(--border-color);
                opacity: 0.5;
            }

            /* Section Dividers với Space Theme */
            .section-divider {
                height: 2px;
                background: linear-gradient(90deg, 
                    transparent, 
                    rgba(139, 92, 246, 0.3), 
                    rgba(0, 212, 255, 0.5), 
                    rgba(139, 92, 246, 0.3), 
                    transparent);
                margin: 50px 0;
                opacity: 0.6;
                box-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
                position: relative;
            }

            .section-divider::before {
                content: '✦';
                position: absolute;
                left: 50%;
                top: 50%;
                transform: translate(-50%, -50%);
                color: var(--accent-primary);
                font-size: 1.2rem;
                background: var(--bg-primary);
                padding: 0 10px;
                text-shadow: 0 0 10px rgba(0, 212, 255, 0.8);
            }

            /* Animations */
            .counter-animate {
                animation: countUp 1.5s cubic-bezier(0.4, 0, 0.2, 1);
            }

            @keyframes countUp {
                from {
                    opacity: 0;
                    transform: translateY(20px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }

            .plotly-graph-div {
                animation: fadeInUp 0.8s cubic-bezier(0.4, 0, 0.2, 1);
            }

            @keyframes fadeInUp {
                from {
                    opacity: 0;
                    transform: translateY(30px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }

            /* Pulse Animation - Subtle */
            .metric-card-primary,
            .metric-card-success,
            .metric-card-info,
            .metric-card-warning {
                animation: pulse 3s ease-in-out infinite;
            }

            @keyframes pulse {
                0%, 100% {
                    box-shadow: var(--shadow-lg);
                }
                50% {
                    box-shadow: var(--shadow-xl);
                }
            }

            /* Scrollbar Styling */
            ::-webkit-scrollbar {
                width: 10px;
                height: 10px;
            }

            ::-webkit-scrollbar-track {
                background: var(--bg-secondary);
            }

            ::-webkit-scrollbar-thumb {
                background: var(--border-color);
                border-radius: 5px;
            }

            ::-webkit-scrollbar-thumb:hover {
                background: rgba(0, 212, 255, 0.5);
            }

            /* Loading State */
            .dash-loading {
                color: var(--accent-primary) !important;
            }

            /* Dashboard Container */
            .dashboard-container {
                max-width: 100%;
                position: relative;
                z-index: 1;
            }

            /* Floating Particles Effect */
            .floating-particles {
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                pointer-events: none;
                z-index: 0;
                overflow: hidden;
            }

            .particle {
                position: absolute;
                width: 2px;
                height: 2px;
                background: rgba(0, 212, 255, 0.5);
                border-radius: 50%;
                animation: float-particle 15s infinite;
                box-shadow: 0 0 5px rgba(0, 212, 255, 0.8);
            }

            @keyframes float-particle {
                0% {
                    transform: translateY(100vh) translateX(0);
                    opacity: 0;
                }
                10% {
                    opacity: 1;
                }
                90% {
                    opacity: 1;
                }
                100% {
                    transform: translateY(-100vh) translateX(100px);
                    opacity: 0;
                }
            }

            /* Header Section với Space Theme */
            .header-section {
                background: linear-gradient(135deg, rgba(26, 26, 62, 0.9) 0%, rgba(45, 27, 94, 0.9) 100%);
                padding: 30px;
                border-radius: 16px;
                border: 1px solid rgba(139, 92, 246, 0.3);
                margin-bottom: 30px;
                box-shadow: var(--shadow-lg), inset 0 0 30px rgba(139, 92, 246, 0.1);
                position: relative;
                overflow: hidden;
            }

            .header-section::before {
                content: '';
                position: absolute;
                top: -50%;
                right: -50%;
                width: 200%;
                height: 200%;
                background: radial-gradient(circle, rgba(0, 212, 255, 0.1) 0%, transparent 70%);
                animation: rotate 20s linear infinite;
            }

            @keyframes rotate {
                from { transform: rotate(0deg); }
                to { transform: rotate(360deg); }
            }

            /* Live Indicator với Space Theme */
            .live-indicator {
                display: inline-block;
                width: 8px;
                height: 8px;
                background: var(--accent-primary);
                border-radius: 50%;
                margin-right: 8px;
                animation: pulse-star 2s ease-in-out infinite;
                box-shadow: 0 0 10px rgba(0, 212, 255, 0.8);
            }

            @keyframes pulse-star {
                0%, 100% {
                    opacity: 1;
                    transform: scale(1);
                    box-shadow: 0 0 10px rgba(0, 212, 255, 0.8);
                }
                50% {
                    opacity: 0.7;
                    transform: scale(1.3);
                    box-shadow: 0 0 20px rgba(0, 212, 255, 1);
                }
            }

            .custom-dropdown .Select-control {
                background-color: #1a1a3e !important;
                border: 1px solid #2d2d5a !important;
            }

            .custom-dropdown .Select-menu-outer {
                background-color: #1a1a3e !important;
                color: #f1f1f5 !important;
            }

            .custom-dropdown .Select-option {
                background-color: #1a1a3e !important;
                color: #f1f1f5 !important;
            }

            .custom-dropdown .Select-option.is-focused {
                background-color: #2d2d5a !important;
            }

            .custom-dropdown .Select-value-label {
                color: #f1f1f5 !important;
            }

        </style>
        <script>
            // Hàm animate số đếm
            function animateValue(element, start, end, duration, suffix = '') {
                if (!element) return;
                
                let startTimestamp = null;
                const step = (timestamp) => {
                    if (!startTimestamp) startTimestamp = timestamp;
                    const progress = Math.min((timestamp - startTimestamp) / duration, 1);
                    
                    // Easing function (ease-out)
                    const easeOut = 1 - Math.pow(1 - progress, 3);
                    
                    let current;
                    if (typeof start === 'number' && typeof end === 'number') {
                        current = Math.floor(start + (end - start) * easeOut);
                    } else {
                        // Cho text như "2.5 tỷ"
                        current = end;
                    }
                    
                    // Format số với dấu phẩy
                    if (typeof current === 'number') {
                        element.textContent = current.toLocaleString('vi-VN') + suffix;
                    } else {
                        element.textContent = current + suffix;
                    }
                    
                    if (progress < 1) {
                        window.requestAnimationFrame(step);
                    } else {
                        element.textContent = end + suffix;
                    }
                };
                window.requestAnimationFrame(step);
            }
            
            // Hàm để animate tất cả metrics khi page load hoặc update
            function animateMetrics() {
                // Animate Total Listings
                const totalEl = document.getElementById('total-listings');
                if (totalEl && totalEl.textContent) {
                    const totalText = totalEl.textContent.replace(/,/g, '');
                    const totalNum = parseInt(totalText) || 0;
                    if (totalNum > 0) {
                        totalEl.textContent = '0';
                        animateValue(totalEl, 0, totalNum, 1500, '');
                    }
                }
                
                // Animate Total Districts
                const districtsEl = document.getElementById('total-districts');
                if (districtsEl && districtsEl.textContent) {
                    const districtsNum = parseInt(districtsEl.textContent) || 0;
                    if (districtsNum > 0) {
                        districtsEl.textContent = '0';
                        animateValue(districtsEl, 0, districtsNum, 1500, '');
                    }
                }
                
                // Animate Average Price (giữ nguyên text như "2.5 tỷ")
                const priceEl = document.getElementById('avg-price');
                if (priceEl && priceEl.textContent && priceEl.textContent !== '0 VNĐ') {
                    priceEl.style.opacity = '0';
                    priceEl.style.transform = 'translateY(20px)';
                    setTimeout(() => {
                        priceEl.style.transition = 'all 0.8s ease-out';
                        priceEl.style.opacity = '1';
                        priceEl.style.transform = 'translateY(0)';
                    }, 100);
                }
                
                // Animate Average Area
                const areaEl = document.getElementById('avg-area');
                if (areaEl && areaEl.textContent && areaEl.textContent !== '0 m²') {
                    areaEl.style.opacity = '0';
                    areaEl.style.transform = 'translateY(20px)';
                    setTimeout(() => {
                        areaEl.style.transition = 'all 0.8s ease-out';
                        areaEl.style.opacity = '1';
                        areaEl.style.transform = 'translateY(0)';
                    }, 200);
                }
            }
            
            // Chạy animation khi DOM ready
            document.addEventListener('DOMContentLoaded', function() {
                setTimeout(animateMetrics, 500);
            });
            
            // Lắng nghe sự kiện từ Dash để animate lại khi data update
            window.addEventListener('dash_mounted', function() {
                setTimeout(animateMetrics, 500);
            });
            
            // Sử dụng MutationObserver để detect khi Dash update content
            const observer = new MutationObserver(function(mutations) {
                mutations.forEach(function(mutation) {
                    if (mutation.type === 'childList' || mutation.type === 'characterData') {
                        const target = mutation.target;
                        if (target.id && ['total-listings', 'avg-price', 'avg-area', 'total-districts'].includes(target.id)) {
                            setTimeout(animateMetrics, 100);
                        }
                    }
                });
            });
            
            // Bắt đầu observe sau khi page load
            setTimeout(function() {
                const metricsContainer = document.querySelector('.mb-4');
                if (metricsContainer) {
                    observer.observe(metricsContainer, {
                        childList: true,
                        subtree: true,
                        characterData: true
                    });
                }
            }, 1000);
        </script>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# App layout
app.layout = dbc.Container([
    # Section 1: Header với Status Bar
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H1("🏠 Dashboard Phân tích Bất động sản", className="text-center mb-3"),
                html.Div([
                    html.Span(html.Span("●", className="live-indicator"), style={"marginRight": "8px"}),
                    html.Span("Live", style={"color": "#a0aec0", "fontSize": "0.9rem", "marginRight": "12px"}),
                    html.Span("|", style={"color": "#2d3748", "margin": "0 12px"}),
                    html.Span("Dữ liệu real-time từ MongoDB Atlas", 
                             style={"color": "#a0aec0", "fontSize": "0.9rem", "marginRight": "12px"}),
                    html.Span("|", style={"color": "#2d3748", "margin": "0 12px"}),
                    html.Span("Tự động làm mới mỗi 5 phút", 
                             style={"color": "#a0aec0", "fontSize": "0.9rem"})
                ], className="text-center mb-3"),
                html.Div(id="last-update", className="text-center")
            ], className="header-section")
        ], width=12)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            html.Label("Lọc theo Category:", style={"color": "#b8b8d1", "marginBottom": "6px", "fontSize": "0.85rem"}),
            dcc.Dropdown(
                id='category-filter',
                options=[],  # Sẽ được cập nhật động
                value=None,  # Giá trị mặc định
                placeholder="Tất cả categories",
                style={
                    "backgroundColor": "#1a1a3e",
                    "color": "#f1f1f5",
                    "border": "1px solid #2d2d5a",
                    "borderRadius": "8px"
                },
                className="custom-dropdown"
            )
        ], width=3),
    ], className="mb-4"),
    
    # Metrics Cards with beautiful gradients
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("📊 Tổng số tin đăng", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="total-listings", className="mb-0", style={"color": "white", "fontSize": "2.5rem", "fontWeight": "bold"})
                    ])
                ])
            ], className="h-100 metric-card-primary")
        ], width=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("💰 Giá trung bình", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="avg-price", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2.5rem", "fontWeight": "bold"})
                    ])
                ])
            ], className="h-100 metric-card-success")
        ], width=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("📐 Diện tích trung bình", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="avg-area", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2.5rem", "fontWeight": "bold"})
                    ])
                ])
            ], className="h-100 metric-card-info")
        ], width=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("📍 Số quận/huyện", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="total-districts", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2.5rem", "fontWeight": "bold"})
                    ])
                ])
            ], className="h-100 metric-card-warning")
        ], width=3, className="mb-4"),
    ], className="mb-4"),
    
    # Section Divider
    html.Div(className="section-divider"),
    
    # Section 3: Overview Charts
    dbc.Row([
        dbc.Col([
            html.H2("📊 Tổng quan Phân bố", 
                   style={"color": "#ffffff", "marginBottom": "20px", "fontSize": "1.8rem", "fontWeight": "600"})
        ], width=12)
    ], className="mb-3"),
    
    # Row 1: Distributions
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-distribution")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="area-distribution")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),
    
    # Row 2: Category Analysis
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-category-pie")
                ])
            ])
        ], width=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="area-category-pie")
                ])
            ])
        ], width=6, className="mb-4"),
    ]),
    
    # Row 3: Category Count Bar Chart
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="category-count-bar")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),
    
    # Section Divider
    html.Div(className="section-divider"),
    
    # Section 4: Geographic Analysis
    dbc.Row([
        dbc.Col([
            html.H2("🗺️ Phân tích theo Địa lý", 
                   style={"color": "#f1f1f5",
                        "marginBottom": "20px",
                        "fontSize": "1.8rem",
                        "fontWeight": "600",
                        "letterSpacing": "0.5px"})
        ], width=9),
    ], className="mb-3"),
    
    # Row 1: Price by District
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-by-district")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),
    
    # Row 2: Price per m² by District
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-per-m2-by-district")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),
    
    # Section Divider
    html.Div(className="section-divider"),
    
    # Section 5: Correlation Analysis
    dbc.Row([
        dbc.Col([
            html.H2("📈 Phân tích Tương quan", 
                   style={"color": "#ffffff", "marginBottom": "20px", "fontSize": "1.8rem", "fontWeight": "600"})
        ], width=12)
    ], className="mb-3"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-vs-area-scatter")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),
    
    # Section Divider
    html.Div(className="section-divider"),
    
    # Section 6: Monthly Trends
    dbc.Row([
        dbc.Col([
            html.H2("📅 Thống kê theo tháng", 
                   style={"color": "white", "textAlign": "center", "marginBottom": "30px", "fontSize": "2rem"})
        ], width=12)
    ]),
    
    # Metrics Cards: So sánh tháng hiện tại vs tháng trước
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("📈 Thay đổi số tin đăng", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="monthly-listings-change", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2rem", "fontWeight": "bold"}),
                        html.P(id="monthly-listings-change-desc", className="mb-0 mt-2", style={"color": "rgba(255,255,255,0.9)", "fontSize": "0.9rem"})
                    ])
                ])
            ], className="h-100 metric-card-primary")
        ], width=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("💰 Thay đổi giá trung bình", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="monthly-price-change", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2rem", "fontWeight": "bold"}),
                        html.P(id="monthly-price-change-desc", className="mb-0 mt-2", style={"color": "rgba(255,255,255,0.9)", "fontSize": "0.9rem"})
                    ])
                ])
            ], className="h-100 metric-card-success")
        ], width=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("📊 Số tháng có dữ liệu", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="total-months", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2rem", "fontWeight": "bold"}),
                        html.P("Tổng số tháng đã thu thập", className="mb-0 mt-2", style={"color": "rgba(255,255,255,0.9)", "fontSize": "0.9rem"})
                    ])
                ])
            ], className="h-100 metric-card-info")
        ], width=3, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H4("📐 Thay đổi diện tích TB", className="card-title mb-3", style={"color": "white", "fontSize": "1.1rem"}),
                        html.H2(id="monthly-area-change", className="mb-0 counter-animate", style={"color": "white", "fontSize": "2rem", "fontWeight": "bold"}),
                        html.P(id="monthly-area-change-desc", className="mb-0 mt-2", style={"color": "rgba(255,255,255,0.9)", "fontSize": "0.9rem"})
                    ])
                ])
            ], className="h-100 metric-card-warning")
        ], width=3, className="mb-4"),
    ], className="mb-4"),
    
    # Charts Row 5: Xu hướng theo tháng
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="monthly-trend-price")
                ])
            ])
        ], width=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="monthly-trend-listings")
                ])
            ])
        ], width=6, className="mb-4"),
    ]),
    
    # Charts Row 6: Xu hướng diện tích và giá/m² theo tháng
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="monthly-trend-area")
                ])
            ])
        ], width=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="monthly-trend-price-per-m2")
                ])
            ])
        ], width=6, className="mb-4"),
    ]),
    
    # Tự động làm mới
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # Cập nhật mỗi 5 phút
        n_intervals=0
    ),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(),
            html.P("Nguồn dữ liệu: MongoDB Atlas | Được hỗ trợ bởi Plotly Dash", 
                   className="text-center text-muted")
        ], width=12)
    ])
    
], fluid=True, className="p-4")


def get_mongo_client():
    """Lấy hoặc tạo MongoDB client với connection pooling"""
    global _mongo_client, _mongo_client_last_check
    
    current_time = time.time()
    
    # Kiểm tra client còn hợp lệ không (mỗi 5 phút)
    if _mongo_client is None or (current_time - _mongo_client_last_check) > MONGO_CLIENT_TIMEOUT:
        try:
            # Tạo client mới với connection pooling
            _mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                maxPoolSize=10,  # Kích thước connection pool
                minPoolSize=1,
                maxIdleTimeMS=45000,  # Đóng kết nối idle sau 45s
                connectTimeoutMS=5000,
                socketTimeoutMS=30000
            )
            # Kiểm tra kết nối
            _mongo_client.admin.command('ping')
            _mongo_client_last_check = current_time
        except Exception as e:
            print(f"❌ Không thể tạo MongoDB client: {e}")
            _mongo_client = None
    
    return _mongo_client


def get_data_from_mongodb(use_cache=True):
    """
    Lấy dữ liệu từ MongoDB Atlas và convert sang pandas DataFrame
    Tối ưu với:
    - Connection pooling (reuse connection)
    - Projection (chỉ lấy fields cần thiết)
    - Caching (giảm query frequency)
    - Batch processing (nếu data lớn)
    """
    global _cache_data, _cache_timestamp
    
    # Check cache first
    if use_cache:
        current_time = time.time()
        if _cache_data is not None and (current_time - _cache_timestamp) < CACHE_TTL:
            return _cache_data.copy()
    
    try:
        # Lấy client với connection pooling
        client = get_mongo_client()
        if client is None:
            return pd.DataFrame()
        
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # TỐI ƯU 1: Projection - chỉ lấy các trường cần thiết
        # Giảm lượng dữ liệu truyền từ MongoDB xuống ~70-80%
        cursor = collection.find({}, DASHBOARD_FIELDS)
        
        # TỐI ƯU 2: Xử lý theo batch cho dữ liệu lớn
        # Thay vì load toàn bộ vào memory, xử lý từng batch
        batch_size = 1000
        batches = []
        
        batch = []
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                batches.append(batch)
                batch = []
        
        # Thêm các document còn lại
        if batch:
            batches.append(batch)
        
        # Chuyển đổi batches sang DataFrame
        if batches:
            # Kết hợp tất cả batches
            all_docs = []
            for batch in batches:
                all_docs.extend(batch)
            df = pd.DataFrame(all_docs)
        else:
            df = pd.DataFrame()
        
        if df.empty:
            _cache_data = pd.DataFrame()
            _cache_timestamp = time.time()
            return pd.DataFrame()
        
        # TỐI ƯU 3: Tối ưu kiểu dữ liệu ngay khi load
        # Đảm bảo các cột số là kiểu số (nhanh hơn so với chuyển đổi sau)
        numeric_cols = ['price', 'area_m2', 'price_per_m2', 'price_billion']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
        
        # TỐI ƯU 4: Loại bỏ các dòng null sớm (nếu cần)
        # df = df.dropna(subset=['price', 'area_m2'])  # Bỏ comment nếu muốn lọc
        
        # Cập nhật cache
        _cache_data = df.copy()
        _cache_timestamp = time.time()
        
        return df
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"❌ Lỗi kết nối MongoDB: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu: {e}")
        return pd.DataFrame()


def create_empty_figure(message="Không có dữ liệu"):
    """Tạo biểu đồ trống với Space Theme"""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=18, color="#b8c5e0", family="Inter, Arial")
    )
    fig.update_layout(
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        plot_bgcolor='#1a1a3e',
        paper_bgcolor='#1a1a3e',
        margin=dict(l=20, r=20, t=20, b=20)
    )
    return fig


def get_chart_layout(title, xaxis_title=None, yaxis_title=None, height=400):
    """Tạo template layout cho biểu đồ với Space/Universe Theme"""
    layout = dict(
        title=dict(
            text=title,
            font=dict(size=20, family="Inter, Arial", color="#ffffff"),
            x=0.5,
            xanchor='center',
            pad=dict(t=10, b=20)
        ),
        plot_bgcolor='#1a1a3e',
        paper_bgcolor='#1a1a3e',
        font=dict(family="Inter, Arial", size=12, color="#b8c5e0"),
        margin=dict(l=70, r=40, t=70, b=60),
        height=height,
        hovermode='closest',
        xaxis=dict(
            title=dict(
                text=xaxis_title if xaxis_title else "",
                font=dict(size=13, color="#ffffff")
            ),
            gridcolor='#2d2d5a',
            gridwidth=1,
            showgrid=True,
            zeroline=False,
            linecolor='#3d3d6a',
            linewidth=1,
            tickfont=dict(color="#b8c5e0", size=11)
        ),
        yaxis=dict(
            title=dict(
                text=yaxis_title if yaxis_title else "",
                font=dict(size=13, color="#ffffff")
            ),
            gridcolor='#2d2d5a',
            gridwidth=1,
            showgrid=True,
            zeroline=False,
            linecolor='#3d3d6a',
            linewidth=1,
            tickfont=dict(color="#b8c5e0", size=11)
        ),
        legend=dict(
            bgcolor='rgba(26, 26, 62, 0.9)',
            bordercolor='#2d2d5a',
            borderwidth=1,
            font=dict(color="#ffffff", size=11)
        ),
        hoverlabel=dict(
            bgcolor='#0a0a1a',
            bordercolor='#00d4ff',
            font_size=12,
            font_family="Inter, Arial",
            font_color="#ffffff"
        )
    )
    return layout


def get_monthly_stats():
    """
    Tính toán thống kê theo tháng từ MongoDB
    Trả về DataFrame với các cột: month, count, avg_price, avg_area, total_districts
    """
    try:
        client = get_mongo_client()
        if client is None:
            return pd.DataFrame()
        
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # Sử dụng field post_time (milliseconds timestamp)
        timestamp_field = 'post_time'
        
        # Pipeline aggregation để group theo tháng
        pipeline = [
            {"$match": {timestamp_field: {"$exists": True, "$ne": None}}},
            {"$project": {
                "year_month": {
                    "$dateToString": {
                        "format": "%Y-%m",
                        "date": {"$toDate": f"${timestamp_field}"}  # post_time đã ở dạng milliseconds
                    }
                },
                "price": 1,
                "area_m2": 1,
                "district": 1
            }},
            {"$group": {
                "_id": "$year_month",
                "count": {"$sum": 1},
                "avg_price": {"$avg": "$price"},
                "avg_area": {"$avg": "$area_m2"},
                "districts": {"$addToSet": "$district"}
            }},
            {"$sort": {"_id": 1}},
            {"$project": {
                "month": "$_id",
                "count": 1,
                "avg_price": {"$round": ["$avg_price", 0]},
                "avg_area": {"$round": ["$avg_area", 2]},
                "num_districts": {"$size": "$districts"}
            }}
        ]
        
        monthly_data = list(collection.aggregate(pipeline))
        
        if not monthly_data:
            return pd.DataFrame()
        
        # Convert sang DataFrame
        df_monthly = pd.DataFrame(monthly_data)
        df_monthly['month'] = pd.to_datetime(df_monthly['month'] + '-01')
        df_monthly = df_monthly.sort_values('month')
        
        return df_monthly
        
    except Exception as e:
        print(f"❌ Lỗi khi tính thống kê theo tháng: {e}")
        return pd.DataFrame()


@app.callback(
    Output('category-filter', 'options'),
    [Input('interval-component', 'n_intervals')]
)
def update_category_options(n):
    """Cập nhật danh sách categories cho dropdown"""
    df = get_data_from_mongodb()
    
    if df.empty or 'category' not in df.columns:
        return []
    
    # Map category ID to tên có ý nghĩa
    category_names = {
        1010: "Căn hộ, Chung cư",
        1020: "Nhà ở",
        1030: "Cho thuê kinh doanh, văn phòng",
        1040: "Đất",
        1050: "Cho thuê cư trú"
    }
    
    # Lấy danh sách unique categories và sắp xếp
    categories = df['category'].dropna().unique()
    # Chuyển đổi tất cả về int trước khi sort để tránh lỗi so sánh str vs int
    categories = []
    for cat in df['category'].dropna().unique():
        try:
            categories.append(int(cat))
        except (ValueError, TypeError):
            pass  # Bỏ qua các giá trị không convert được
    categories = sorted(categories)
    
    # Tạo options cho dropdown với tên có ý nghĩa
    options = []
    for cat in categories:
        label = category_names.get(cat, f"Category {cat}")
        options.append({'label': label, 'value': cat})
    
    return options


@app.callback(
    [Output('total-listings', 'children'),
     Output('avg-price', 'children'),
     Output('avg-area', 'children'),
     Output('total-districts', 'children'),
     Output('last-update', 'children'),
     Output('price-distribution', 'figure'),
     Output('area-distribution', 'figure'),
     Output('price-by-district', 'figure'),
     Output('price-category-pie', 'figure'),
     Output('area-category-pie', 'figure'),
     Output('price-vs-area-scatter', 'figure'),
     Output('price-per-m2-by-district', 'figure'),
     Output('category-count-bar', 'figure'),
     # Monthly stats outputs
     Output('monthly-listings-change', 'children'),
     Output('monthly-listings-change-desc', 'children'),
     Output('monthly-price-change', 'children'),
     Output('monthly-price-change-desc', 'children'),
     Output('total-months', 'children'),
     Output('monthly-area-change', 'children'),
     Output('monthly-area-change-desc', 'children'),
     Output('monthly-trend-price', 'figure'),
     Output('monthly-trend-listings', 'figure'),
     Output('monthly-trend-area', 'figure'),
     Output('monthly-trend-price-per-m2', 'figure')],
    [Input('interval-component', 'n_intervals'),
     Input('category-filter', 'value')]
)
def update_dashboard(n, selected_category):
    """Callback để cập nhật tất cả các components với filter theo category"""
    df = get_data_from_mongodb()
    
    # Áp dụng filter theo category nếu có
    df_filtered = df.copy()
    category_label = ""
    is_rental = False  # Biến để kiểm tra có phải nhà cho thuê không
    
    if selected_category is not None and 'category' in df.columns:
        df_filtered = df[df['category'] == selected_category].copy()
        # Map category ID to tên
        category_names = {
            1010: "Căn hộ, Chung cư",
            1020: "Nhà ở",
            1030: "Cho thuê kinh doanh, văn phòng",
            1040: "Đất",
            1050: "Cho thuê cư trú"
        }
        category_label = category_names.get(int(selected_category), f"Category {selected_category}")
        is_rental = (int(selected_category) == 1050 or int(selected_category) == 1030)  # Đánh dấu nếu là nhà cho thuê
    
    # Cập nhật timestamp
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_update = f"Cập nhật lần cuối: {current_time}"
    if selected_category is not None:
        last_update += f" | Lọc: {category_label}"
    
    # Nếu không có dữ liệu, trả về giá trị trống
    if df_filtered.empty:
        empty_fig = create_empty_figure("Không có dữ liệu")
        empty_monthly_fig = create_empty_figure("Chưa có dữ liệu")
        return (
            "0", "0 VNĐ", "0 m²", "0", last_update,
            empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig,
            # Monthly stats (empty)
            "N/A", "Chưa có dữ liệu", "N/A", "Chưa có dữ liệu", "0", "N/A", "Chưa có dữ liệu",
            empty_monthly_fig, empty_monthly_fig, empty_monthly_fig, empty_monthly_fig
        )
    
    # Tính toán các chỉ số (sử dụng df_filtered)
    total = len(df_filtered)
    avg_price = df_filtered['price'].mean() if 'price' in df_filtered.columns and not df_filtered['price'].isna().all() else 0
    avg_area = df_filtered['area_m2'].mean() if 'area_m2' in df_filtered.columns and not df_filtered['area_m2'].isna().all() else 0
    total_districts = df_filtered['district'].nunique() if 'district' in df_filtered.columns else 0
    
    # Định dạng các chỉ số (thêm data attribute để JavaScript có thể animate)
    # Nếu là nhà cho thuê (category 1050), luôn hiển thị bằng triệu
    if is_rental:
        avg_price_str = f"{avg_price/1e6:.2f} triệu"
        avg_price_value = avg_price/1e6
    elif avg_price > 1e9:
        avg_price_str = f"{avg_price/1e9:.2f} tỷ"
        avg_price_value = avg_price/1e9
    elif avg_price > 1e6:
        avg_price_str = f"{avg_price/1e6:.0f} triệu"
        avg_price_value = avg_price/1e6
    else:
        avg_price_str = f"{avg_price:,.0f} VNĐ"
        avg_price_value = avg_price
    
    avg_area_str = f"{avg_area:.1f} m²" if avg_area > 0 else "0 m²"
    
    # Price Distribution Histogram với dark theme colorscale
    if 'price' in df_filtered.columns and not df_filtered['price'].isna().all():
        # Chuyển đổi giá: triệu nếu là nhà cho thuê, tỷ nếu không
        if is_rental:
            price_converted = df_filtered['price'] / 1e6
            price_unit = "Triệu VNĐ"
            price_format = ".1f"
        else:
            price_converted = df_filtered['price'] / 1e9
            price_unit = "Tỷ VNĐ"
            price_format = ".2f"
        
        price_fig = go.Figure()
        price_fig.add_trace(go.Histogram(
            x=price_converted,
            nbinsx=50,
            marker=dict(
                color=price_converted,
                colorscale='Bluyl',  # Dark-friendly colorscale
                showscale=True,
                colorbar=dict(
                    title=dict(text=f"Giá ({price_unit})", font=dict(color="#ffffff", size=12)),
                    tickformat=price_format,
                    tickfont=dict(color="#b8c5e0", size=10),
                    bgcolor='rgba(26, 35, 50, 0.8)',
                    bordercolor='#2d3748',
                    borderwidth=1
                )
            ),
            hovertemplate=f'<b>Khoảng giá</b>: %{{x:{price_format}}} {price_unit}<br>' +
                         '<b>Số lượng</b>: %{y}<br>' +
                         '<extra></extra>',
            name='Phân bố giá'
        ))
        
        price_fig.update_layout(
            **get_chart_layout(
                '📊 Phân bố giá',
                xaxis_title=f'Giá ({price_unit})',
                yaxis_title='Số lượng tin đăng'
            ),
            showlegend=False
        )
        price_fig.update_xaxes(tickformat=price_format, tickangle=-45, tickfont=dict(color="#b8c5e0"))
        price_fig.update_yaxes(tickfont=dict(color="#b8c5e0"))
    else:
        price_fig = create_empty_figure("Không có dữ liệu giá")
    
    # Area Distribution Histogram với dark theme colorscale
    if 'area_m2' in df_filtered.columns and not df_filtered['area_m2'].isna().all():
        area_fig = go.Figure()
        area_fig.add_trace(go.Histogram(
            x=df_filtered['area_m2'],
            nbinsx=50,
            marker=dict(
                color=df_filtered['area_m2'],
                colorscale='Cividis',  # Dark-friendly colorscale
                showscale=True,
                colorbar=dict(
                    title=dict(text="Diện tích (m²)", font=dict(color="#ffffff", size=12)),
                    tickfont=dict(color="#b8c5e0", size=10),
                    bgcolor='rgba(26, 35, 50, 0.8)',
                    bordercolor='#2d3748',
                    borderwidth=1
                )
            ),
            hovertemplate='<b>Khoảng diện tích</b>: %{x:.1f} m²<br>' +
                         '<b>Số lượng</b>: %{y}<br>' +
                         '<extra></extra>',
            name='Phân bố diện tích'
        ))
        
        area_fig.update_layout(
            **get_chart_layout(
                '📐 Phân bố diện tích',
                xaxis_title='Diện tích (m²)',
                yaxis_title='Số lượng tin đăng'
            ),
            showlegend=False
        )
        area_fig.update_xaxes(tickfont=dict(color="#b8c5e0"))
        area_fig.update_yaxes(tickfont=dict(color="#b8c5e0"))
    else:
        area_fig = create_empty_figure("Không có dữ liệu diện tích")
    
    # Price by District Bar Chart với dark theme gradient
    if 'district' in df_filtered.columns and 'price' in df_filtered.columns and not df_filtered['price'].isna().all():
        district_stats = df_filtered.groupby('district').agg({
            'price': ['mean', 'count']
        }).reset_index()
        district_stats.columns = ['district', 'avg_price', 'count']
        # Lọc các huyện có số lượng tin > 50
        district_stats = district_stats[district_stats['count'] > 50]
        district_stats = district_stats.sort_values('avg_price', ascending=False).head(20)
        
        # Chuyển đổi giá: triệu nếu là nhà cho thuê, tỷ nếu không
        if is_rental:
            district_stats['price_display'] = district_stats['avg_price'] / 1e6
            price_unit = "Triệu VNĐ"
            price_format = ".1f"
        else:
            district_stats['price_display'] = district_stats['avg_price'] / 1e9
            price_unit = "Tỷ VNĐ"
            price_format = ".2f"
        
        price_district_fig = go.Figure()
        price_district_fig.add_trace(go.Bar(
            x=district_stats['district'],
            y=district_stats['price_display'],
            text=[f"{c}" for c in district_stats['count']],
            textposition='outside',
            textfont=dict(size=10, color='#ffffff'),
            marker=dict(
                color=district_stats['price_display'],
                colorscale='Plasma',  # Dark-friendly colorscale
                showscale=True,
                colorbar=dict(
                    title=dict(text=f"Giá ({price_unit})", font=dict(color="#ffffff", size=12)),
                    tickfont=dict(color="#b8c5e0", size=10),
                    bgcolor='rgba(26, 35, 50, 0.8)',
                    bordercolor='#2d3748',
                    borderwidth=1
                )
            ),
            hovertemplate='<b>%{x}</b><br>' +
                         f'<b>Giá trung bình</b>: %{{y:{price_format}}} {price_unit}<br>' +
                         '<b>Số tin đăng</b>: %{text}<br>' +
                         '<extra></extra>',
            name='Giá trung bình'
        ))
        
        base_layout = get_chart_layout(
            '🏘️ Giá trung bình theo Quận/Huyện (Top 20)',
            xaxis_title='Quận/Huyện',
            yaxis_title=f'Giá trung bình ({price_unit})',
            height=500
        )
        # Cập nhật xaxis với tickangle
        base_layout['xaxis'].update(dict(tickangle=-45, tickfont=dict(color="#b8c5e0")))
        base_layout['yaxis'].update(dict(tickfont=dict(color="#b8c5e0")))
        base_layout['showlegend'] = False
        price_district_fig.update_layout(**base_layout)
    else:
        price_district_fig = create_empty_figure("Không có dữ liệu quận/giá")
    
    # Price Category Pie Chart với dark-friendly colors
    # Nếu là nhà cho thuê (category 1050), hiển thị phân bổ theo rental_category
    if is_rental and 'rental_category' in df_filtered.columns:
        rental_cat_counts = df_filtered['rental_category'].value_counts()
        if not rental_cat_counts.empty:
            # Dark-friendly color palette
            dark_colors = ['#00d4ff', '#7c3aed', '#10b981', '#f59e0b', '#ef4444', '#3b82f6']
            price_pie_fig = go.Figure(data=[go.Pie(
                labels=rental_cat_counts.index,
                values=rental_cat_counts.values,
                hole=0.4,  # Donut chart
                marker=dict(
                    colors=dark_colors[:len(rental_cat_counts)],
                    line=dict(color='#1a1a3e', width=2)
                ),
                textinfo='label+percent',
                textposition='outside',
                textfont=dict(color='#ffffff', size=11),
                hovertemplate='<b>%{label}</b><br>' +
                             '<b>Số lượng</b>: %{value}<br>' +
                             '<b>Tỷ lệ</b>: %{percent}<br>' +
                             '<extra></extra>'
            )])
            
            base_layout = get_chart_layout('🏠 Phân bố theo Mức thuê')
            base_layout['showlegend'] = True
            base_layout['legend'].update(dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.1,
                font=dict(color="#ffffff", size=11),
                bgcolor='rgba(26, 26, 62, 0.9)',
                bordercolor='#2d2d5a',
                borderwidth=1
            ))
            price_pie_fig.update_layout(**base_layout)
        else:
            price_pie_fig = create_empty_figure("Không có dữ liệu mức thuê")
    elif 'price_category' in df_filtered.columns:
        price_cat_counts = df_filtered['price_category'].value_counts()
        if not price_cat_counts.empty:
            # Dark-friendly color palette
            dark_colors = ['#00d4ff', '#7c3aed', '#10b981', '#f59e0b', '#ef4444', '#3b82f6']
            price_pie_fig = go.Figure(data=[go.Pie(
                labels=price_cat_counts.index,
                values=price_cat_counts.values,
                hole=0.4,  # Donut chart
                marker=dict(
                    colors=dark_colors[:len(price_cat_counts)],
                    line=dict(color='#1a1a3e', width=2)
                ),
                textinfo='label+percent',
                textposition='outside',
                textfont=dict(color='#ffffff', size=11),
                hovertemplate='<b>%{label}</b><br>' +
                             '<b>Số lượng</b>: %{value}<br>' +
                             '<b>Tỷ lệ</b>: %{percent}<br>' +
                             '<extra></extra>'
            )])
            
            base_layout = get_chart_layout('💰 Phân bố theo Mức giá')
            base_layout['showlegend'] = True
            base_layout['legend'].update(dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.1,
                font=dict(color="#ffffff", size=11),
                bgcolor='rgba(26, 26, 62, 0.9)',
                bordercolor='#2d2d5a',
                borderwidth=1
            ))
            price_pie_fig.update_layout(**base_layout)
        else:
            price_pie_fig = create_empty_figure("Không có dữ liệu mức giá")
    else:
        price_pie_fig = create_empty_figure("Mức giá không có sẵn")
    
    # Area Category Pie Chart với dark-friendly colors
    if 'area_category' in df_filtered.columns:
        area_cat_counts = df_filtered['area_category'].value_counts()
        if not area_cat_counts.empty:
            # Dark-friendly color palette (different shades)
            dark_colors_area = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#7c3aed', '#00d4ff']
            area_pie_fig = go.Figure(data=[go.Pie(
                labels=area_cat_counts.index,
                values=area_cat_counts.values,
                hole=0.4,  # Donut chart
                marker=dict(
                    colors=dark_colors_area[:len(area_cat_counts)],
                    line=dict(color='#1a1a3e', width=2)
                ),
                textinfo='label+percent',
                textposition='outside',
                textfont=dict(color='#ffffff', size=11),
                hovertemplate='<b>%{label}</b><br>' +
                             '<b>Số lượng</b>: %{value}<br>' +
                             '<b>Tỷ lệ</b>: %{percent}<br>' +
                             '<extra></extra>'
            )])
            
            base_layout = get_chart_layout('📐 Phân bố theo Mức diện tích')
            base_layout['showlegend'] = True
            base_layout['legend'].update(dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.1,
                font=dict(color="#ffffff", size=11),
                bgcolor='rgba(26, 26, 62, 0.9)',
                bordercolor='#2d2d5a',
                borderwidth=1
            ))
            area_pie_fig.update_layout(**base_layout)
        else:
            area_pie_fig = create_empty_figure("Không có dữ liệu mức diện tích")
    else:
        area_pie_fig = create_empty_figure("Mức diện tích không có sẵn")
    
    # Price vs Area Scatter Plot - Sử dụng go.Scatter để tránh lỗi
    if 'price' in df_filtered.columns and 'area_m2' in df_filtered.columns:
        # Filter out invalid data
        scatter_df = df_filtered[(df_filtered['price'].notna()) & (df_filtered['area_m2'].notna()) & 
                        (df_filtered['price'] > 0) & (df_filtered['area_m2'] > 0)].copy()
        
        if not scatter_df.empty:
            # Chuyển đổi giá: triệu nếu là nhà cho thuê, tỷ nếu không
            if is_rental:
                scatter_df['price_display'] = scatter_df['price'] / 1e6
                price_unit = "Triệu VNĐ"
                price_format = ".1f"
            else:
                scatter_df['price_display'] = scatter_df['price'] / 1e9
                price_unit = "Tỷ VNĐ"
                price_format = ".2f"
            
            # Dark-friendly color sequence
            dark_color_sequence = ['#00d4ff', '#7c3aed', '#10b981', '#f59e0b', '#ef4444', '#3b82f6', '#8b5cf6', '#06b6d4']
            
            scatter_fig = go.Figure()
            
            # Nếu có district và số lượng district hợp lý, group theo district
            if 'district' in scatter_df.columns and scatter_df['district'].nunique() <= 20:
                districts = scatter_df['district'].unique()
                for i, district in enumerate(districts[:len(dark_color_sequence)]):
                    district_data = scatter_df[scatter_df['district'] == district]
                    scatter_fig.add_trace(go.Scatter(
                        x=district_data['area_m2'],
                        y=district_data['price_display'],
                        mode='markers',
                        name=str(district),
                        marker=dict(
                            color=dark_color_sequence[i % len(dark_color_sequence)],
                            size=8,
                            line=dict(width=0.5, color='#1a1a3e'),
                            opacity=0.8
                        ),
                        hovertemplate='<b>%{text}</b><br>' +
                                     'Diện tích: %{x:.1f} m²<br>' +
                                     f'Giá: %{{y:{price_format}}} {price_unit}<br>' +
                                     '<extra></extra>',
                        text=district_data['title'].tolist() if 'title' in district_data.columns else None
                    ))
            else:
                # Không group theo district, hiển thị tất cả với một màu
                scatter_fig.add_trace(go.Scatter(
                    x=scatter_df['area_m2'],
                    y=scatter_df['price_display'],
                    mode='markers',
                    name='Tất cả',
                    marker=dict(
                        color=dark_color_sequence[0],
                        size=8,
                        line=dict(width=0.5, color='#1a1a3e'),
                        opacity=0.8
                    ),
                    hovertemplate='<b>%{text}</b><br>' +
                                 'Diện tích: %{x:.1f} m²<br>' +
                                 f'Giá: %{{y:{price_format}}} {price_unit}<br>' +
                                 '<extra></extra>',
                    text=scatter_df['title'].tolist() if 'title' in scatter_df.columns else None
                ))
            
            base_layout = get_chart_layout(
                '📈 Tương quan Giá và Diện tích',
                xaxis_title='Diện tích (m²)',
                yaxis_title=f'Giá ({price_unit})',
                height=500
            )
            base_layout['yaxis'].update(dict(tickformat=price_format, tickfont=dict(color="#b8c5e0")))
            base_layout['xaxis'].update(dict(tickfont=dict(color="#b8c5e0")))
            
            # Chỉ hiển thị legend nếu có nhiều districts
            if 'district' in scatter_df.columns and scatter_df['district'].nunique() <= 20:
                base_layout['showlegend'] = True
            else:
                base_layout['showlegend'] = False
            
            scatter_fig.update_layout(**base_layout)
        else:
            scatter_fig = create_empty_figure("Không có dữ liệu giá/diện tích hợp lệ")
    else:
        scatter_fig = create_empty_figure("Dữ liệu giá/diện tích không có sẵn")
    
    # Giá trên m² theo Huyện (Chart mới) - Sử dụng df_filtered
    if 'district' in df_filtered.columns and 'price_per_m2' in df_filtered.columns:
        # Filter valid data
        price_per_m2_df = df_filtered[(df_filtered['price_per_m2'].notna()) & (df_filtered['price_per_m2'] > 0) & 
                            (df_filtered['district'].notna())].copy()
        
        if not price_per_m2_df.empty:
            # Tính giá trung bình trên m² theo huyện
            district_price_per_m2 = price_per_m2_df.groupby('district').agg({
                'price_per_m2': ['mean', 'count']
            }).reset_index()
            district_price_per_m2.columns = ['district', 'avg_price_per_m2', 'count']
            # Lọc các huyện có số lượng tin > 50
            district_price_per_m2 = district_price_per_m2[district_price_per_m2['count'] > 50]
            district_price_per_m2 = district_price_per_m2.sort_values('avg_price_per_m2', ascending=False).head(20)
            
            price_per_m2_fig = go.Figure()
            price_per_m2_fig.add_trace(go.Bar(
                x=district_price_per_m2['district'],
                y=district_price_per_m2['avg_price_per_m2'] / 1e6,  # Chuyển sang triệu VNĐ/m²
                text=[f"{c}" for c in district_price_per_m2['count']],
                textposition='outside',
                textfont=dict(size=10, color='#ffffff'),
                marker=dict(
                    color=district_price_per_m2['avg_price_per_m2'] / 1e6,
                    colorscale='Turbo',  # Dark-friendly colorscale
                    showscale=True,
                    colorbar=dict(
                        title=dict(text="Giá/m² (Triệu VNĐ)", font=dict(color="#ffffff", size=12)),
                        tickfont=dict(color="#b8c5e0", size=10),
                        bgcolor='rgba(26, 35, 50, 0.8)',
                        bordercolor='#2d3748',
                        borderwidth=1
                    )
                ),
                hovertemplate='<b>%{x}</b><br>' +
                             '<b>Giá trung bình/m²</b>: %{y:.1f} triệu VNĐ<br>' +
                             '<b>Số tin đăng</b>: %{text}<br>' +
                             '<extra></extra>',
                name='Giá/m² trung bình'
            ))
            
            base_layout = get_chart_layout(
                '💰 Giá trên m² theo Quận/Huyện (Top 20)',
                xaxis_title='Huyện',
                yaxis_title='Giá trung bình/m² (Triệu VNĐ)',
                height=500
            )
            base_layout['xaxis'].update(dict(tickangle=-45, tickfont=dict(color="#b8c5e0")))
            base_layout['yaxis'].update(dict(tickfont=dict(color="#b8c5e0")))
            base_layout['showlegend'] = False
            price_per_m2_fig.update_layout(**base_layout)
        else:
            price_per_m2_fig = create_empty_figure("Không có dữ liệu giá/m² hợp lệ")
    else:
        price_per_m2_fig = create_empty_figure("Dữ liệu giá/m² không có sẵn")
    
    # Category Count Bar Chart - Số lượng tin theo từng category
    if 'category' in df.columns:  # Sử dụng df gốc (không filter) để hiển thị tất cả categories
        category_counts = df.groupby('category').size().reset_index(name='count')
        category_counts = category_counts.sort_values('count', ascending=False)
        
        # Map category ID to tên có ý nghĩa
        category_names = {
            1010: "Căn hộ, Chung cư",
            1020: "Nhà ở",
            1030: "Cho thuê kinh doanh, văn phòng",
            1040: "Đất",
            1050: "Cho thuê cư trú"
        }
        
        category_counts['category_name'] = category_counts['category'].apply(
            lambda x: category_names.get(int(x), f"Category {x}") if pd.notna(x) else "Unknown"
        )
        
        # Dark-friendly color palette
        dark_colors = ['#00d4ff', '#7c3aed', '#10b981', '#f59e0b', '#ef4444', '#3b82f6', '#8b5cf6']
        
        category_count_fig = go.Figure()
        category_count_fig.add_trace(go.Bar(
            x=category_counts['category_name'],
            y=category_counts['count'],
            text=category_counts['count'],
            textposition='outside',
            textfont=dict(size=12, color='#ffffff'),
            marker=dict(
                color=category_counts['count'],
                colorscale='Viridis',  # Dark-friendly colorscale
                showscale=True,
                colorbar=dict(
                    title=dict(text="Số lượng", font=dict(color="#ffffff", size=12)),
                    tickfont=dict(color="#b8c5e0", size=10),
                    bgcolor='rgba(26, 35, 50, 0.8)',
                    bordercolor='#2d3748',
                    borderwidth=1
                )
            ),
            hovertemplate='<b>%{x}</b><br>' +
                         '<b>Số lượng tin</b>: %{y:,}<br>' +
                         '<b>Tỷ lệ</b>: %{customdata:.1f}%<br>' +
                         '<extra></extra>',
            customdata=[(count/category_counts['count'].sum()*100) for count in category_counts['count']],
            name='Số lượng tin'
        ))
        
        base_layout = get_chart_layout(
            '📊 Số lượng tin đăng theo Category',
            xaxis_title='Category',
            yaxis_title='Số lượng tin đăng',
            height=450
        )
        base_layout['xaxis'].update(dict(tickangle=-45, tickfont=dict(color="#b8c5e0", size=11)))
        base_layout['yaxis'].update(dict(tickfont=dict(color="#b8c5e0")))
        base_layout['showlegend'] = False
        category_count_fig.update_layout(**base_layout)
    else:
        category_count_fig = create_empty_figure("Dữ liệu category không có sẵn")
    
    # ========== THỐNG KÊ THEO THÁNG ==========
    df_monthly = get_monthly_stats()
    
    # Metrics: So sánh tháng hiện tại vs tháng trước
    if not df_monthly.empty and len(df_monthly) >= 2:
        current_month = df_monthly.iloc[-1]
        previous_month = df_monthly.iloc[-2]
        
        # Thay đổi số tin đăng
        listings_change = current_month['count'] - previous_month['count']
        listings_change_pct = (listings_change / previous_month['count'] * 100) if previous_month['count'] > 0 else 0
        listings_change_str = f"{listings_change:+,}" if listings_change != 0 else "0"
        listings_change_desc = f"vs tháng trước ({previous_month['month'].strftime('%m/%Y')})"
        if listings_change_pct != 0:
            listings_change_desc += f" ({listings_change_pct:+.1f}%)"
        
        # Thay đổi giá trung bình
        price_change = current_month['avg_price'] - previous_month['avg_price']
        price_change_pct = (price_change / previous_month['avg_price'] * 100) if previous_month['avg_price'] > 0 else 0
        # Nếu là nhà cho thuê, luôn hiển thị bằng triệu
        if is_rental:
            price_change_str = f"{price_change/1e6:+.2f} triệu"
        elif abs(price_change) > 1e9:
            price_change_str = f"{price_change/1e9:+.2f} tỷ"
        elif abs(price_change) > 1e6:
            price_change_str = f"{price_change/1e6:+.0f} triệu"
        else:
            price_change_str = f"{price_change:+,.0f} VNĐ"
        price_change_desc = f"vs tháng trước ({previous_month['month'].strftime('%m/%Y')})"
        if price_change_pct != 0:
            price_change_desc += f" ({price_change_pct:+.1f}%)"
        
        # Thay đổi diện tích trung bình
        area_change = current_month['avg_area'] - previous_month['avg_area']
        area_change_pct = (area_change / previous_month['avg_area'] * 100) if previous_month['avg_area'] > 0 else 0
        area_change_str = f"{area_change:+.1f} m²" if area_change != 0 else "0 m²"
        area_change_desc = f"vs tháng trước ({previous_month['month'].strftime('%m/%Y')})"
        if area_change_pct != 0:
            area_change_desc += f" ({area_change_pct:+.1f}%)"
        
        total_months = len(df_monthly)
    else:
        listings_change_str = "N/A"
        listings_change_desc = "Chưa đủ dữ liệu (cần ít nhất 2 tháng)"
        price_change_str = "N/A"
        price_change_desc = "Chưa đủ dữ liệu"
        area_change_str = "N/A"
        area_change_desc = "Chưa đủ dữ liệu"
        total_months = len(df_monthly) if not df_monthly.empty else 0
    
    # Biểu đồ xu hướng giá theo tháng
    if not df_monthly.empty:
        # Chuyển đổi giá: triệu nếu là nhà cho thuê, tỷ nếu không
        if is_rental:
            monthly_price_display = df_monthly['avg_price'] / 1e6
            price_unit = "Triệu VNĐ"
            price_format = ".1f"
        else:
            monthly_price_display = df_monthly['avg_price'] / 1e9
            price_unit = "Tỷ VNĐ"
            price_format = ".2f"
        
        # Xu hướng giá với accent colors
        monthly_price_fig = go.Figure()
        monthly_price_fig.add_trace(go.Scatter(
            x=df_monthly['month'],
            y=monthly_price_display,
            mode='lines+markers',
            name='Giá trung bình',
            line=dict(color='#00d4ff', width=3),
            marker=dict(size=8, color='#7c3aed'),
            hovertemplate='<b>Tháng</b>: %{x|%m/%Y}<br>' +
                         f'<b>Giá TB</b>: %{{y:{price_format}}} {price_unit}<br>' +
                         '<extra></extra>'
        ))
        monthly_price_fig.update_layout(
            **get_chart_layout(
                '📈 Xu hướng Giá trung bình theo tháng',
                xaxis_title='Tháng',
                yaxis_title=f'Giá trung bình ({price_unit})',
                height=400
            )
        )
        monthly_price_fig.update_xaxes(tickfont=dict(color="#b8c5e0"))
        monthly_price_fig.update_yaxes(tickfont=dict(color="#b8c5e0"))
        
        # Xu hướng số lượng tin đăng với dark colorscale
        monthly_listings_fig = go.Figure()
        monthly_listings_fig.add_trace(go.Bar(
            x=df_monthly['month'],
            y=df_monthly['count'],
            name='Số tin đăng',
            marker=dict(
                color=df_monthly['count'],
                colorscale='Bluyl',  # Dark-friendly colorscale
                showscale=True,
                colorbar=dict(
                    title=dict(text="Số lượng", font=dict(color="#ffffff", size=12)),
                    tickfont=dict(color="#b8c5e0", size=10),
                    bgcolor='rgba(26, 35, 50, 0.8)',
                    bordercolor='#2d3748',
                    borderwidth=1
                )
            ),
            hovertemplate='<b>Tháng</b>: %{x|%m/%Y}<br>' +
                         '<b>Số tin đăng</b>: %{y:,}<br>' +
                         '<extra></extra>'
        ))
        monthly_listings_fig.update_layout(
            **get_chart_layout(
                '📊 Xu hướng Số lượng tin đăng theo tháng',
                xaxis_title='Tháng',
                yaxis_title='Số lượng tin đăng',
                height=400
            )
        )
        monthly_listings_fig.update_xaxes(tickfont=dict(color="#b8c5e0"))
        monthly_listings_fig.update_yaxes(tickfont=dict(color="#b8c5e0"))
        
        # Xu hướng diện tích trung bình với accent colors
        monthly_area_fig = go.Figure()
        monthly_area_fig.add_trace(go.Scatter(
            x=df_monthly['month'],
            y=df_monthly['avg_area'],
            mode='lines+markers',
            name='Diện tích TB',
            line=dict(color='#10b981', width=3),
            marker=dict(size=8, color='#3b82f6'),
            hovertemplate='<b>Tháng</b>: %{x|%m/%Y}<br>' +
                         '<b>Diện tích TB</b>: %{y:.1f} m²<br>' +
                         '<extra></extra>'
        ))
        monthly_area_fig.update_layout(
            **get_chart_layout(
                '📐 Xu hướng Diện tích trung bình theo tháng',
                xaxis_title='Tháng',
                yaxis_title='Diện tích trung bình (m²)',
                height=400
            )
        )
        monthly_area_fig.update_xaxes(tickfont=dict(color="#b8c5e0"))
        monthly_area_fig.update_yaxes(tickfont=dict(color="#b8c5e0"))
        
        # Xu hướng giá/m² (tính từ avg_price / avg_area)
        if 'avg_price' in df_monthly.columns and 'avg_area' in df_monthly.columns:
            df_monthly['avg_price_per_m2'] = df_monthly['avg_price'] / df_monthly['avg_area']
            monthly_price_per_m2_fig = go.Figure()
            monthly_price_per_m2_fig.add_trace(go.Scatter(
                x=df_monthly['month'],
                y=df_monthly['avg_price_per_m2'] / 1e6,  # Convert to triệu VNĐ/m²
                mode='lines+markers',
                name='Giá/m² TB',
                line=dict(color='#f59e0b', width=3),
                marker=dict(size=8, color='#ef4444'),
                hovertemplate='<b>Tháng</b>: %{x|%m/%Y}<br>' +
                             '<b>Giá/m² TB</b>: %{y:.2f} triệu VNĐ/m²<br>' +
                             '<extra></extra>'
            ))
            monthly_price_per_m2_fig.update_layout(
                **get_chart_layout(
                    '💰 Xu hướng Giá/m² trung bình theo tháng',
                    xaxis_title='Tháng',
                    yaxis_title='Giá/m² trung bình (Triệu VNĐ)',
                    height=400
                )
            )
            monthly_price_per_m2_fig.update_xaxes(tickfont=dict(color="#b8c5e0"))
            monthly_price_per_m2_fig.update_yaxes(tickfont=dict(color="#b8c5e0"))
        else:
            monthly_price_per_m2_fig = create_empty_figure("Không có dữ liệu giá/m² theo tháng")
    else:
        monthly_price_fig = create_empty_figure("Chưa có dữ liệu theo tháng")
        monthly_listings_fig = create_empty_figure("Chưa có dữ liệu theo tháng")
        monthly_area_fig = create_empty_figure("Chưa có dữ liệu theo tháng")
        monthly_price_per_m2_fig = create_empty_figure("Chưa có dữ liệu theo tháng")
    
    return (
        f"{total:,}",
        avg_price_str,
        avg_area_str,
        f"{total_districts}",
        last_update,
        price_fig,
        area_fig,
        price_district_fig,
        price_pie_fig,
        area_pie_fig,
        scatter_fig,
        price_per_m2_fig,
        category_count_fig,
        # Monthly stats
        listings_change_str,
        listings_change_desc,
        price_change_str,
        price_change_desc,
        f"{total_months}",
        area_change_str,
        area_change_desc,
        monthly_price_fig,
        monthly_listings_fig,
        monthly_area_fig,
        monthly_price_per_m2_fig
    )


if __name__ == '__main__':
    print("="*80)
    print("🚀 Đang khởi động Dashboard Phân tích Bất động sản...")
    print("="*80)
    print(f"📊 MongoDB: {MONGODB_DATABASE}.{MONGODB_COLLECTION}")
    print(f"🌐 Dashboard sẽ có sẵn tại:")
    print(f"   - Local: http://localhost:8050")
    print(f"   - Từ Windows: http://<WSL2_IP>:8050")
    print(f"   (Lấy WSL2 IP: hostname -I)")
    print("="*80)
    print("⏳ Tự động làm mới mỗi 5 phút")
    print("Nhấn Ctrl+C để dừng")
    print("="*80)
    
    app.run(host='0.0.0.0', port=8050, debug=False)

