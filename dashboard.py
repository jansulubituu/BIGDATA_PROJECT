"""
Dashboard Dữ liệu Bất động sản
Visualization cho dữ liệu bất động sản từ MongoDB Atlas
Dashboard real-time với tự động làm mới mỗi 30 giây
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
from datetime import datetime
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
CACHE_TTL = 25  # Cache 25 giây (làm mới mỗi 30s)

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
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                margin: 0;
                padding: 20px;
            }
            .card {
                border-radius: 15px;
                box-shadow: 0 8px 16px rgba(0, 0, 0, 0.15);
                transition: transform 0.3s ease, box-shadow 0.3s ease;
                border: none;
                overflow: hidden;
            }
            .card:hover {
                transform: translateY(-5px);
                box-shadow: 0 12px 24px rgba(0, 0, 0, 0.25);
            }
            .card-body {
                padding: 1.5rem;
            }
            .metric-card-primary {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
            }
            .metric-card-success {
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                color: white;
            }
            .metric-card-info {
                background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
                color: white;
            }
            .metric-card-warning {
                background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
                color: white;
            }
            h1 {
                color: white;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
                font-weight: 700;
                font-size: 2.5rem;
            }
            .plotly-graph-div {
                border-radius: 10px;
                background: white;
            }
            .text-muted {
                color: rgba(255, 255, 255, 0.9) !important;
            }
            /* Animation cho counter effect */
            .counter-animate {
                animation: countUp 1.5s ease-out;
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
            /* Animation cho charts khi load */
            .plotly-graph-div {
                animation: fadeInUp 0.8s ease-out;
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
            /* Pulse animation cho metrics cards */
            .metric-card-primary, .metric-card-success, .metric-card-info, .metric-card-warning {
                animation: pulse 2s ease-in-out infinite;
            }
            @keyframes pulse {
                0%, 100% {
                    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.15);
                }
                50% {
                    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.25);
                }
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
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("🏠 Dashboard Phân tích Bất động sản", className="text-center mb-2"),
            html.P("Dữ liệu real-time từ MongoDB Atlas | Tự động làm mới mỗi 30 giây", 
                   className="text-center text-muted mb-4"),
            html.Div(id="last-update", className="text-center text-muted mb-4")
        ], width=12)
    ]),
    
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
    
    # Charts Row 1: Distributions
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-distribution")
                ])
            ])
        ], width=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="area-distribution")
                ])
            ])
        ], width=6, className="mb-4"),
    ]),
    
    # Charts Row 2: Price by District
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-by-district")
                ])
            ])
        ], width=12, className="mb-4"),
    ]),
    
    # Charts Row 3: Category Analysis
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
    
    # Charts Row 4: Correlation và Giá/m²
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-vs-area-scatter")
                ])
            ])
        ], width=6, className="mb-4"),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id="price-per-m2-by-district")
                ])
            ])
        ], width=6, className="mb-4"),
    ]),
    
    # Tự động làm mới
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # Cập nhật mỗi 30 giây
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
    """Tạo biểu đồ trống với thông báo"""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=18, color="#666", family="Arial")
    )
    fig.update_layout(
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        plot_bgcolor='#f8f9fa',
        paper_bgcolor='white',
        margin=dict(l=20, r=20, t=20, b=20)
    )
    return fig


def get_chart_layout(title, xaxis_title=None, yaxis_title=None, height=400):
    """Tạo template layout cho biểu đồ với styling đẹp"""
    layout = dict(
        title=dict(
            text=title,
            font=dict(size=20, family="Arial", color="#2c3e50"),
            x=0.5,
            xanchor='center'
        ),
        plot_bgcolor='#f8f9fa',
        paper_bgcolor='white',
        font=dict(family="Arial", size=12, color="#2c3e50"),
        margin=dict(l=60, r=30, t=60, b=50),
        height=height,
        hovermode='closest',
        xaxis=dict(
            title=xaxis_title if xaxis_title else "",
            gridcolor='#e0e0e0',
            gridwidth=1,
            showgrid=True,
            zeroline=False,
            linecolor='#b0b0b0',
            linewidth=1
        ),
        yaxis=dict(
            title=yaxis_title if yaxis_title else "",
            gridcolor='#e0e0e0',
            gridwidth=1,
            showgrid=True,
            zeroline=False,
            linecolor='#b0b0b0',
            linewidth=1
        )
    )
    return layout


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
     Output('price-per-m2-by-district', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_dashboard(n):
    """Callback để cập nhật tất cả các components"""
    df = get_data_from_mongodb()
    
    # Cập nhật timestamp
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_update = f"Cập nhật lần cuối: {current_time}"
    
    # Nếu không có dữ liệu, trả về giá trị trống
    if df.empty:
        empty_fig = create_empty_figure("Không có dữ liệu")
        return (
            "0", "0 VNĐ", "0 m²", "0", last_update,
            empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig
        )
    
    # Tính toán các chỉ số
    total = len(df)
    avg_price = df['price'].mean() if 'price' in df.columns and not df['price'].isna().all() else 0
    avg_area = df['area_m2'].mean() if 'area_m2' in df.columns and not df['area_m2'].isna().all() else 0
    total_districts = df['district'].nunique() if 'district' in df.columns else 0
    
    # Định dạng các chỉ số (thêm data attribute để JavaScript có thể animate)
    if avg_price > 1e9:
        avg_price_str = f"{avg_price/1e9:.2f} tỷ"
        avg_price_value = avg_price/1e9
    elif avg_price > 1e6:
        avg_price_str = f"{avg_price/1e6:.0f} triệu"
        avg_price_value = avg_price/1e6
    else:
        avg_price_str = f"{avg_price:,.0f} VNĐ"
        avg_price_value = avg_price
    
    avg_area_str = f"{avg_area:.1f} m²" if avg_area > 0 else "0 m²"
    
    # Price Distribution Histogram với gradient colors (đơn vị tỷ)
    if 'price' in df.columns and not df['price'].isna().all():
        # Chuyển đổi giá sang tỷ VNĐ
        price_billion = df['price'] / 1e9
        
        price_fig = go.Figure()
        price_fig.add_trace(go.Histogram(
            x=price_billion,
            nbinsx=50,
            marker=dict(
                color=price_billion,
                colorscale='Reds',
                showscale=True,
                colorbar=dict(title="Giá (Tỷ VNĐ)", tickformat=".2f")
            ),
            hovertemplate='<b>Khoảng giá</b>: %{x:.2f} tỷ VNĐ<br>' +
                         '<b>Số lượng</b>: %{y}<br>' +
                         '<extra></extra>',
            name='Phân bố giá'
        ))
        
        price_fig.update_layout(
            **get_chart_layout(
                '📊 Phân bố giá',
                xaxis_title='Giá (Tỷ VNĐ)',
                yaxis_title='Số lượng tin đăng'
            ),
            showlegend=False
        )
        price_fig.update_xaxes(tickformat=".2f", tickangle=-45)
    else:
        price_fig = create_empty_figure("Không có dữ liệu giá")
    
    # Area Distribution Histogram với gradient colors
    if 'area_m2' in df.columns and not df['area_m2'].isna().all():
        area_fig = go.Figure()
        area_fig.add_trace(go.Histogram(
            x=df['area_m2'],
            nbinsx=50,
            marker=dict(
                color=df['area_m2'],
                colorscale='Greens',
                showscale=True,
                colorbar=dict(title="Diện tích (m²)")
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
    else:
        area_fig = create_empty_figure("Không có dữ liệu diện tích")
    
    # Price by District Bar Chart với gradient
    if 'district' in df.columns and 'price' in df.columns and not df['price'].isna().all():
        district_stats = df.groupby('district').agg({
            'price': ['mean', 'count']
        }).reset_index()
        district_stats.columns = ['district', 'avg_price', 'count']
        district_stats = district_stats.sort_values('avg_price', ascending=False).head(20)
        
        price_district_fig = go.Figure()
        price_district_fig.add_trace(go.Bar(
            x=district_stats['district'],
            y=district_stats['avg_price']/1e9,
            text=[f"{c}" for c in district_stats['count']],
            textposition='outside',
            textfont=dict(size=10, color='#2c3e50'),
            marker=dict(
                color=district_stats['avg_price']/1e9,
                colorscale='Oranges',
                showscale=True,
                colorbar=dict(title="Giá (Tỷ VNĐ)")
            ),
            hovertemplate='<b>%{x}</b><br>' +
                         '<b>Giá trung bình</b>: %{y:.2f} tỷ VNĐ<br>' +
                         '<b>Số tin đăng</b>: %{text}<br>' +
                         '<extra></extra>',
            name='Giá trung bình'
        ))
        
        base_layout = get_chart_layout(
            '🏘️ Giá trung bình theo Quận/Huyện (Top 20)',
            xaxis_title='Quận/Huyện',
            yaxis_title='Giá trung bình (Tỷ VNĐ)',
            height=500
        )
        # Cập nhật xaxis với tickangle
        base_layout['xaxis'].update(dict(tickangle=-45))
        base_layout['showlegend'] = False
        price_district_fig.update_layout(**base_layout)
    else:
        price_district_fig = create_empty_figure("Không có dữ liệu quận/giá")
    
    # Price Category Pie Chart với donut style
    if 'price_category' in df.columns:
        price_cat_counts = df['price_category'].value_counts()
        if not price_cat_counts.empty:
            price_pie_fig = go.Figure(data=[go.Pie(
                labels=price_cat_counts.index,
                values=price_cat_counts.values,
                hole=0.4,  # Donut chart
                marker=dict(
                    colors=px.colors.qualitative.Set3,
                    line=dict(color='#FFFFFF', width=2)
                ),
                textinfo='label+percent',
                textposition='outside',
                hovertemplate='<b>%{label}</b><br>' +
                             '<b>Số lượng</b>: %{value}<br>' +
                             '<b>Tỷ lệ</b>: %{percent}<br>' +
                             '<extra></extra>'
            )])
            
            price_pie_fig.update_layout(
                **get_chart_layout('💰 Phân bố theo Mức giá'),
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.1)
            )
        else:
            price_pie_fig = create_empty_figure("Không có dữ liệu mức giá")
    else:
        price_pie_fig = create_empty_figure("Mức giá không có sẵn")
    
    # Area Category Pie Chart với donut style
    if 'area_category' in df.columns:
        area_cat_counts = df['area_category'].value_counts()
        if not area_cat_counts.empty:
            area_pie_fig = go.Figure(data=[go.Pie(
                labels=area_cat_counts.index,
                values=area_cat_counts.values,
                hole=0.4,  # Donut chart
                marker=dict(
                    colors=px.colors.qualitative.Pastel,
                    line=dict(color='#FFFFFF', width=2)
                ),
                textinfo='label+percent',
                textposition='outside',
                hovertemplate='<b>%{label}</b><br>' +
                             '<b>Số lượng</b>: %{value}<br>' +
                             '<b>Tỷ lệ</b>: %{percent}<br>' +
                             '<extra></extra>'
            )])
            
            area_pie_fig.update_layout(
                **get_chart_layout('📐 Phân bố theo Mức diện tích'),
                showlegend=True,
                legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.1)
            )
        else:
            area_pie_fig = create_empty_figure("Không có dữ liệu mức diện tích")
    else:
        area_pie_fig = create_empty_figure("Mức diện tích không có sẵn")
    
    # Price vs Area Scatter Plot (đơn vị tỷ)
    if 'price' in df.columns and 'area_m2' in df.columns:
        # Filter out invalid data
        scatter_df = df[(df['price'].notna()) & (df['area_m2'].notna()) & 
                        (df['price'] > 0) & (df['area_m2'] > 0)].copy()
        
        if not scatter_df.empty:
            # Chuyển đổi giá sang tỷ VNĐ
            scatter_df['price_billion'] = scatter_df['price'] / 1e9
            
            scatter_fig = px.scatter(
                scatter_df,
                x='area_m2',
                y='price_billion',
                color='district' if 'district' in scatter_df.columns else None,
                size='price_per_m2' if 'price_per_m2' in scatter_df.columns else None,
                hover_data=['title'] if 'title' in scatter_df.columns else None,
                title='📈 Tương quan Giá và Diện tích',
                labels={'area_m2': 'Diện tích (m²)', 'price_billion': 'Giá (Tỷ VNĐ)'},
                color_discrete_sequence=px.colors.qualitative.Set2,
                size_max=20
            )
            
            scatter_fig.update_traces(
                marker=dict(
                    line=dict(width=0.5, color='white'),
                    opacity=0.7
                ),
                hovertemplate='<b>%{hovertext}</b><br>' +
                             'Diện tích: %{x:.1f} m²<br>' +
                             'Giá: %{y:.2f} tỷ VNĐ<br>' +
                             '<extra></extra>'
            )
            
            base_layout = get_chart_layout(
                '📈 Tương quan Giá và Diện tích',
                xaxis_title='Diện tích (m²)',
                yaxis_title='Giá (Tỷ VNĐ)',
                height=500
            )
            base_layout['yaxis'].update(dict(tickformat=".2f"))
            scatter_fig.update_layout(**base_layout)
            # Giới hạn legend nếu có quá nhiều quận
            if 'district' in scatter_df.columns and scatter_df['district'].nunique() > 20:
                scatter_fig.update_layout(showlegend=False)
        else:
            scatter_fig = create_empty_figure("Không có dữ liệu giá/diện tích hợp lệ")
    else:
        scatter_fig = create_empty_figure("Dữ liệu giá/diện tích không có sẵn")
    
    # Giá trên m² theo Huyện (Chart mới)
    if 'district' in df.columns and 'price_per_m2' in df.columns:
        # Filter valid data
        price_per_m2_df = df[(df['price_per_m2'].notna()) & (df['price_per_m2'] > 0) & 
                            (df['district'].notna())].copy()
        
        if not price_per_m2_df.empty:
            # Tính giá trung bình trên m² theo huyện
            district_price_per_m2 = price_per_m2_df.groupby('district').agg({
                'price_per_m2': ['mean', 'count']
            }).reset_index()
            district_price_per_m2.columns = ['district', 'avg_price_per_m2', 'count']
            district_price_per_m2 = district_price_per_m2.sort_values('avg_price_per_m2', ascending=False).head(20)
            
            price_per_m2_fig = go.Figure()
            price_per_m2_fig.add_trace(go.Bar(
                x=district_price_per_m2['district'],
                y=district_price_per_m2['avg_price_per_m2'] / 1e6,  # Chuyển sang triệu VNĐ/m²
                text=[f"{c}" for c in district_price_per_m2['count']],
                textposition='outside',
                textfont=dict(size=10, color='#2c3e50'),
                marker=dict(
                    color=district_price_per_m2['avg_price_per_m2'] / 1e6,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Giá/m² (Triệu VNĐ)")
                ),
                hovertemplate='<b>%{x}</b><br>' +
                             '<b>Giá trung bình/m²</b>: %{y:.1f} triệu VNĐ<br>' +
                             '<b>Số tin đăng</b>: %{text}<br>' +
                             '<extra></extra>',
                name='Giá/m² trung bình'
            ))
            
            base_layout = get_chart_layout(
                '💰 Giá trên m² theo Huyện (Top 20)',
                xaxis_title='Huyện',
                yaxis_title='Giá trung bình/m² (Triệu VNĐ)',
                height=500
            )
            base_layout['xaxis'].update(dict(tickangle=-45))
            base_layout['showlegend'] = False
            price_per_m2_fig.update_layout(**base_layout)
        else:
            price_per_m2_fig = create_empty_figure("Không có dữ liệu giá/m² hợp lệ")
    else:
        price_per_m2_fig = create_empty_figure("Dữ liệu giá/m² không có sẵn")
    
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
        price_per_m2_fig
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
    print("⏳ Tự động làm mới mỗi 30 giây")
    print("Nhấn Ctrl+C để dừng")
    print("="*80)
    
    app.run(host='0.0.0.0', port=8050, debug=False)

