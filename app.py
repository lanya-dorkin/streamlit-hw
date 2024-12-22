import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import aiohttp
import asyncio
import requests
from concurrent.futures import ProcessPoolExecutor
import time

st.set_page_config(
    page_title="Анализ температурных данных",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data(uploaded_file):
    """
    Загрузка и кэширование данных из CSV файла
    """
    data = pd.read_csv(uploaded_file)
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

def calculate_moving_average(data, window=30):
    """Вычисление скользящего среднего"""
    return data['temperature'].rolling(window=window).mean()

def calculate_anomalies(data, window=30):
    """Поиск аномалий на основе скользящего среднего и стандартного отклонения"""
    rolling_mean = calculate_moving_average(data, window)
    rolling_std = data['temperature'].rolling(window=window).std()
    
    upper_bound = rolling_mean + 2 * rolling_std
    lower_bound = rolling_mean - 2 * rolling_std
    
    anomalies = data[
        (data['temperature'] > upper_bound) | 
        (data['temperature'] < lower_bound)
    ].copy()
    
    return anomalies, rolling_mean, upper_bound, lower_bound

async def get_weather_async(city, api_key):
    """Асинхронное получение текущей погоды"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, params=params) as response:
            if response.status == 401:
                st.error("Invalid API key. Please see https://openweathermap.org/faq#error401 for more info.")
                return None
            data = await response.json()
            return data

def get_weather_sync(city, api_key):
    """Синхронное получение текущей погоды"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    
    response = requests.get(base_url, params=params)
    if response.status_code == 401:
        st.error("Неверный API ключ")
        return None
    return response.json()

def analyze_data_parallel(data):
    """Параллельный анализ данных для всех городов с использованием ProcessPoolExecutor"""
    groups = data.groupby('city')
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = executor.map(calculate_anomalies, [group for name, group in groups])
        return {name: result for name, result in zip(groups.groups.keys(), results)}

def analyze_data_sequential(data):
    """Последовательный анализ данных для всех городов с использованием groupby и apply"""
    return data.groupby('city').apply(calculate_anomalies).to_dict()

def compare_execution_times(data):
    """Сравнение времени выполнения параллельного и последовательного анализа"""
    start_time = time.time()
    _ = analyze_data_sequential(data)
    sequential_time = time.time() - start_time

    start_time = time.time()
    _ = analyze_data_parallel(data)
    parallel_time = time.time() - start_time

    return sequential_time, parallel_time

def main():
    st.title("🌡️ Анализ температурных данных")
    st.markdown("---")

    uploaded_file = st.file_uploader("Загрузите файл с температурными данными (CSV)", type=['csv'])
    
    if uploaded_file is not None:
        data = load_data(uploaded_file)

        with st.sidebar:
            st.markdown("## ⚙️ Настройки")
            st.markdown("---")
            
            if st.checkbox("📊 Показать сравнение времени выполнения"):
                sequential_time, parallel_time = compare_execution_times(data)
                
                st.markdown("### 🚀 Сравнение производительности (все города)")
                perf_col1, perf_col2 = st.columns(2)
                perf_col1.metric("Последовательно", f"{sequential_time:.2f} сек")
                perf_col2.metric("Параллельно", f"{parallel_time:.2f} сек")
                
                speedup = sequential_time / parallel_time
                st.info(f"⚡ Ускорение: {speedup:.2f}x")
                st.markdown("---")

            selected_city = st.selectbox("🌍 Выберите город", sorted(data['city'].unique()))
            api_key = st.text_input("🔑 API ключ OpenWeatherMap", type="password")
            
            if api_key:
                st.markdown("### 🔄 Метод запроса")
                use_async = st.checkbox("Использовать асинхронный запрос", value=True)

        st.markdown("### 📈 Статистика по историческим данным")
        city_data = data[data['city'] == selected_city]
        
        stats = city_data.groupby('season')['temperature'].agg(['mean', 'std', 'min', 'max']).round(2)
        stats.columns = ['Среднее', 'Стд. откл.', 'Минимум', 'Максимум']
        st.dataframe(stats, use_container_width=True)
        
        anomalies, rolling_mean, upper_bound, lower_bound = calculate_anomalies(city_data)
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=city_data['timestamp'],
            y=city_data['temperature'],
            name='Температура',
            mode='lines',
            line=dict(color='#2E86AB', width=1)
        ))
        
        fig.add_trace(go.Scatter(
            x=city_data['timestamp'],
            y=rolling_mean,
            name='Скользящее среднее (30 дней)',
            line=dict(color='#F24236', width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=anomalies['timestamp'],
            y=anomalies['temperature'],
            mode='markers',
            name='Аномалии',
            marker=dict(color='#F24236', size=8, symbol='diamond')
        ))

        fig.add_trace(go.Scatter(
            x=city_data['timestamp'],
            y=upper_bound,
            name='Верхняя граница',
            line=dict(color='#95A5A6', width=1, dash='dash')
        ))
        
        fig.add_trace(go.Scatter(
            x=city_data['timestamp'],
            y=lower_bound,
            name='Нижняя граница',
            line=dict(color='#95A5A6', width=1, dash='dash')
        ))
        
        fig.update_layout(
            title={
                'text': 'Временной ряд температур и аномалии',
                'y':0.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            xaxis_title='Дата',
            yaxis_title='Температура (°C)',
            height=500,
            template='plotly_dark',
            hovermode='x unified',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            margin=dict(l=60, r=30, t=50, b=50)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### 🌡️ Сезонный профиль")
        
        seasonal_stats = city_data.groupby('season')['temperature'].agg(['mean', 'std']).reset_index()
        seasons_order = ['winter', 'spring', 'summer', 'autumn']
        seasonal_stats['season'] = pd.Categorical(seasonal_stats['season'], categories=seasons_order, ordered=True)
        seasonal_stats = seasonal_stats.sort_values('season')
        
        seasons_translate = {
            'winter': 'Зима',
            'spring': 'Весна',
            'summer': 'Лето',
            'autumn': 'Осень'
        }
        seasonal_stats['season_ru'] = seasonal_stats['season'].map(seasons_translate)
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=seasonal_stats['season_ru'],
            y=seasonal_stats['mean'],
            name='Среднее',
            mode='lines+markers',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=10)
        ))
        
        fig.add_trace(go.Scatter(
            x=seasonal_stats['season_ru'],
            y=seasonal_stats['mean'] + 2*seasonal_stats['std'],
            name='Верхняя граница (2σ)',
            line=dict(color='#95A5A6', width=2, dash='dash')
        ))
        
        fig.add_trace(go.Scatter(
            x=seasonal_stats['season_ru'],
            y=seasonal_stats['mean'] - 2*seasonal_stats['std'],
            name='Нижняя граница (2σ)',
            line=dict(color='#95A5A6', width=2, dash='dash'),
            fill='tonexty'
        ))
        
        fig.update_layout(
            title={
                'text': 'Сезонный профиль температур',
                'y':0.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            xaxis_title='Сезон',
            yaxis_title='Температура (°C)',
            height=500,
            template='plotly_dark',
            hovermode='x unified',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            margin=dict(l=60, r=30, t=50, b=50)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        if api_key:
            st.markdown("### 🌡️ Текущая температура")
            
            try:
                if use_async:
                    weather_data = asyncio.run(get_weather_async(selected_city, api_key))
                else:
                    weather_data = get_weather_sync(selected_city, api_key)
                
                if weather_data:
                    if 'cod' in weather_data and weather_data['cod'] == 401:
                        st.error('{"cod":401, "message": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info."}')
                    elif 'main' in weather_data:
                        current_temp = weather_data['main']['temp']
                        current_month = datetime.now().month
                        current_season = {12: 'winter', 1: 'winter', 2: 'winter',
                                       3: 'spring', 4: 'spring', 5: 'spring',
                                       6: 'summer', 7: 'summer', 8: 'summer',
                                       9: 'autumn', 10: 'autumn', 11: 'autumn'}[current_month]
                        
                        season_stats = stats.loc[current_season]
                        is_anomaly = (current_temp > season_stats['Среднее'] + 2*season_stats['Стд. откл.']) or \
                                   (current_temp < season_stats['Среднее'] - 2*season_stats['Стд. откл.'])
                        
                        st.metric("Текущая температура", f"{current_temp:.1f}°C")
                        
                        if is_anomaly:
                            st.warning("⚠️ Текущая температура является аномальной для этого сезона!")
                        else:
                            st.success("✅ Текущая температура в пределах нормы для этого сезона")
                            
            except Exception as e:
                st.error(f"Ошибка при получении данных о погоде: {str(e)}")

if __name__ == "__main__":
    main() 