import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from astropy.time import Time
import os
import base64

# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'hardware_database')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'admin')
DB_PORT = os.getenv('DB_PORT', 5432)

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    print("Connection successful")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    conn = None

# Convert the image to base64
def convert_image_to_base64(image_path):
    with open(image_path, 'rb') as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode()
    return encoded_string

image_base64 = convert_image_to_base64('C:/Users/User/Downloads/Logo_NPL_india.svg')
image_base64 = f"data:image/svg+xml;base64,{image_base64}"

# Sensor name mapping
SENSOR_NAMES = {
    1: "Sensor A",
    2: "Sensor B"
}

# Function to fetch temperature data and convert timestamp to MJD
def fetch_temperature_data(cur, start_datetime, end_datetime, channels, ma_window_size):
    try:
        temp_query = sql.SQL(
            "SELECT timestamp, temperature_s1, temperature_s2 FROM sensor_data WHERE timestamp BETWEEN %s AND %s ORDER BY timestamp"
        )
        cur.execute(temp_query, (start_datetime, end_datetime))
        temp_data = cur.fetchall()
        temp_df = pd.DataFrame(temp_data, columns=["timestamp", "temperature_s1", "temperature_s2"])

        # Convert timestamp to MJD and format to three decimal places
        temp_df['mjd'] = temp_df['timestamp'].apply(lambda x: "{:.3f}".format(Time(x).mjd))

        return temp_df[['timestamp', 'mjd', 'temperature_s1', 'temperature_s2']]

    except Exception as e:
        print(f"Error fetching temperature data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

# Function to fetch humidity data and convert timestamp to MJD
def fetch_humidity_data(cur, start_datetime, end_datetime, channels, ma_window_size):
    try:
        humidity_query = sql.SQL(
            "SELECT timestamp, humidity_s1, humidity_s2 FROM sensor_data WHERE timestamp BETWEEN %s AND %s ORDER BY timestamp"
        )
        cur.execute(humidity_query, (start_datetime, end_datetime))
        humidity_data = cur.fetchall()
        humidity_df = pd.DataFrame(humidity_data, columns=["timestamp", "humidity_s1", "humidity_s2"])

        # Convert timestamp to MJD and format to three decimal places
        humidity_df['mjd'] = humidity_df['timestamp'].apply(lambda x: "{:.3f}".format(Time(x).mjd))

        return humidity_df[['timestamp', 'mjd', 'humidity_s1', 'humidity_s2']]

    except Exception as e:
        print(f"Error fetching humidity data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = html.Div([
    html.Div([
        html.Img(src=image_base64, style={'width': '150px', 'float': 'left', 'margin-right': '20px'}),
        html.H1("LIVE DATA PLOTTING", style={'display': 'inline-block', 'vertical-align': 'middle'})
    ], style={'margin-bottom': '20px'}),
    
    # Date Picker Range
    dcc.DatePickerRange(
        id='date-picker-range',
        start_date=datetime.now().date(),
        end_date=datetime.now().date(),
        display_format='DD/MM/YYYY'
    ),
    
    # Time Inputs
    dcc.Input(
        id='start-time',
        type='text',
        placeholder='Start time (HH:MM)',
        value='00:00'
    ),
    dcc.Input(
        id='end-time',
        type='text',
        placeholder='End time (HH:MM)',
        value='23:59'
    ),
    
    # Time Range Dropdown
    dcc.Dropdown(
        id='time-range-dropdown',
        options=[
            {'label': 'Last 5 minutes', 'value': 5},
            {'label': 'Last 10 minutes', 'value': 10},
            {'label': 'Last 15 minutes', 'value': 15},
            {'label': 'Last 30 minutes', 'value': 30},
            {'label': 'Last 1 hour', 'value': 60},
        ],
        placeholder='Select time range',
        value=None  # Default value should be None
    ),
    
    # Channel Dropdown
    dcc.Dropdown(
        id='channel-dropdown',
        options=[
            {'label': SENSOR_NAMES[1], 'value': 1},
            {'label': SENSOR_NAMES[2], 'value': 2},
        ],
        value=[1, 2],
        multi=True,
        placeholder='Select sensor(s)'
    ),
    
    # Sensor Name Update Section
    html.Div([
        dcc.Input(id='sensor-name-1', type='text', placeholder='Sensor A', value=SENSOR_NAMES[1]),
        dcc.Input(id='sensor-name-2', type='text', placeholder='Sensor B', value=SENSOR_NAMES[2]),
        html.Button('Update Sensor Names', id='update-sensor-names', n_clicks=0)
    ], style={'margin-top': '20px'}),
    
    # Color Dropdowns
    dcc.Dropdown(
        id='color-dropdown-1',
        options=[
            {'label': 'Red', 'value': 'red'},
            {'label': 'Blue', 'value': 'blue'},
            {'label': 'Green', 'value': 'green'},
            {'label': 'Black', 'value': 'black'},
            {'label': 'Yellow', 'value': 'yellow'}
        ], 
        placeholder='Select color for Sensor A',
        value='red'
    ),
    dcc.Dropdown(
        id='color-dropdown-2',
        options=[
            {'label': 'Red', 'value': 'red'},
            {'label': 'Blue', 'value': 'blue'},
            {'label': 'Green', 'value': 'green'},
            {'label': 'Black', 'value': 'black'},
            {'label': 'Yellow', 'value': 'yellow'}
        ],
        placeholder='Select color for Sensor B',
        value='blue'
    ),
    
    # Moving Average Window Size Input
    html.Div([
        html.Label("Moving Average Window Size"),
        dcc.Input(
            id='ma-window-size',
            type='number',
            placeholder='Enter window size',
            value=5,
            min=1,
            max=50,
            step=1
        ),
    ], style={'margin-top': '20px'}),

    # Latest Readings Section
    html.Div([
        html.H2("Latest Readings"),
        html.Div(id='latest-readings')
    ], style={'margin-top': '20px'}),

    # X-Axis Format Selection
    html.Div([
        html.Label("X-axis Format"),
        dcc.RadioItems(
            id='xaxis-format',
            options=[
                {'label': 'Date', 'value': 'date'},
                {'label': 'MJD', 'value': 'mjd'}
            ],
            value='date',
            labelStyle={'display': 'inline-block', 'margin-right': '10px'}
        )
    ], style={'margin-top': '20px'}),

    # Graphs
    dcc.Graph(id='temperature-graph'),
    dcc.Graph(id='humidity-graph'),
])

# Callback function to update sensor names
@app.callback(
    Output('channel-dropdown', 'options'),
    Input('update-sensor-names', 'n_clicks'),
    State('sensor-name-1', 'value'),
    State('sensor-name-2', 'value')
)
def update_sensor_names(n_clicks, sensor_name_1, sensor_name_2):
    if n_clicks > 0:
        SENSOR_NAMES[1] = sensor_name_1
        SENSOR_NAMES[2] = sensor_name_2
        return [{'label': sensor_name_1, 'value': 1}, {'label': sensor_name_2, 'value': 2}]
    return [{'label': SENSOR_NAMES[1], 'value': 1}, {'label': SENSOR_NAMES[2], 'value': 2}]

# Validate time input format
def validate_time(time_str):
    try:
        datetime.strptime(time_str, "%H:%M")
        return True
    except ValueError:
        return False

# Callback to fetch and update temperature and humidity data
@app.callback(
    [Output('temperature-graph', 'figure'),
     Output('humidity-graph', 'figure'),
     Output('latest-readings', 'children')],
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('start-time', 'value'),
     Input('end-time', 'value'),
     Input('time-range-dropdown', 'value'),
     Input('channel-dropdown', 'value'),
     Input('color-dropdown-1', 'value'),
     Input('color-dropdown-2', 'value'),
     Input('ma-window-size', 'value'),
     Input('xaxis-format', 'value')]
)
def update_graphs(start_date, end_date, start_time, end_time, time_range, selected_channels, color_1, color_2, ma_window_size, xaxis_format):
    if conn is None:
        return {}, {}, "Database connection is not established."

    # Validate time inputs
    if not validate_time(start_time) or not validate_time(end_time):
        return {}, {}, "Invalid time format. Please use HH:MM."

    cur = conn.cursor()

    # Combine date and time inputs to create datetime objects
    start_datetime = pd.to_datetime(f"{start_date} {start_time}")
    end_datetime = pd.to_datetime(f"{end_date} {end_time}")

    # Fetch temperature and humidity data
    temp_df = fetch_temperature_data(cur, start_datetime, end_datetime, selected_channels, ma_window_size)
    humidity_df = fetch_humidity_data(cur, start_datetime, end_datetime, selected_channels, ma_window_size)

    if temp_df.empty or humidity_df.empty:
        return {}, {}, "No data available for the selected date and time range."

    # Filter temperature data to exclude values >= 50 or <= -15
    temp_df_filtered = temp_df[(temp_df['temperature_s1'] < 50) & (temp_df['temperature_s1'] > -15)
                               & (temp_df['temperature_s2'] < 50) & (temp_df['temperature_s2'] > -15)]

    # Calculate moving average
    temp_df_filtered['temperature_s1_ma'] = temp_df_filtered['temperature_s1'].rolling(window=ma_window_size).mean()
    temp_df_filtered['temperature_s2_ma'] = temp_df_filtered['temperature_s2'].rolling(window=ma_window_size).mean()
    humidity_df['humidity_s1_ma'] = humidity_df['humidity_s1'].rolling(window=ma_window_size).mean()
    humidity_df['humidity_s2_ma'] = humidity_df['humidity_s2'].rolling(window=ma_window_size).mean()

    # Determine x-axis based on the selected format
    if xaxis_format == 'mjd':
        x_axis_temp = temp_df_filtered['mjd']
        x_axis_humidity = humidity_df['mjd']
        x_title = 'Modified Julian Date (MJD)'
    else:
        x_axis_temp = temp_df_filtered['timestamp']
        x_axis_humidity = humidity_df['timestamp']
        x_title = 'Date'

    # Create temperature graph
    temperature_fig = go.Figure()
    if 1 in selected_channels:
        temperature_fig.add_trace(go.Scatter(
            x=x_axis_temp,
            y=temp_df_filtered['temperature_s1'],
            mode='lines',
            name=f'{SENSOR_NAMES[1]}',
            line=dict(color=color_1)
        ))
        temperature_fig.add_trace(go.Scatter(
            x=x_axis_temp,
            y=temp_df_filtered['temperature_s1_ma'],
            mode='lines',
            name=f'{SENSOR_NAMES[1]} (MA)',
            line=dict(color=color_1, dash='dash')
        ))
    if 2 in selected_channels:
        temperature_fig.add_trace(go.Scatter(
            x=x_axis_temp,
            y=temp_df_filtered['temperature_s2'],
            mode='lines',
            name=f'{SENSOR_NAMES[2]}',
            line=dict(color=color_2)
        ))
        temperature_fig.add_trace(go.Scatter(
            x=x_axis_temp,
            y=temp_df_filtered['temperature_s2_ma'],
            mode='lines',
            name=f'{SENSOR_NAMES[2]} (MA)',
            line=dict(color=color_2, dash='dot')
        ))
    temperature_fig.update_layout(title='Temperature Data', xaxis_title=x_title, yaxis_title='Temperature (°C)')

    # Create humidity graph
    humidity_fig = go.Figure()
    if 1 in selected_channels:
        humidity_fig.add_trace(go.Scatter(
            x=x_axis_humidity,
            y=humidity_df['humidity_s1'],
            mode='lines',
            name=f'{SENSOR_NAMES[1]}',
            line=dict(color=color_1)
        ))
        humidity_fig.add_trace(go.Scatter(
            x=x_axis_humidity,
            y=humidity_df['humidity_s1_ma'],
            mode='lines',
            name=f'{SENSOR_NAMES[1]} (MA)',
            line=dict(color=color_1, dash='dash')
        ))
    if 2 in selected_channels:
        humidity_fig.add_trace(go.Scatter(
            x=x_axis_humidity,
            y=humidity_df['humidity_s2'],
            mode='lines',
            name=f'{SENSOR_NAMES[2]}',
            line=dict(color=color_2)
        ))
        humidity_fig.add_trace(go.Scatter(
            x=x_axis_humidity,
            y=humidity_df['humidity_s2_ma'],
            mode='lines',
            name=f'{SENSOR_NAMES[2]} (MA)',
            line=dict(color=color_2, dash='dot')
        ))
    humidity_fig.update_layout(title='Humidity Data', xaxis_title=x_title, yaxis_title='Humidity (%)')

    # Get the latest readings
    latest_temp_s1 = temp_df['temperature_s1'].iloc[-1]
    latest_temp_s2 = temp_df['temperature_s2'].iloc[-1]
    latest_humidity_s1 = humidity_df['humidity_s1'].iloc[-1]
    latest_humidity_s2 = humidity_df['humidity_s2'].iloc[-1]

    latest_readings = [
        html.Div(f"Latest {SENSOR_NAMES[1]} Temperature: {latest_temp_s1}°C"),
        html.Div(f"Latest {SENSOR_NAMES[2]} Temperature: {latest_temp_s2}°C"),
        html.Div(f"Latest {SENSOR_NAMES[1]} Humidity: {latest_humidity_s1}%"),
        html.Div(f"Latest {SENSOR_NAMES[2]} Humidity: {latest_humidity_s2}%")
    ]

    cur.close()

    return temperature_fig, humidity_fig, latest_readings

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
