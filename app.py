import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from data_loading import load_data

mentions_df = load_data()

# Ensure Date column is datetime
mentions_df["Date"] = pd.to_datetime(mentions_df["Date"])

# Streamlit app layout
st.title("Mentions Data Visualization")
st.write("Select the time range and granularity to view the data.")

# Time range selection
time_range = st.selectbox("Select Time Range", ["Year", "Month", "Week"], index=0)

# Granularity selection
granularity = st.selectbox("Select Granularity", ["Daily", "Weekly", "Monthly"], index=0)

# Axis toggle
axis_mode = st.radio("Axis Mode:", ["Fixed", "Dynamic"], index=0)

# Filter data based on time range
if time_range == "Year":
    start_date = datetime.now() - timedelta(days=365)
elif time_range == "Month":
    start_date = datetime.now() - timedelta(days=30)
elif time_range == "Week":
    start_date = datetime.now() - timedelta(days=7)

filtered_df = mentions_df[mentions_df["Date"] >= start_date]

# Adjust granularity
if granularity == "Daily":
    plot_df = filtered_df
elif granularity == "Weekly":
    plot_df = filtered_df.resample("W-Mon", on="Date").sum().reset_index()
elif granularity == "Monthly":
    plot_df = filtered_df.resample("M", on="Date").sum().reset_index()

# Traces for each channel
traces = [
    {"name": "Twitter", "data": plot_df["Twitter"], "color": "red", "fillcolor": "rgba(255, 0, 0, 0.2)"},
    {"name": "News", "data": plot_df["News"], "color": "blue", "fillcolor": "rgba(0, 0, 255, 0.2)"},
    {"name": "Blog", "data": plot_df["Blog"], "color": "green", "fillcolor": "rgba(0, 255, 0, 0.2)"},
]

# Create the Plotly chart
fig = go.Figure()

for trace in traces:
    fig.add_trace(go.Scatter(
        x=plot_df["Date"],
        y=trace["data"],
        mode='lines+markers',
        name=trace["name"],
        line=dict(shape='spline', color=trace["color"], width=2),
        marker=dict(size=6, color=trace["color"], symbol='circle'),
        fill='tozeroy',  # Fill to x-axis
        fillcolor=trace["fillcolor"],  # Translucent fill
        hovertemplate='<b>%{fullData.name}</b><br>Date: %{x}<br>Value: %{y}<extra></extra>',  # Show dynamic trace name
        opacity=1  # Default full opacity
    ))

# Configure the axes based on the toggle mode
if axis_mode == "Fixed":
    # Fixed axis range
    fig.update_layout(
        xaxis=dict(showgrid=False, fixedrange=True),
        yaxis=dict(showgrid=True, fixedrange=True, range=[0, max(plot_df.max(axis=1)) + 5]),
    )
else:
    # Dynamic axis range
    fig.update_layout(
        xaxis=dict(showgrid=False, automargin=True, fixedrange=False),
        yaxis=dict(showgrid=True, automargin=True, fixedrange=False, rangemode="tozero"),
    )

# Customize layout
fig.update_layout(
    title="Mentions Over Time",
    xaxis_title="Date",
    yaxis_title="Mentions",
    legend_title="Channels (Click to toggle visibility)",
    template="plotly_white",
    hovermode='x unified',
    showlegend=True,
)

# Display the chart
st.plotly_chart(fig)
