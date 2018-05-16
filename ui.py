import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, Event
from cassandra.cluster import Cluster
import config
from plotly.graph_objs import Figure

cluster = Cluster(config.CASS_CLUSTER)
session = cluster.connect()
session.execute("USE " + config.KEYSPACE)

app = dash.Dash('ML Watch')
server = app.server

app.layout = html.Div([
    html.Div([
        html.H2("ML Watch: Keep an eye on your predictions."),
        #html.Img(src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe-inverted.png"),
    ],  className='Banner'), 
  html.Div([
	    dcc.Graph(id='frequency_out'),
    ], style={'width': '100%', 'height': 'auto', 'display': 'inline-block'} ),
    html.Div([
    	    dcc.Graph(id='confidence_out'),
    ] , style={'width': '100%', 'display': 'inline-block'} ),
    dcc.Interval(id='confidence_update', interval=10000, n_intervals=0),
    dcc.Interval(id='frequency_update', interval=10000, n_intervals=0),
    ], style={'width': '90%', 'display': 'inline-block'})

@app.callback(
    Output('confidence_out','figure' ),
    [Input('confidence_update', 'n_intervals')]
)
def generate_confidence_histogram(interval):
    rows = session.execute("SELECT * FROM stats")
    
    x=[]
    y1=[]
    y2=[]
    for row in rows:
        x.append(row.prediction)
        y2.append(row.acc_score/row.count)

    trace=[{'x': x, 'y': y2, 'type': 'bar', 'name': 'Count'}]
    layout={'height': 300, 'yaxis': {'title': 'Avg. Confidence'} }  
    return Figure(data=trace, layout=layout)

@app.callback(
    Output('frequency_out','figure' ),
    [Input('frequency_update', 'n_intervals')]
)
def generate_frequency_histogram(interval):
    rows = session.execute("SELECT * FROM stats")
    
    x=[]
    y1=[]
    y2=[]
    for row in rows:
        x.append(row.prediction)
        y1.append(row.count)

    trace=[{'x': x, 'y': y1, 'type': 'bar', 'name': 'Count'}]
    layout={'height': 300, 'yaxis': {'title': 'Frequency'}
           }  
    return Figure(data=trace, layout=layout)

if __name__ == '__main__':
    
    #import daemon
    #with daemon.DaemonContext():
        #app.run_server(debug=True)
    app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
    app.run_server(debug=True, host="0.0.0.0", port=1080)
