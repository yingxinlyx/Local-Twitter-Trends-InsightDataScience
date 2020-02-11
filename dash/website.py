import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import plotly
import plotly.graph_objs as go

import pandas as pd
import psycopg2


def query_from_db(query, database, user, password, host, port):
    """get data from postgresql and store data in dataframe"""

    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    col_names = [i[0] for i in cur.description]
    df = pd.DataFrame(rows, columns=col_names)
    conn.commit()
    cur.close()
    conn.close()

    return df


database='******'
user='******'
password='******'
host='10.0.0.6'
port='5432'


# web ui
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

app.layout = html.Div(
    html.Div([
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=1*1000*60, # in milliseconds
            n_intervals=0
        )
    ])
)


@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):

	# active user dataframe
	query1 = """select * 
				from activity 
				where timestamp in (select distinct max(timestamp) from activity)"""
	df1 = query_from_db(query1, database, user, password, host, port)

	# tag dataframe
	query2 = """select * 
	            from (select state, tag, cnt, rank() over(partition by state order by cnt desc)
	            	  from twitter_trend 
	            	  where timestamp in (select distinct max(timestamp) from twitter_trend)) temp
	            where rank <= 5"""
	df2 = query_from_db(query2, database, user, password, host, port)
	df3 = df2.groupby('state').tag.apply(list).to_frame().reset_index()
	df3['demo_tag'] = df3.tag.apply(lambda x: x[:5])
	df3['text'] = df3['demo_tag'].apply(lambda x: '<br>'.join(x))

	# join 2 dataframes
	res = df3[['state','text']].merge(df1[['state','cnt']], on='state')


	# graph
	fig = go.Figure(data=go.Choropleth(
	    locations=res['state'],
	    z=res['cnt'].astype(float),
	    locationmode='USA-states',
	    colorscale='Blues',
	    autocolorscale=False,
	    text=res['text'], # hover text
	    marker_line_color='white', # line markers between states
	    colorbar_title="Active #"
	))

	fig.update_layout(
	    title_text='Local Twitter Trends',
	    geo = dict(
	        scope='usa',
	        projection=go.layout.geo.Projection(type = 'albers usa'),
	        showlakes=True, # lakes
	        lakecolor='rgb(255, 255, 255)'),
	)

	return fig


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True)
