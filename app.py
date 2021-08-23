import pandas as pd
import mysql.connector
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
import os


param_mysql = {
    "host"      : "107.180.20.85",
    "database"  : "i3291942_wp2",
    "user"      : "henryro",
    "passwd"  : "Ger@ld1981"
}


def connect_mysql(params_mysql):
    try:
        conn_mysql = None
        conn_mysql = mysql.connector.connect(
            host=params_mysql.get("host"),
            user=params_mysql.get("user"),
            passwd=params_mysql.get("passwd"),
            database=params_mysql.get("database"))
        print("Connection successful")
    except (Exception) as error:
        print(error)
    return conn_mysql


def get_sessions(param_con):
    sql_query="SELECT s_created FROM wp_watsonconv_sessions"
    mydb = connect_mysql(param_con)
    data = pd.read_sql(sql_query, mydb)
    mydb.close()
    return data


def get_intents_text(param_con):
    sql_query="SELECT * FROM wp_watsonconv_intents_text"
    mydb = connect_mysql(param_con)
    data = pd.read_sql(sql_query, mydb)
    mydb.close()
    return data


def get_requests(param_con):
    sql_query = "select r.s_created as created,r.id as request_id,r.a_session_id as session_id,i.p_text as input_text " \
              "from  wp_watsonconv_requests r  INNER JOIN wp_watsonconv_user_inputs i on(i.id=r.id)"
    mydb = connect_mysql(param_con)
    data = pd.read_sql(sql_query, mydb)
    data['created'] = pd.to_datetime(data['created']).dt.date
    mydb.close()
    data['session_id'] = data['session_id'].astype(str)
    data['session_id'] = data['session_id'].astype("|S")
    return data


def get_requests_outputs(param_con):
    sql_query="SELECT o.id,o.a_request_id as request_id,o.p_response_type as output_response_type,o.p_text as output_text," \
                "o.p_title as output_title  FROM wp_watsonconv_watson_outputs o where o.p_response_type<>'pause'"
    mydb = connect_mysql(param_con)
    data = pd.read_sql(sql_query, mydb)
    mydb.close()
    return data


def get_request_intent(param_con):
    sql_query="select o.a_request_id as request_id,MIN(o.id) as intent_id,i.p_intent as intent from wp_watsonconv_output_intents o " \
                "INNER JOIN wp_watsonconv_intents i on (i.id=o.o_intent_id) " \
                "GROUP BY o.a_request_id"
    mydb = connect_mysql(param_con)
    data = pd.read_sql(sql_query, mydb)
    mydb.close()
    return data



app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

requests = get_requests(param_mysql)
requests_outputs = get_requests_outputs(param_mysql)
requests_intents = get_request_intent(param_mysql)
sessions = get_sessions(param_mysql)
chats = pd.merge(requests,requests_outputs, how="outer", on="request_id")
end_date_sessions = chats['created'].max()
start_date_sessions= chats['created'].min()
sessions_counts = chats.groupby('session_id').count()[['request_id','id']].reset_index()
sessions_counts.rename(columns={"request_id": "count_request", "id": "count_output"}, inplace=True)
chats = pd.merge(chats,sessions_counts, on="session_id")
mask_1 = chats['count_request']==1
mask_2 = chats['count_output']==1
mask_3 = chats['input_text']==''
chats.loc[mask_1 & mask_2 & mask_3, 'interaction'] = 'no interaction'
mask_1 = chats['interaction'].isnull()
chats.loc[mask_1, 'interaction'] = 'interaction'

#intents
chats = pd.merge(chats,requests_intents, how="outer", on="request_id")

data_feedback = chats.groupby(['session_id','intent'])
data_feedback = data_feedback.size().reset_index(name='counts_intents')


mask_1 = data_feedback['intent']=='General_Positive_Feedback'
mask_2 = data_feedback['intent']=='General_Negative_Feedback'
mask_3 = data_feedback['intent']=='feedback'

data_intent_feedback = data_feedback[mask_1][['session_id','counts_intents']]
data_intent_feedback.rename(columns={"counts_intents": "count_positive_feedback"}, inplace=True)
chats = pd.merge(chats,data_intent_feedback, how="outer", on="session_id")

data_intent_feedback = data_feedback[mask_2][['session_id','counts_intents']]
data_intent_feedback.rename(columns={"counts_intents": "count_negative_feedback"}, inplace=True)
chats = pd.merge(chats,data_intent_feedback, how="outer", on="session_id")

data_intent_feedback = data_feedback[mask_3][['session_id','counts_intents']]
data_intent_feedback.rename(columns={"counts_intents": "count_generald_feedback"}, inplace=True)
chats = pd.merge(chats,data_intent_feedback, how="outer", on="session_id")

mask_1 = chats['count_positive_feedback']>0
mask_2 = chats['count_negative_feedback'].isnull()
mask_3 = chats['count_generald_feedback'].isnull()
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'positive feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback'].isnull()
mask_2 = chats['count_negative_feedback']>0
mask_3 = chats['count_generald_feedback'].isnull()
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'negative feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback'].isnull()
mask_2 = chats['count_negative_feedback'].isnull()
mask_3 = chats['count_generald_feedback']>0
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'general feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback']>0
mask_2 = chats['count_negative_feedback']>0
mask_3 = chats['count_generald_feedback'].isnull()
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'compound feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback']>0
mask_2 = chats['count_negative_feedback'].isnull()
mask_3 = chats['count_generald_feedback']>0
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'compound feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback'].isnull()
mask_2 = chats['count_negative_feedback']>0
mask_3 = chats['count_generald_feedback']>0
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'compound feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback']>0
mask_2 = chats['count_negative_feedback']>0
mask_3 = chats['count_generald_feedback']>0
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'compound feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'yes'

mask_1 = chats['count_positive_feedback'].isnull()
mask_2 = chats['count_negative_feedback'].isnull()
mask_3 = chats['count_generald_feedback'].isnull()
chats.loc[mask_1 & mask_2 & mask_3, 'feedback'] = 'no feedback'
chats.loc[mask_1 & mask_2 & mask_3, 'has_feedback'] = 'not'

mask_1 = chats['interaction'] == 'no interaction'
chats.loc[mask_1, 'feedback'] = 'no interaction'


all_intents = get_intents_text(param_mysql)
#print(all_intents)


def get_total_active_sessions(chats_filtered):
    df_active_sessions = chats_filtered.loc[chats_filtered['interaction'] == 'interaction', 'session_id'].drop_duplicates().count()
    return df_active_sessions


def get_average(total,delta):
    daily = int(total/delta.days)
    weekly = int(total/(delta.days/7))
    monthly = int(total / (delta.days / 30))

    return [daily,weekly,monthly]


def get_total_sessions(chats_filtered):
    df_sessions = chats_filtered['session_id'].drop_duplicates().count()
    return df_sessions


def get_total_unique_intents(chats_filtered):
    df_unique_intents = chats_filtered.loc[chats_filtered['intent'].notnull(), 'intent'].drop_duplicates().count()
    return df_unique_intents


def get_fig_active_session(chats_filtered):
    chats_filtered = chats_filtered.groupby(['session_id', 'interaction']).size().reset_index(name='count')
    chats_filtered.drop('count', axis=1, inplace=True)
    chats_filtered = chats_filtered.groupby(['interaction']).size().reset_index(name='count')
    fig = px.pie(chats_filtered,
                 values='count',
                 names='interaction',
                 labels={'count': 'sessions', 'interaction': 'status'},
                 title="% Sessions that have interaction",
                 color= 'interaction',
                 color_discrete_map={'interaction': 'green', 'no interaction': px.colors.qualitative.G10[5]},)
    return fig


def get_fig_feedback_session(chat_filtered):
    result = chat_filtered[chat_filtered['feedback'] != 'no interaction']
    result = result[result['feedback'] != 'no feedback']
    result = result.groupby(['session_id', 'feedback'])
    result = result.size().reset_index(name='count')
    chat_filtered = result['feedback'].value_counts().rename_axis('feedback').reset_index(name='sessions')

    fig = go.Figure(go.Bar(
        x=chat_filtered['sessions'],
        y=chat_filtered['feedback'],
        orientation='h',
        marker=dict(
            color=px.colors.qualitative.G10[5],
            line=dict(color='rgb(248, 248, 249)', width=1)
        )
    ))

    fig.update_layout(
                      xaxis_ticksuffix = ' sessions',
                      title='Feedback of the sessions' ,
                      title_x = 0.5)

    return fig


def get_fig_total_feedback_session(chats_filtered):
    mask = chats_filtered['interaction'] == 'interaction'
    chats_filtered = chats_filtered[mask]
    chats_filtered = chats_filtered.groupby(['session_id', 'has_feedback']).size().reset_index(name='count')
    chats_filtered.drop('count', axis=1, inplace=True)
    chats_filtered = chats_filtered.groupby(['has_feedback']).size().reset_index(name='count')
    fig = px.pie(chats_filtered,
                 values='count',
                 names='has_feedback',
                 labels={'count': 'sessions', 'has_feedback': 'feedback'},
                 title="% Active sessions that have feedback",
                 color= 'has_feedback',
                 color_discrete_map={'yes': 'green', 'not': px.colors.qualitative.G10[5]},)
    return fig


def get_fig_total_feedback_session(chats_filtered):
    mask = chats_filtered['interaction'] == 'interaction'
    chats_filtered = chats_filtered[mask]
    chats_filtered = chats_filtered.groupby(['session_id', 'has_feedback']).size().reset_index(name='count')
    chats_filtered.drop('count', axis=1, inplace=True)
    chats_filtered = chats_filtered.groupby(['has_feedback']).size().reset_index(name='count')
    fig = px.pie(chats_filtered,
                 values='count',
                 names='has_feedback',
                 labels={'count': 'sessions', 'has_feedback': 'feedback'},
                 title="% Active sessions that have feedback",
                 color= 'has_feedback',
                 color_discrete_map={'yes': 'green', 'not': px.colors.qualitative.G10[5]},)
    return fig


def get_top_10_intent(chat_filtered):
    mask = chat_filtered['interaction'] == 'interaction'
    result = chat_filtered[mask]
    result = result.dropna(subset=['intent'])
    result = result.groupby(['request_id', 'intent'])
    result = result.size().reset_index(name='count')
    top_10 = result['intent'].value_counts().rename_axis('intent').reset_index(name='requests')
    top_10 = top_10.sort_values('requests', ascending=False).head(10)
    top_10 = top_10.sort_values('requests', ascending=True)

    fig = go.Figure(go.Bar(
        x=top_10['requests'],
        y=top_10['intent'],
        orientation='h',
        marker=dict(
            color=px.colors.qualitative.G10[5],
            line=dict(color='rgb(248, 248, 249)', width=1)
        )
    ))

    fig.update_layout(
                      xaxis_ticksuffix = ' requests',
                      title='Top 10 Intents' ,
                      title_x = 0.5)

    return fig


def get_last_10_intent(chat_filtered):
    mask = chat_filtered['interaction'] == 'interaction'
    result = chat_filtered[mask]
    result = result.dropna(subset=['intent'])
    result = result.groupby(['request_id', 'intent'])
    result = result.size().reset_index(name='count')
    top_10 = result['intent'].value_counts().rename_axis('intent').reset_index(name='requests')
    top_10 = pd.merge(top_10, all_intents, how="outer", on="intent")
    top_10['requests'] = top_10['requests'].fillna(0)
    #print(top_10_intents)
    top_10 = top_10.sort_values('requests', ascending=True).head(10)

    fig = go.Figure(go.Bar(
        x=top_10['requests'],
        y=top_10['intent'],
        orientation='h',
        marker=dict(
            color=px.colors.qualitative.G10[5],
            line=dict(color='rgb(248, 248, 249)', width=1)
        )
    ))

    fig.update_layout(
                      xaxis_ticksuffix = ' requests',
                      title='Bottom 10 Intents' ,
                      title_x = 0.5)

    return fig


def get_fig_session_time(chat_filtered):
    mask_1 = chat_filtered['interaction'] == 'interaction'
    chat_filtered = chat_filtered[mask_1]
    result = chat_filtered.groupby(['created', 'session_id']).size().reset_index(name='count')
    result.drop('count', axis=1, inplace=True)
    result = result.groupby(['created']).size().reset_index(name='count')
    fig = go.Figure(data=[go.Scatter(x=result['created'], y=result['count'],mode='lines+markers')])
    # Add range slider
    fig.update_layout(
        title="Active sessions per day",
        yaxis_ticksuffix=' sessions',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )
    return fig


def get_fig_session_monthly(chat_filtered):
    mask_1 = chats['interaction'] == 'interaction'
    chat_filtered = chat_filtered[mask_1]
    result = chat_filtered.groupby(['created', 'session_id']).size().reset_index(name='count')
    result.drop('count', axis=1, inplace=True)
    result = result.groupby(['created']).size().reset_index(name='count')
    #result['created'] = pd.to_datetime(result['created'])
    result_1 = result
    result_1['created'] = pd.to_datetime(result_1['created'])
    result_1 = result_1.groupby(pd.Grouper(key='created', freq='M')).sum()
    result_1=result_1.reset_index()
    result_1['created'] = pd.to_datetime(result_1['created']).dt.date
    #print(result_1)
    fig = go.Figure(data=[go.Scatter(x=result_1['created'], y=result_1['count'],mode='lines+markers')])
    # Add range slider
    fig.update_layout(
        title="Active sessions per month",
        yaxis_ticksuffix=' sessions',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label="1m",
                         step="month",
                         stepmode="backward"),
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="YTD",
                         step="year",
                         stepmode="todate"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )
    return fig


card_content_options = [
    dbc.CardBody([
        html.H4('Select dates', style = {'textAlign':'left'}),
        dbc.Row([
            dbc.Col([
                dcc.DatePickerRange(
                    id='date-picker-range',
                    min_date_allowed=start_date_sessions,
                    max_date_allowed=end_date_sessions,
                    #initial_visible_month=date(2017, 8, 5),
                    end_date=end_date_sessions,
                    start_date=start_date_sessions,
                )
            ])

        ], align="center")
    ])
]

body_app = dbc.Container([
    html.Br(),
    dbc.Row([
        dbc.Col([dbc.Card(card_content_options, style={'height':'150px'})], md=3,lg=3, sm=12),
        dbc.Col([dbc.Card(id='card_total_sessions_general',style={'height':'150px'})], md=3, lg=3, sm=12),
        dbc.Col([dbc.Card(id='card_total_sessions',style={'height':'150px'})], md=3, lg=3, sm=12),
        dbc.Col([dbc.Card(id='card_total_intents',style={'height':'150px'})], md=3, lg=3, sm=12),
    ]),

    html.Br(),
    dbc.Row([
        dbc.Col([dbc.Card(id='card_pie_active_session')], md=4, lg=4, sm=12),
        dbc.Col([dbc.Card(id='card_pie_total_feedback_session')], md=4, lg=4, sm=12),
        dbc.Col([dbc.Card(id='card_pie_feedback_session')], md=4, lg=4, sm=12),
    ]),
    html.Br(),
    dbc.Row([
        dbc.Col([dbc.Card(id='card_sessions_time')], md=12, lg=12, sm=12),
    ]),
    html.Br(),
    dbc.Row([
        dbc.Col([dbc.Card(id='card_sessions_monthly')], md=12, lg=12, sm=12),
    ]),
    dbc.Row([
        dbc.Col([dbc.Card(id='card_top_10_intent')], md=6, lg=6, sm=12),
        dbc.Col([dbc.Card(id='card_last_10_intent')], md=6, lg=6, sm=12),
    ]),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),


], fluid=True)

navbar = dbc.Navbar(id='navbar', children=[

    html.A(
        dbc.Row([
            dbc.Col(html.Img(src=app.get_asset_url('logo.png'), height="70px")),
            dbc.Col(
                dbc.NavbarBrand("XiBot Dashboard",
                                style={'color': 'black', 'fontSize': '30px', 'fontFamily': 'Times New Roman','marginLeft':25}
                                )

            )

        ], align="center",
            no_gutters=True),
        href='/'
    )

])

app.layout = html.Div(id='parent', children=[navbar, body_app])


@app.callback([Output('card_total_sessions_general', 'children'),
               Output('card_total_sessions', 'children'),
               Output('card_total_intents', 'children'),
               Output('card_pie_active_session', 'children'),
               Output('card_pie_total_feedback_session', 'children'),
               Output('card_pie_feedback_session', 'children'),
               Output('card_sessions_time', 'children'),
               Output('card_sessions_monthly', 'children'),
               Output('card_top_10_intent', 'children'),
               Output('card_last_10_intent', 'children'),
               ],
              [dash.dependencies.Input('date-picker-range', 'start_date'),
               dash.dependencies.Input('date-picker-range', 'end_date')])
def update_cards(start_date, end_date):
    start_date=datetime.strptime(start_date, '%Y-%m-%d')
    end_date=datetime.strptime(end_date, '%Y-%m-%d')
    delta = end_date - start_date
    chats_filtered = chats[(chats['created'] >= start_date.date()) & (chats['created'] <= end_date.date())]

    card_content_sessions_general = [

        dbc.CardBody(
            [
                html.H4('Total Sessions ', style={ 'textAlign': 'center'}),
                html.H3('{0}'.format(get_total_sessions(chats_filtered)), style={'color': '#090059', 'textAlign': 'center'}),
                html.H5('Daily:{0[0]} Weekly:{0[1]} Monthly: {0[2]}'.format(
                    get_average(get_total_sessions(chats_filtered), delta)),
                        style={'color': '#090059', 'textAlign': 'center'}),
            ]

        )
    ]
    card_content_sessions = [

        dbc.CardBody(
            [
                html.H4('Total Active Sessions ', style={ 'textAlign': 'center'}),
                html.H3('{0}'.format(get_total_active_sessions(chats_filtered)), style={'color': '#090059', 'textAlign': 'center'}),
                html.H5('Daily:{0[0]} Weekly:{0[1]} Monthly: {0[2]}'.format(get_average(get_total_active_sessions(chats_filtered),delta)),
                        style={'color': '#090059', 'textAlign': 'center'}),
            ]

        )
    ]
    card_content_intents = [

        dbc.CardBody(
            [
                html.H4('Total Unique Intents ', style={ 'textAlign': 'center'}),
                html.H3('{0}'.format(get_total_unique_intents(chats_filtered)),
                        style={'color': '#090059', 'textAlign': 'center'}),
            ]

        )
    ]
    card_pie_active_session = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_fig_active_session(chats_filtered)),
            ]

        )
    ]
    card_pie_total_feedback_session = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_fig_total_feedback_session(chats_filtered)),
            ]

        )
    ]
    card_pie_feedback_session = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_fig_feedback_session(chats_filtered)),

            ]

        )
    ]

    card_session_time = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_fig_session_time(chats_filtered)),

            ]

        )
    ]


    card_session_monthly = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_fig_session_monthly(chats_filtered)),

            ]

        )
    ]

    card_top_10_intent = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_top_10_intent(chats_filtered)),

            ]

        )
    ]
    card_last_10_intent = [

        dbc.CardBody(
            [
                dcc.Graph(figure=get_last_10_intent(chats_filtered)),

            ]

        )
    ]
    return card_content_sessions_general,\
        card_content_sessions, \
        card_content_intents,\
        card_pie_active_session, \
        card_pie_total_feedback_session, \
        card_pie_feedback_session,\
        card_session_time,\
        card_session_monthly,\
        card_top_10_intent,\
        card_last_10_intent


if __name__ == '__main__':

    app.run_server()