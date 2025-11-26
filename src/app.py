# app.py
from dash import Dash, html, dcc, callback, Output, Input, State, ctx, no_update
# import dash
import plotly.express as px
import pandas as pd
import datetime
# from dash.dcc import SendFile  # for typing only; actual send uses dcc.send_data_frame
# from dash import dcc as _dcc

# --- Placeholders for your real functions (imported from other modules) ---
# You said you'll import functions from other files. Replace these imports with your real ones.
# Example:
# from my_updaters import run_all_update_funcs
# from my_query_utils import generate_query, run_query

def run_all_update_funcs():
    """
    Placeholder. Your real function should run all update functions and return
    whatever in-memory objects you need. Expected return (example):
    {
        'species_df': pd.DataFrame([...]),  # columns: species_name, species_id
        'observations_df': pd.DataFrame([...])  # may contain species_id, serial_id, date, etc.
    }
    """
    # Create fake data for demo/testing if your real import isn't available.
    species_df = pd.DataFrame({
        'species_name': [f'Sp{i}' for i in range(1, 6)],
        'species_id': [101, 102, 103, 104, 105]
    })
    # observations_df contains columns used for serial_id extraction and for plotting demo
    observations_df = pd.DataFrame({
        'species_id': [101, 101, 102, 103, 104, 105] * 7,
        'serial_id': [f'S{n:02d}' for n in range(1, 43)][:42][:42][:42][:42][:42][:42][:42][:42]  # will be trimmed
    })
    # To produce distinct serial_ids (approx 40)
    observations_df = pd.DataFrame({
        'species_id': [101 + (i % 5) for i in range(40)],
        'serial_id': [f'S{n:02d}' for n in range(1, 41)],
        'date': pd.date_range('2020-01-01', periods=40, freq='7D'),
        # add some measurement columns for demo plotting:
        'x': list(range(40)),
        'y': [v * 2 + (i % 3) for i, v in enumerate(range(40))]
    })
    return {'species_df': species_df, 'observations_df': observations_df}

def generate_query(species_wanted, serial_id_wanted, datemin, datemax):
    """
    Placeholder. Your real function should accept the memory variables
    and return (sql_string, params_dict).
    """
    # Demo SQL string and params for display
    where_clauses = []
    params = {}
    if species_wanted is not None:
        where_clauses.append("species_id IN %(species)s")
        params['species'] = tuple(species_wanted)
    if serial_id_wanted is not None:
        where_clauses.append("serial_id IN %(serial)s")
        params['serial'] = tuple(serial_id_wanted)
    if datemin is not None:
        where_clauses.append("date >= %(datemin)s")
        params['datemin'] = datemin.isoformat()
    if datemax is not None:
        where_clauses.append("date <= %(datemax)s")
        params['datemax'] = datemax.isoformat()
    where = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = f"SELECT * FROM observations WHERE {where};"
    return sql, params

def run_query(sql, params):
    """
    Placeholder. Your real function should execute the SQL and return a pandas DataFrame.
    For demo we will return a filtered subset of the observations_df that we keep in memory.
    """
    # For demonstration we will filter the initial observations in-memory.
    # In real use, your run_query(sql, params) should execute against a DB.
    global _startup_observations_df  # set at startup below
    df = _startup_observations_df.copy()
    # apply filters based on params
    if params is None:
        return df
    if 'species' in params:
        df = df[df['species_id'].isin(params['species'])]
    if 'serial' in params:
        df = df[df['serial_id'].isin(params['serial'])]
    if 'datemin' in params:
        df = df[df['date'] >= pd.to_datetime(params['datemin'])]
    if 'datemax' in params:
        df = df[df['date'] <= pd.to_datetime(params['datemax'])]
    return df.reset_index(drop=True)


# ---------------------------------------------------------
# APPLICATION SETUP
# ---------------------------------------------------------
app = Dash(__name__)
server = app.server

# ------------------------------
# RUN AT APP STARTUP (module import time)
# ------------------------------
# IMPORTANT: The following block runs exactly once when the module is imported,
# i.e., when the app starts. This is where we call your "update current" logic
# automatically on startup.
startup_results = run_all_update_funcs()
# Extract expected frames into module-level variables for demo-run_query to use:
_startup_species_df = startup_results.get('species_df', pd.DataFrame(columns=['species_name', 'species_id']))
_startup_observations_df = startup_results.get('observations_df', pd.DataFrame())

# NOTE TO YOU: The above run_all_update_funcs() is executed on startup. You can
# modify it to call whatever update functions you need so that memory is
# pre-populated when the app begins. This mirrors the "automatic run at startup"
# requirement you asked for.
# ------------------------------

# Helper to build dropdown options from a dataframe
def df_to_options(df, label_col, value_col):
    return [{'label': row[label_col], 'value': row[value_col]} for _, row in df.iterrows()]

# Initial Store data (populated from startup call)
initial_species_store = _startup_species_df.to_dict(orient='records')
initial_observations_store = _startup_observations_df.to_dict(orient='records')

# ------------------------------
# LAYOUT
# ------------------------------
app.layout = html.Div([
    # Stores: persist "in-memory" objects
    dcc.Store(id='store-species-df', data=initial_species_store),
    dcc.Store(id='store-observations-df', data=initial_observations_store),
    dcc.Store(id='store-sql', data=None),
    dcc.Store(id='store-params', data=None),
    dcc.Store(id='store-results-df', data=None),

    html.H1('Title of Dash App', style={'textAlign': 'center'}),

    html.Div([
        # LEFT: controls column
        html.Div([
            html.H3("Query Controls"),
            html.Label("Species (multi-select)"),
            dcc.Dropdown(
                id='dropdown-species',
                options=df_to_options(_startup_species_df, 'species_name', 'species_id'),
                multi=True,
                placeholder='Select species...'
            ),

            html.Br(),
            html.Label("Serial ID (multi-select)"),
            dcc.Dropdown(
                id='dropdown-serial',
                options=[{'label': s, 'value': s} for s in sorted(_startup_observations_df['serial_id'].unique())] if not _startup_observations_df.empty else [],
                multi=True,
                placeholder='Select serial IDs...'
            ),
            html.Button("Select all serials", id='btn-select-all-serials', n_clicks=0),

            html.Br(), html.Br(),
            html.Label("Minimum date"),
            dcc.DatePickerSingle(
                id='date-min',
                placeholder='Select minimum date',
                display_format='YYYY-MM-DD'
            ),
            html.Br(),
            html.Label("Maximum date"),
            dcc.DatePickerSingle(
                id='date-max',
                placeholder='Select maximum date',
                display_format='YYYY-MM-DD'
            ),
            html.Br(), html.Br(),

            html.Button("Generate query", id='btn-generate-query', n_clicks=0),
            html.Span(id='sql-display', style={'fontSize': '12px', 'marginLeft': '10px', 'display': 'inline-block', 'verticalAlign': 'middle', 'maxWidth': '400px', 'whiteSpace': 'pre-wrap'}),
            html.Br(), html.Br(),

            html.Button("Run query", id='btn-run-query', n_clicks=0, disabled=True),
        ], style={'width': '28%', 'display': 'inline-block', 'verticalAlign': 'top', 'padding': '10px', 'boxSizing': 'border-box'}),

        # RIGHT: update button, figure, export button
        html.Div([
            html.Div([
                html.Button("Update current", id='btn-update-current', n_clicks=0)
            ], style={'textAlign': 'right', 'marginBottom': '10px'}),

            dcc.Graph(id='graph-content', style={'height': '60vh'}),

            html.Br(),
            html.Div([
                html.Button("Export CSV (current results)", id='btn-export-csv', n_clicks=0),
                dcc.Download(id='download-results-csv')
            ], style={'textAlign': 'right', 'marginTop': '10px'})
        ], style={'width': '68%', 'display': 'inline-block', 'padding': '10px', 'boxSizing': 'border-box', 'verticalAlign': 'top'})
    ], style={'width': '100%', 'display': 'flex', 'justifyContent': 'space-between'}),
    html.Div([
        html.P("Note: 'Update current' was executed once at app startup. "
               "Replace the placeholder functions at top with your real imports."),
    ], style={'marginTop': '10px', 'fontStyle': 'italic'})
])


# ---------------------------------------------------------
# CALLBACKS
# ---------------------------------------------------------

# 1) Update current button: re-run update functions and re-populate stores
@callback(
    Output('store-species-df', 'data'),
    Output('store-observations-df', 'data'),
    Input('btn-update-current', 'n_clicks'),
    prevent_initial_call=False  # allow it to run at page load if triggered; we've already run at module import
)
def on_update_current(n_clicks):
    """
    Runs the (imported) update functions to refresh in-memory data.
    This is connected to the 'Update current' button. It was ALSO invoked at
    module import time above (see startup_results).
    """
    # call your external function(s) that refresh the data
    results = run_all_update_funcs()
    species_df = results.get('species_df', pd.DataFrame(columns=['species_name', 'species_id']))
    observations_df = results.get('observations_df', pd.DataFrame())

    # Update module-level copy used by demo run_query
    global _startup_species_df, _startup_observations_df
    _startup_species_df = species_df
    _startup_observations_df = observations_df

    return species_df.to_dict(orient='records'), observations_df.to_dict(orient='records')


# 2) When species or observations store changes, refresh dropdown options for species and serial dropdown
@callback(
    Output('dropdown-species', 'options'),
    Output('dropdown-serial', 'options'),
    Input('store-species-df', 'data'),
    Input('store-observations-df', 'data')
)
def refresh_dropdown_options(species_store, observations_store):
    species_df = pd.DataFrame(species_store) if species_store else pd.DataFrame(columns=['species_name', 'species_id'])
    obs_df = pd.DataFrame(observations_store) if observations_store else pd.DataFrame()

    species_options = df_to_options(species_df, 'species_name', 'species_id') if not species_df.empty else []
    # serial options are the unique serial_id from observations_df
    serial_options = [{'label': sid, 'value': sid} for sid in sorted(obs_df['serial_id'].unique())] if not obs_df.empty and 'serial_id' in obs_df.columns else []
    return species_options, serial_options


# 3) Select all serials button: set dropdown-serial value to all available options
@callback(
    Output('dropdown-serial', 'value'),
    Input('btn-select-all-serials', 'n_clicks'),
    State('dropdown-serial', 'options'),
    prevent_initial_call=True
)
def select_all_serials(n_clicks, options):
    if not options:
        return no_update
    all_values = [opt['value'] for opt in options]
    return all_values


# 4) Generate query button: read current selections and create SQL + params via your function
@callback(
    Output('store-sql', 'data'),
    Output('store-params', 'data'),
    Output('sql-display', 'children'),
    Input('btn-generate-query', 'n_clicks'),
    State('dropdown-species', 'value'),
    State('dropdown-serial', 'value'),
    State('date-min', 'date'),
    State('date-max', 'date'),
    State('dropdown-species', 'options'),
    State('dropdown-serial', 'options'),
    prevent_initial_call=False
)
def on_generate_query(n_clicks, species_selected, serial_selected, date_min, date_max, species_options, serial_options):
    """
    Creates species_wanted and serial_id_wanted with your rule:
    - If none selected OR all selected -> set to None
    - Else -> list of selected ids
    Then calls generate_query(...) to produce sql and params to store.
    """
    # Convert options lists into full-value lists for "all" checking
    all_species_values = [opt['value'] for opt in species_options] if species_options else []
    all_serial_values = [opt['value'] for opt in serial_options] if serial_options else []

    # Species: None if none selected or all selected
    if not species_selected or set(species_selected) == set(all_species_values):
        species_wanted = None
    else:
        # species_selected values are already species_id numbers (per design)
        species_wanted = list(species_selected)

    # Serial: None if none selected or all selected
    if not serial_selected or set(serial_selected) == set(all_serial_values):
        serial_id_wanted = None
    else:
        serial_id_wanted = list(serial_selected)

    # Date handling: date strings from DatePickerSingle or None
    datemin = pd.to_datetime(date_min).to_pydatetime().date() if date_min else None
    datemax = pd.to_datetime(date_max).to_pydatetime().date() if date_max else None

    # Call your generate_query(...) placeholder / real function
    sql, params = generate_query(species_wanted, serial_id_wanted, datemin, datemax)

    # Display the SQL in small font; store SQL and params in Stores
    return sql, params, sql


# 5) Enable/disable Run Query button depending on whether SQL exists in memory
@callback(
    Output('btn-run-query', 'disabled'),
    Input('store-sql', 'data')
)
def toggle_run_button(sql):
    # If sql is None or empty, disable run button
    if not sql:
        return True
    return False


# 6) Run query button: run the query (using your run_query function) and store result df and return a figure
@callback(
    Output('store-results-df', 'data'),
    Output('graph-content', 'figure'),
    Input('btn-run-query', 'n_clicks'),
    State('store-sql', 'data'),
    State('store-params', 'data'),
    prevent_initial_call=True
)
def on_run_query(n_clicks, sql, params):
    # Call your run_query placeholder / real function to execute SQL + params and return a DataFrame
    df = run_query(sql, params)
    # Save results to store
    results_data = df.to_dict(orient='records')

    # Build a demo figure. You will replace this with your real plotting code later.
    # Use some sensible defaults if columns not present.
    if not df.empty and {'x', 'y'}.issubset(df.columns):
        fig = px.scatter(df, x='x', y='y', title='Query Results (x vs y)')
    elif not df.empty and 'date' in df.columns and 'serial_id' in df.columns:
        # Example time series: count per date
        df2 = df.groupby('date').size().reset_index(name='count')
        fig = px.line(df2, x='date', y='count', title='Observations over time')
    else:
        # fallback: simple table-like empty figure
        fig = px.scatter(pd.DataFrame({'x': [], 'y': []}), x='x', y='y', title='No data to plot')

    return results_data, fig


# 7) Export CSV: create and send CSV from store-results-df
@callback(
    Output('download-results-csv', 'data'),
    Input('btn-export-csv', 'n_clicks'),
    State('store-results-df', 'data'),
    prevent_initial_call=True
)
def export_csv(n_clicks, results_store):
    if not results_store:
        return no_update
    df = pd.DataFrame(results_store)
    # send_data_frame automatically sets a filename and MIME type
    return dcc.send_data_frame(df.to_csv, filename=f'results_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv', index=False)


# ---------------------------------------------------------
# RUN APP
# ---------------------------------------------------------

# # Testing mode
# if __name__ == '__main__':
#     app.run(debug=True)

import webbrowser
from threading import Timer
# Deployment mode
if __name__ == '__main__':
    # A warning comes up not to use this server for production, but since this app is only designed to be run locally and occasionally
    # Using an extremely more stable server or really anything different is unnecessary.

    port = 8050 # default production is 8050
    address = f"http://localhost:{port}"

    Timer(5, webbrowser.open_new(address))
    app.run(debug=False, port = port)
