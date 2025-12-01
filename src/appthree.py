# app.py
from dash import Dash, html, dcc, callback, Output, Input, State, ctx, no_update
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import datetime

# Try to import your real functions. If they are not available (e.g. during local dev),
# fall back to demo placeholders so the app still runs.
try:
    from app_functions.generate_sql_query import generate_query_and_params
    print("generate_query_and_params imported")
except Exception as e:
    generate_query_and_params = None
    _IMPORT_GENERATE_QUERY_ERROR = str(e)

try:
    from db_code.interact_db import read_db
    print("interact_db imported")
except Exception as e:
    read_db = None
    _IMPORT_READ_DB_ERROR = str(e)

# NEW IMPORTS FOR WEBSCRAPING ##########
try:
    from app_functions.webscraping import do_webscrape
except Exception as e:
    do_webscrape = None
    _IMPORT_WEBSCRAPE_ERROR = str(e)

try:
    from db_code.interact_db import add_new
except Exception as e:
    add_new = None
    _IMPORT_ADD_NEW_ERROR = str(e)

# -------------------------
# Helper helpers & fallback demo data
# -------------------------
def df_to_options(df, label_col, value_col):
    return [{'label': row[label_col], 'value': row[value_col]} for _, row in df.iterrows()]

def _demo_species_df():
    return pd.DataFrame({
        'species_name': [f'Sp{i}' for i in range(1, 6)],
        'species_id': [101, 102, 103, 104, 105]
    })

def _demo_serials_df():
    # column named 'serialId' in demo to mirror the expected read_db output for serials
    return pd.DataFrame({'serialId': [f'S{n:02d}' for n in range(1, 21)]})

# -------------------------
# Integrations: run_all_update_funcs -> queries the DB for "all" lists using "##!##" placeholders
# -------------------------
def run_all_update_funcs():
    """
    This function runs at startup and returns:
     - species_df: DataFrame with at least columns ['species_name', 'species_id']
     - observations/serials_df: DataFrame used to populate serial dropdown (expected to contain serial IDs)

    IMPORTANT:
    - The SQL strings below are intentionally set to "##!##" which is your marker for "please replace me"
      — you will provide custom SQL that returns the expected columns.
    - read_db(sql) is expected to return:
        * For species SQL: DataFrame with columns ['species_id', 'species_name']
        * For serials SQL: DataFrame with a single column 'serialId'
    """
    # SQL placeholders for you to replace with your own queries that return the correct columns
    sql_serials = "SELECT serialId FROM tAnimal"   # expects single column 'serialId'
    sql_species = "SELECT * FROM tSpecies"   # expects columns 'species_id' and 'species_name'

    # If read_db available, try to call it. Otherwise use demo data
    if read_db is not None:
        try:
            serials_df = read_db(sql_serials)
            species_df = read_db(sql_species)
        except Exception as e:
            # If read_db or your SQL raise errors, fall back to demo data.
            species_df = _demo_species_df()
            serials_df = _demo_serials_df()
    else:
        # fallback demo
        species_df = _demo_species_df()
        serials_df = _demo_serials_df()

    # Normalize column names to what the rest of the app expects:
    # - species_df should have 'species_name' and 'species_id'
    # - observations_df used to populate serial dropdown will use column 'serial_id'
    if 'species_id' not in species_df.columns and 'species' in species_df.columns:
        species_df = species_df.rename(columns={'species': 'species_id'})
    if 'species_name' not in species_df.columns and 'name' in species_df.columns:
        species_df = species_df.rename(columns={'name': 'species_name'})

    # Convert serials_df to a form compatible with dropdown population:
    # If upstream returned 'serialId' rename to 'serial_id' so dropdown code can reuse prior logic.
    if 'serialId' in serials_df.columns and 'serial_id' not in serials_df.columns:
        serials_for_dropdown = serials_df.rename(columns={'serialId': 'serial_id'})
    elif 'serial_id' in serials_df.columns:
        serials_for_dropdown = serials_df.copy()
    else:
        # If no expected column, create a minimal one from first column
        if not serials_df.empty:
            first_col = serials_df.columns[0]
            serials_for_dropdown = serials_df.rename(columns={first_col: 'serial_id'})
        else:
            serials_for_dropdown = pd.DataFrame(columns=['serial_id'])

    # Observations df at startup can be empty (full results arrive when user runs a query).
    # But we keep a serials-only observations_df to allow the user to select serial ids.
    observations_df = serials_for_dropdown.copy()

    return {'species_df': species_df, 'observations_df': observations_df}

# -------------------------
# Minimal wrapper for calling generate_query_and_params + read_db
# -------------------------
def build_sql_and_params_from_selections(species_wanted, serial_id_wanted, datemin, datemax):
    """
    species_wanted: list[int] or None
    serial_id_wanted: list[str] or None
    datemin / datemax: python datetime.datetime or None

    Returns (sql, params)
    """
    if generate_query_and_params is None:
        # fallback to old placeholder behavior if user hasn't provided module
        # Build a simple SQL and params reminiscent of the previous placeholder
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
    else:
        # generate_query_and_params expects datemin/datemax as datetimes (or None)
        # The function signature you described: (serialIds, species_ids, datemin, datemax)
        # Map our args appropriately.
        return generate_query_and_params(
            serialIds=serial_id_wanted,
            species_ids=species_wanted,
            datemin=datemin,
            datemax=datemax
        )

def execute_sql(sql, params):
    """
    Executes SQL + params via read_db and returns a pandas DataFrame.
    If read_db is missing, or call fails, returns an empty DataFrame.
    """
    if read_db is None:
        # Debug fallback: return empty DataFrame so app remains functional
        return pd.DataFrame()
    try:
        # read_db already accepts (sql, params)
        return read_db(sql, params)
    except TypeError:
        # Some versions might expect only sql if no params - try both ways
        return read_db(sql)
    except Exception:
        return pd.DataFrame()

# ---------------------------------------------------------
# APPLICATION SETUP
# ---------------------------------------------------------
app = Dash(__name__)
server = app.server

# ------------------------------
# RUN AT APP STARTUP (module import time)
# ------------------------------
startup_results = run_all_update_funcs()
_startup_species_df = startup_results.get('species_df', pd.DataFrame(columns=['species_name', 'species_id']))
_startup_observations_df = startup_results.get('observations_df', pd.DataFrame())

# Prepare initial store data
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
    dcc.Store(id='store-last-scraped', data=None),
    # dcc.Store(id='store-scrape-status', data="ready"),  



    html.H1('Serengeti Tracker Assistant', style={'textAlign': 'center'}),

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
                html.Span(id='display-last-scraped', style={'marginRight': '20px'}),

                html.Button("Webscrape", id='btn-webscrape', n_clicks=0, style={'marginRight': '20px'}),

                # html.Span(id='display-scrape-status', style={'fontWeight': 'bold', 'marginRight': '20px'}),

                html.Button("Update current", id='btn-update-current', n_clicks=0)
                    ], style={'textAlign': 'right', 'marginBottom': '10px', 'display': 'flex', 'alignItems': 'center', 'gap': '10px'}),


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
               "Replace the placeholder SQL strings marked with \"##!##\" in the code "
               "and ensure generate_sql_query.generate_query_and_params and interact_db.read_db are available."),
    ], style={'marginTop': '10px', 'fontStyle': 'italic'})
],

    style={
    'fontFamily': 'Arial, sans-serif',
    'backgroundColor': "#f5c058ff",
    'padding': '0',
    'margin': '0',
    }

)

# ---------------------------------------------------------
# CALLBACKS
# ---------------------------------------------------------

# 1) Update current button: re-run update functions and re-populate stores
@callback(
    Output('store-species-df', 'data'),
    Output('store-observations-df', 'data'),
    Output('store-last-scraped', 'data'),
    Input('btn-update-current', 'n_clicks'),
    prevent_initial_call=False
)
def on_update_current(n_clicks):
    results = run_all_update_funcs()
    species_df = results.get('species_df', pd.DataFrame(columns=['species_name', 'species_id']))
    observations_df = results.get('observations_df', pd.DataFrame())

    # Update module-level copy (if any other functions use it)
    global _startup_species_df, _startup_observations_df
    _startup_species_df = species_df
    _startup_observations_df = observations_df

    try:
        last_df = read_db("SELECT last_scraped FROM tAnimal")
        last_scraped = last_df['last_scraped'].max()
        last_scraped = str(last_scraped) if last_scraped is not None else "Unknown"
    except Exception:
        last_scraped = "Unknown"

    return (
    species_df.to_dict(orient='records'),
    observations_df.to_dict(orient='records'),
    last_scraped
        )


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
    # Support both 'serial_id' and 'serialId' column names
    if not obs_df.empty:
        if 'serial_id' in obs_df.columns:
            serial_list = sorted(obs_df['serial_id'].astype(str).unique())
        elif 'serialId' in obs_df.columns:
            serial_list = sorted(obs_df['serialId'].astype(str).unique())
        else:
            # fallback to first column
            serial_list = sorted(obs_df[obs_df.columns[0]].astype(str).unique())
        serial_options = [{'label': s, 'value': s} for s in serial_list]
    else:
        serial_options = []
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
    # Convert options lists into full-value lists for "all" checking
    all_species_values = [opt['value'] for opt in species_options] if species_options else []
    all_serial_values = [opt['value'] for opt in serial_options] if serial_options else []

    # Species: None if none selected or all selected
    if not species_selected or set(species_selected) == set(all_species_values):
        species_wanted = None
    else:
        species_wanted = list(species_selected)

    # Serial: None if none selected or all selected
    if not serial_selected or set(serial_selected) == set(all_serial_values):
        serial_id_wanted = None
    else:
        serial_id_wanted = list(serial_selected)

    # Date handling: convert date strings from DatePickerSingle to datetime.datetime or None
    datemin = pd.to_datetime(date_min).to_pydatetime() if date_min else None
    datemax = pd.to_datetime(date_max).to_pydatetime() if date_max else None

    # Call generate_query_and_params (or fallback wrapper)
    sql, params = build_sql_and_params_from_selections(species_wanted, serial_id_wanted, datemin, datemax)

    # Store SQL + params
    return sql, params, (sql if sql else "")


# 5) Enable/disable Run Query button depending on whether SQL exists in memory
@callback(
    Output('btn-run-query', 'disabled'),
    Input('store-sql', 'data')
)
def toggle_run_button(sql):
    if not sql:
        return True
    return False


# 6) Run query button: run the query (using read_db) and store result df and return a map figure
@callback(
    Output('store-results-df', 'data'),
    Output('graph-content', 'figure'),
    Input('btn-run-query', 'n_clicks'),
    State('store-sql', 'data'),
    State('store-params', 'data'),
    prevent_initial_call=True
)
def on_run_query(n_clicks, sql, params):
    # Execute SQL against DB
    df = execute_sql(sql, params)

    # If DB returned no results, keep df empty DataFrame
    if df is None:
        df = pd.DataFrame()

    results_data = df.to_dict(orient='records')

    # Build map figure
    fig = build_map_figure_from_df(df)

    return results_data, fig

@callback(
    Output('display-last-scraped', 'children'),
    Input('store-last-scraped', 'data')
)
def show_last_scraped(last_scraped):
    if not last_scraped:
        return "Last scraped: Unknown"
    return f"Last scraped: {last_scraped}"
    
@callback(
    Input('btn-webscrape', 'n_clicks'),
    prevent_initial_call=True
)
def on_webscrape(n_clicks):
    if do_webscrape is None or add_new is None:
        print("Webscraping or add_new function unavailable.")
        return
    try:
        df = do_webscrape()
        add_new(df)
    except Exception as e:
        print("Webscrape error:", e)



def build_map_figure_from_df(df):
    """
    Build a map figure that:
     - Plots all points (latitude/longitude) colored by serialId
     - Links points for the same serialId in chronological order
     - Shows hovertext with serialId, date, latitude, longitude, species_name
    Handles common column-name variants: 'serialId' or 'serial_id', etc.
    """
    # If empty, return an empty scatter
    if df is None or df.empty:
        empty_fig = px.scatter_map(pd.DataFrame({'lat': [], 'lon': []}), lat='lat', lon='lon', title='No data to plot')
        empty_fig.update_layout(map_style='open-street-map')
        return empty_fig

    # Normalize column names: prefer serialId, species_name, latitude, longitude, date
    df2 = df.copy()

    # serial id normalization
    if 'serialId' not in df2.columns and 'serial_id' in df2.columns:
        df2 = df2.rename(columns={'serial_id': 'serialId'})
    elif 'serialId' not in df2.columns and 'serial' in df2.columns:
        df2 = df2.rename(columns={'serial': 'serialId'})

    # latitude/longitude normalization
    lat_col = None
    lon_col = None
    for cand in ['latitude', 'lat', 'Latitude', 'LAT']:
        if cand in df2.columns:
            lat_col = cand
            break
    for cand in ['longitude', 'lon', 'Longitude', 'LON']:
        if cand in df2.columns:
            lon_col = cand
            break

    if lat_col is None or lon_col is None:
        # cannot map, return a fallback plot
        fallback = px.scatter(pd.DataFrame({'x': [], 'y': []}), x='x', y='y', title='No latitude/longitude columns found in result')
        return fallback

    df2 = df2.rename(columns={lat_col: 'latitude', lon_col: 'longitude'})

    # species_name normalization
    if 'species_name' not in df2.columns:
        for cand in ['species', 'speciesName', 'species_name_str']:
            if cand in df2.columns:
                df2 = df2.rename(columns={cand: 'species_name'})
                break
    # date normalization
    if 'date' not in df2.columns:
        for cand in ['datetime', 'obs_date', 'time']:
            if cand in df2.columns:
                df2 = df2.rename(columns={cand: 'date'})
                break

    # Ensure date column is datetime
    if 'date' in df2.columns:
        df2['date'] = pd.to_datetime(df2['date'])
    else:
        # If there's no date, create a placeholder ordering column to connect in insertion order
        df2['date'] = pd.Series([pd.NaT] * len(df2))

    # Ensure serialId exists
    if 'serialId' not in df2.columns:
        # fallback: use first column as serialId
        df2['serialId'] = df2[df2.columns[0]].astype(str)

    # Build map: create one trace per serialId (a line + markers)
    fig = go.Figure()

    # Compute map center
    try:
        ## old logic, leaving it here so that it can be fallen back on if necessary
        center_lat = df2['latitude'].astype(float).mean()
        center_lon = df2['longitude'].astype(float).mean()
        map_center = {"lat": center_lat, "lon": center_lon}

        ### actually I want the map centered manually 
        map_center = {"lat": -1.9, "lon": 34.81076841740793}
    except Exception:
        map_center = {"lat": 0, "lon": 0}

    for sid, group in df2.groupby('serialId'):
        g = group.sort_values('date')  # sort by date so connections follow chronological order
        # Line connecting points in order (skip if only one point)
        if len(g) >= 2:
            fig.add_trace(go.Scattermap(
                lat=g['latitude'].tolist(),
                lon=g['longitude'].tolist(),
                mode='lines',
                name=str(sid),
                hoverinfo='none',  # hover will be on markers
                line=dict(width=2),
                showlegend=True
            ))
        # Markers with hovertext
        hover_texts = []
        for _, row in g.iterrows():
            dt_str = row['date'].isoformat() if pd.notnull(row['date']) else ''
            species_str = row['species_name'] if 'species_name' in row and pd.notnull(row['species_name']) else ''
            hover_texts.append(
                f"serialId: {sid}<br>date: {dt_str}<br>lat: {row['latitude']}<br>lon: {row['longitude']}<br>species_name: {species_str}"
            )
        fig.add_trace(go.Scattermap(
            lat=g['latitude'].tolist(),
            lon=g['longitude'].tolist(),
            mode='markers',
            marker=dict(size=8),
            name=f"{sid} (points)",
            hoverinfo='text',
            hovertext=hover_texts,
            showlegend=False  # legend clutter if many serials
        ))

    fig.update_layout(
        map={
            'style': "open-street-map",
            'center': map_center,
            'zoom': 7.5
        },
        margin={'l':0, 'r':0, 'b':0, 't':30},
        title='Observations map (points linked by serialId in time order)'
    )

    return fig


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
    return dcc.send_data_frame(df.to_csv, filename=f'results_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.csv', index=False)


# ---------------------------------------------------------
# RUN APP
# ---------------------------------------------------------
if __name__ == '__main__':
    port = 8050
    address = f"http://localhost:{port}"
    app.run(debug=True, port=port)
