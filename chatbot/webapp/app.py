import os
import requests
import dash
from dash import Dash, html, dcc, Input, Output, State

MODEL_RUNNER_URL = os.environ.get('MODEL_RUNNER_URL', 'http://localhost:11434')


app = Dash(__name__, update_title=None)
server = app.server


app.layout = html.Div([
    html.H2('LLM test chat for home work'),
    dcc.Textarea(id='prompt', placeholder='Type your message here...', style={'width': '100%', 'height': '120px'}),
    html.Div([
        html.Button('Send', id='send-btn', n_clicks=0, style={'marginRight': '10px'}),
        html.Button('Clear', id='clear-btn', n_clicks=0)
    ]),
    html.Div(id='chat-output', style={'whiteSpace': 'pre-wrap', 'marginTop': '16px', 'border': '1px solid #ddd', 'padding': '12px', 'minHeight': '200px'}),
    dcc.Store(id='history', data=[]),
])


@app.callback(
    Output('history', 'data'),
    Output('chat-output', 'children'),
    Output('prompt', 'value'),
    Input('send-btn', 'n_clicks'),
    Input('clear-btn', 'n_clicks'),
    State('prompt', 'value'),
    State('history', 'data'),
    prevent_initial_call=True
)
def send_to_model(send_clicks, clear_clicks, prompt, history):
    ctx = dash.callback_context
    if not ctx.triggered:
        return history, '', ''
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'clear-btn':
        return [], 'Chat cleared.', ''
    
    if not prompt:
        return history, 'Please type a message.', prompt

    history = history or []
    history.append({'role': 'user', 'text': prompt})

    try:
        print(f"Sending request to: {MODEL_RUNNER_URL}/api/generate")
        resp = requests.post(
            f"{MODEL_RUNNER_URL.rstrip('/')}/api/generate",
            json={
                'model': 'gemma2:2b',
                'prompt': prompt,
                'stream': False
            },
            timeout=120
        )
        print(f"Response status: {resp.status_code}")
        print(f"Response text: {resp.text[:200]}...")
        resp.raise_for_status()
        data = resp.json()
        model_text = data.get('response', 'No response field in JSON')
        print(f"Model response: {model_text[:100]}...")
    except requests.exceptions.RequestException as e:
        model_text = f"Network error: {e}"
        print(f"Request error: {e}")
    except Exception as e:
        model_text = f"Unexpected error: {e}"
        print(f"Unexpected error: {e}")

    history.append({'role': 'assistant', 'text': model_text})

    out_lines = []
    for m in history:
        prefix = 'You: ' if m['role'] == 'user' else 'Assistant: '
        out_lines.append(prefix + m['text'])

    return history, '\n\n'.join(out_lines), ''


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)
