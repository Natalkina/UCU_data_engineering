# Python Chatbot with Local LLM

A Python-based chatbot application with web UI that communicates with a locally running Gemma2 2B model via Docker.

## Architecture

- **Service 1 (webapp)**: Web UI built with Dash framework, exposed on port 8050
- **Service 2 (llm)**: Ollama running Gemma2 2B model, exposed on port 11434

## Quick Start

1. Make sure Docker and Docker Compose are installed
2. Run the application:
   ```bash
   docker-compose up --build
   ```
3. Wait for model to download
4. Open your browser to http://localhost:8050
5. Start chatting with the AI

## Services

### Web Application (Port 8050)
- Built with Python Dash framework
- Provides chat interface

### LLM Service (Port 11434)
- Runs Ollama 
- Lightweight and fast text generation


## Stopping the Application

```bash
docker-compose down
```

To also remove the model data:
```bash
docker-compose down -v
```