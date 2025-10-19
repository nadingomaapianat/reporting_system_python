# Python Backend Configuration

## Environment Variables

The Python backend now uses environment variables for configuration to make deployment easier.

### Required Environment Variables

- `NODE_BACKEND_URL`: The URL of the Node.js backend API
  - Default: `http://localhost:3002`
  - Local development: `http://localhost:3002`
  - Production: `https://reporting-system-backend.pianat.ai`

### Setting Environment Variables

#### Windows (PowerShell)
```powershell
$env:NODE_BACKEND_URL="https://reporting-system-backend.pianat.ai"
python main.py
```

#### Windows (Command Prompt)
```cmd
set NODE_BACKEND_URL=https://reporting-system-backend.pianat.ai
python main.py
```

#### Linux/Mac (Bash)
```bash
export NODE_BACKEND_URL="https://reporting-system-backend.pianat.ai"
python main.py
```

#### Using .env file (recommended)
Create a `.env` file in the project root:
```
NODE_BACKEND_URL=https://reporting-system-backend.pianat.ai
```

Then install python-dotenv and load it in your code:
```python
from dotenv import load_dotenv
load_dotenv()
```

### Deployment Examples

#### Local Development
```bash
NODE_BACKEND_URL=http://localhost:3002 python main.py
```

#### Production
```bash
NODE_BACKEND_URL=https://reporting-system-backend.pianat.ai python main.py
```

#### Docker
```dockerfile
ENV NODE_BACKEND_URL=https://reporting-system-backend.pianat.ai
```

#### Docker Compose
```yaml
environment:
  - NODE_BACKEND_URL=https://reporting-system-backend.pianat.ai
```
