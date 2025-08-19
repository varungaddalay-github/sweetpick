# 🚀 Vercel Deployment Guide for Sweet Morsels

## Prerequisites

### 1. API Keys Required
- [ ] **OpenAI API Key**: https://platform.openai.com/api-keys
- [ ] **SerpAPI Key**: https://serpapi.com/
- [ ] **Milvus Cloud**: https://cloud.zilliz.com/

### 2. Vercel Account
- [ ] Sign up at https://vercel.com
- [ ] Connect GitHub account

## Deployment Steps

### Step 1: Import Repository
1. Go to Vercel Dashboard
2. Click "New Project"
3. Import: `varungaddalay-github/sweetpick`
4. Framework: Python
5. Root Directory: `./`

### Step 2: Build Configuration
- **Build Command**: `pip install -r requirements-vercel.txt`
- **Output Directory**: (leave empty)
- **Install Command**: `pip install -r requirements-vercel.txt`

### Step 3: Environment Variables
Add these in Vercel Project Settings → Environment Variables:

```
OPENAI_API_KEY=sk-...
SERPAPI_API_KEY=...
MILVUS_URI=https://...
MILVUS_TOKEN=...
REDIS_URL=... (optional)
LOG_LEVEL=INFO
ENVIRONMENT=production
```

### Step 4: Function Configuration
- **Max Duration**: 30 seconds
- **Memory**: 1024 MB
- **Python Version**: 3.11

### Step 5: Domain Configuration
- Custom domain (optional)
- Vercel provides: `sweetpick.vercel.app`

## Post-Deployment

### Health Check
Visit: `https://your-domain.vercel.app/health`

### API Endpoints
- **Query**: `POST /query`
- **Health**: `GET /health`
- **Stats**: `GET /stats`
- **UI**: `GET /`

## Troubleshooting

### Common Issues
1. **Import Errors**: Check requirements-vercel.txt
2. **Timeout**: Increase function duration
3. **Memory**: Increase memory allocation
4. **API Keys**: Verify environment variables

### Logs
Check Vercel Function Logs for debugging.

## Success Criteria
- [ ] Health endpoint returns 200
- [ ] Query endpoint processes requests
- [ ] UI loads correctly
- [ ] No timeout errors
