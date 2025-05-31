# AgenticChunkingRAG

# installation

```bash
conda create -n agentchunking python=3.10
conda create agentchunking
conda install -c conda-forge psycopg2
pip install -e .
pip install -q -U google-genai
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip install transformers==4.52.3 sentence-transformers==4.1.0 pandas==2.2.3 
pip install loguru tqdm sqlalchemy
pip install numpy==1.23.0 # revert to this for stable useage
```