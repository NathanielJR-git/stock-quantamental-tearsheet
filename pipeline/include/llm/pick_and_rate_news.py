import os
import pandas as pd
from groq import Groq
from include.configuration import configuration
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from tenacity import retry, wait_exponential, stop_after_attempt


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def call_groq_api(ticker_news_json: str, client: Groq) -> str:
    """
    Main function to call Groq API to select top news and
    return its sentiment score and reasoning in JSON format
    """
    # Handle empty rows
    if not ticker_news_json: return "{}"
    
    prompt = f"""
    Anda adalah analis kuantitatif pasar modal. 
    Berikut adalah 20 berita terbaru untuk sebuah saham:
    {ticker_news_json}
    
    Tugas Anda:
    1. Pilih maksimal 3 berita yang paling berdampak pada pergerakan harga saham.
    2. Berikan sentiment score (-1.0 sangat negatif hingga 1.0 sangat positif).
    3. Output WAJIB dalam format JSON murni dengan struktur:
    {{"extracted_news": [{{"title": "judul", "sentiment_score": 0.5, "summary": "alasan"}}]}}
    """

    # Get and return Groq Llama response
    response = client.chat.completions.create(
        model=configuration.LLM_MODEL_NAME, 
        response_format={"type": "json_object"},
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
    )
    return response.choices[0].message.content


@pandas_udf(StringType())
def extract_top_news_udf(news_batch: pd.Series) -> pd.Series:
    """
    Pandas UDF to make Groq API call parallelable
    """
    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    
    results = []
    for news_json in news_batch:
        try:
            result = call_groq_api(news_json, client)
            results.append(result)
        except Exception as e:
            results.append('{"extracted_news": [], "error": "' + str(e) + '"}')
            
    return pd.Series(results)