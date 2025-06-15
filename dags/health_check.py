import os
import numpy as np
import pandas as pd
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Dict
from fuzzywuzzy import fuzz


class DuplicateDetectionService:
    def __init__(self, data_dir: str = "data", embedding_model: str = "all-mpnet-base-v2"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.embedding_model = embedding_model
        self.model = HuggingFaceEmbeddings(model_name=self.embedding_model)

    def get_embeddings(self, df: pd.DataFrame, selected_columns: List[str]) -> np.ndarray:
        df["combined_text"] = df[selected_columns].astype(str).agg(" ".join, axis=1)
        embeddings = self.model.embed_documents(df["combined_text"].tolist())
        return np.array(embeddings)

    def calculate_similarity_matrix(self, embeddings: np.ndarray) -> np.ndarray:
        similarity_matrix = cosine_similarity(embeddings)
        np.fill_diagonal(similarity_matrix, 0)
        return similarity_matrix

    def calculate_fuzzy_score(self, row1: pd.Series, row2: pd.Series, columns: List[str]) -> float:
        # Placeholder: replace with your actual fuzzy score logic or import
        
        scores = [fuzz.token_sort_ratio(str(row1[col]), str(row2[col])) for col in columns]
        return np.mean(scores)

    def detect_duplicates(
        self,
        df: pd.DataFrame,
        selected_columns: List[str],
        fuzzy_threshold: int = 90,
        semantic_threshold: int = 90,
    ) -> List[Dict]:
        embeddings = self.get_embeddings(df, selected_columns)
        similarity_matrix = self.calculate_similarity_matrix(embeddings)
        n = len(df)
        results = []
        match_pcts = []
        for i in range(n):
            aggregate_score = 0.0
            max_score = n - 1
            for j in range(n):
                if i == j:
                    continue
                # 1. Exact match
                if all(str(df.iloc[i][col]) == str(df.iloc[j][col]) for col in selected_columns):
                    aggregate_score += 1.0
                    continue
                # 2. Fuzzy match
                fuzzy = self.calculate_fuzzy_score(df.iloc[i], df.iloc[j], selected_columns)
                if fuzzy >= fuzzy_threshold:
                    aggregate_score += 0.7
                    continue
                # 3. Semantic match
                semantic = similarity_matrix[i, j]
                if semantic >= (semantic_threshold / 100):
                    aggregate_score += 0.4
            match_pct = aggregate_score / max_score if max_score > 0 else 0
            match_pcts.append(match_pct)
            results.append({
                "index": i,
                "aggregate_score": aggregate_score,
                "raw_match_pct": match_pct,
            })
        # Normalize match_pct values to [0, 1]
        min_val = min(match_pcts)
        max_val = max(match_pcts)
        for k, res in enumerate(results):
            if max_val > min_val:
                norm = (res["raw_match_pct"] - min_val) / (max_val - min_val)
            else:
                norm = 0.0
            results[k]["match_percentage"] = round(norm * 100, 2)
        return results
