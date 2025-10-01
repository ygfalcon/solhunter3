import time
import numpy as np
import faiss
import solhunter_zero.advanced_memory as am

class DummyModel:
    def __init__(self, dim: int = 3):
        self.dim = dim
    def get_sentence_embedding_dimension(self):
        return self.dim
    def encode(self, texts):
        time.sleep(0.001)  # simulate heavy work
        return np.zeros((len(texts), self.dim), dtype="float32")

def main(n: int = 200):
    am.faiss = faiss
    am.SentenceTransformer = lambda *a, **k: DummyModel()
    mem = am.AdvancedMemory(url="sqlite:///:memory:", index_path=":memory:")
    mem.model = DummyModel()
    mem.index = faiss.IndexIDMap2(faiss.IndexFlatL2(mem.model.dim))
    mem.cpu_index = None

    for i in range(n):
        mem.log_trade(token="T", direction="buy", amount=1, price=1, context=f"text {i}")
    mem.cluster_trades(num_clusters=10)

    contexts = [t.context for t in mem.list_trades()]

    start = time.time()
    mem.top_cluster_many(contexts)
    batched = time.time() - start

    start = time.time()
    for ctx in contexts:
        mem.top_cluster(ctx)
    serial = time.time() - start

    print(f"batched={batched:.3f}s old_loop={serial:.3f}s")

if __name__ == "__main__":
    main()
