import os

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv


load_dotenv(dotenv_path='.env')



# Uncomment the following line if you need to initialize FAISS with no AVX2 optimization
# os.environ['FAISS_NO_AVX2'] = '1'
class FaissIndex:
    def __init__(self, index_path, docs=None):
        self.index_path = index_path
        self.index = None
        self.embeddings = OpenAIEmbeddings()

    def create_index(self, docs):
        embeddings = OpenAIEmbeddings()
        self.index = FAISS.from_documents(docs, embeddings)

    def load_index(self):
        self.index = FAISS.load_local(self.index_path, self.embeddings, allow_dangerous_deserialization=True)

    def save_index(self):
        self.index.save_local(self.index_path)

    def similarity_search(self, query, k=10):
        return self.index.similarity_search(query, k)

    def get_retriever(self):
        if os.path.exists(self.index_path) and os.path.isdir(self.index_path):
            self.load_index()
        else:
            raise ValueError("Unable to load the index, please create an index")
        return self.index.as_retriever(search_kwargs={'k': 10})

