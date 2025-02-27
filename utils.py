import os
import pandas as pd
from data_process import DataProcessor
from faiss_index import FaissIndex
from dotenv import load_dotenv

from langchain_community.document_loaders import DataFrameLoader
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter, CharacterTextSplitter
from langchain.text_splitter import TokenTextSplitter
from langchain.document_loaders import NotionDirectoryLoader
from langchain.text_splitter import MarkdownHeaderTextSplitter

load_dotenv(dotenv_path='.env')


# Press the green button in the gutter to run the script.
def index_creation():
    data_processor = DataProcessor("./input/combnedkm.xlsx")
    df = data_processor.process()
    loader = DataFrameLoader(df[["Question_clean"]], page_content_column="Question_clean")
    docs = loader.load()
    results_df = pd.DataFrame(df['Question_clean'])

    # Define the output CSV file
    csv_file = './input/questions_clean.csv'

    # Store the results into the CSV file
    results_df.to_csv(csv_file, index=False)

    print("Similarity search results saved to CSV file:", csv_file)
    faiss_index = FaissIndex(index_path="./index/pdf_questions")
    faiss_index.create_index(docs)
    faiss_index.save_index()
    results = faiss_index.similarity_search("Questions on constitution")
    print(results)



if __name__ == "__main__":
    index_creation()
    question = "Generate 10  questions"
    "that could be potential "
    "questions in the next year competitive exam related to {category} and {sub_category}"
    faiss_index = FaissIndex(index_path="./index/pdf_questions")
    #answer = question_generation_chain(index_path="./index/upsc_questions", question=question)
    #print(answer)
    retriever = faiss_index.get_retriever()
    docs = retriever.invoke(question)
    print(docs)