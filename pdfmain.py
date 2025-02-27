import os
import openai
import sys
from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter, CharacterTextSplitter
from langchain.text_splitter import TokenTextSplitter
from langchain.document_loaders import NotionDirectoryLoader
from langchain.text_splitter import MarkdownHeaderTextSplitter
from langchain.vectorstores import Chroma

sys.path.append('../..')

from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())  # read local .env file

openai.api_key = os.environ['OPENAI_API_KEY']
from langchain.document_loaders import WebBaseLoader

# loader = WebBaseLoader("https://github.com/basecamp/handbook/blob/master/37signals-is-you.md")

# loader = PyPDFLoader("physicskcet.pdf", extract_images=True)
loader = PyPDFLoader("dotnet.pdf")
pages = loader.load()
len(pages)
print(len(pages))
page = pages[0]
print(page.page_content[0:500])
page.metadata
chunk_size = 26
chunk_overlap = 4
r_splitter = RecursiveCharacterTextSplitter(
    chunk_size=chunk_size,
    chunk_overlap=chunk_overlap
)
c_splitter = CharacterTextSplitter(
    chunk_size=chunk_size,
    chunk_overlap=chunk_overlap
)
text_splitter = CharacterTextSplitter(
    separator="\n",
    chunk_size=1000,
    chunk_overlap=150,
    length_function=len
)
docs = text_splitter.split_documents(pages)
print(len(docs))
print(len(pages))
text_splitter = TokenTextSplitter(chunk_size=1500, chunk_overlap=50)
docs = text_splitter.split_documents(pages)

from langchain.text_splitter import RecursiveCharacterTextSplitter

# text_splitter = RecursiveCharacterTextSplitter(
#     chunk_size=1500,
#     chunk_overlap=150
# )
# splits = text_splitter.split_documents(docs)
len(docs)
print(len(docs))
import pandas as pd

# Assuming splits is a list of strings (documents)
# Convert splits into a DataFrame
df = pd.DataFrame({'Document': docs, 'book': "dot net"})

# Define the file path for the Excel file
excel_file_path = 'dotnet.xlsx'

# Export DataFrame to Excel
df.to_excel(excel_file_path, index=False)

